package com.github.lisama569.durablefunctionruntimejava.engine.persistence;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.github.lisama569.durablefunctionruntimejava.engine.EngineProperties;
import com.github.lisama569.durablefunctionruntimejava.engine.json.Json;
import com.github.lisama569.durablefunctionruntimejava.engine.model.WorkflowInstance;

/**
 * Local-filesystem implementation of {@link StateStore}.
 *
 * <p>Layout under {@code <storageDir>/}:
 * <pre>
 *   instances/&lt;instanceId&gt;.json     – authoritative aggregate (atomic rename)
 *   effects/&lt;instanceId&gt;/&lt;step&gt;/&lt;sha256(effectKey)&gt;.json
 * </pre>
 * All mutations of an instance run under a per-instance {@link ReentrantLock}
 * so concurrent drives/callbacks for the same instance are serialized;
 * different instances never block each other. Commits are temp-file + rename,
 * which is atomic on a local POSIX filesystem, so a crash never leaves a torn
 * aggregate.
 */
public class FileStateStore implements StateStore {

    private static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9._-]{1,128}");

    private final Path root;
    private final Path instancesDir;
    private final Path effectsDir;
    private final Map<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    public FileStateStore(EngineProperties properties) {
        this(Path.of(properties.getStorageDir()));
    }

    public FileStateStore(Path storageDir) {
        this.root = storageDir.toAbsolutePath().normalize();
        this.instancesDir = this.root.resolve("instances");
        this.effectsDir = this.root.resolve("effects");
        try {
            Files.createDirectories(instancesDir);
            Files.createDirectories(effectsDir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot initialize storage at " + root, e);
        }
    }

    private ReentrantLock lockFor(String instanceId) {
        return locks.computeIfAbsent(instanceId, k -> new ReentrantLock());
    }

    private Path instanceFile(String instanceId) {
        if (!SAFE_ID.matcher(instanceId).matches()) {
            throw new IllegalArgumentException("illegal instanceId: " + instanceId);
        }
        return instancesDir.resolve(instanceId + ".json");
    }

    private Path effectFile(String instanceId, String stepName, String effectKey) {
        if (!SAFE_ID.matcher(instanceId).matches() || !SAFE_ID.matcher(stepName).matches()) {
            throw new IllegalArgumentException("illegal instance/step identifier");
        }
        String hash = HexFormat.of().formatHex(
                sha256(instanceId + "|" + stepName + "|" + effectKey));
        return effectsDir.resolve(instanceId).resolve(stepName).resolve(hash + ".json");
    }

    private static byte[] sha256(String s) {
        try {
            return java.security.MessageDigest.getInstance("SHA-256")
                    .digest(s.getBytes(StandardCharsets.UTF_8));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private WorkflowInstance readFile(Path file) {
        try {
            byte[] bytes = Files.readAllBytes(file);
            return Json.mapper().readValue(bytes, WorkflowInstance.class);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + file, e);
        }
    }

    private WorkflowInstance deepCopy(WorkflowInstance in) {
        if (in == null) {
            return null;
        }
        try {
            byte[] bytes = Json.mapper().writeValueAsBytes(in);
            return Json.mapper().readValue(bytes, WorkflowInstance.class);
        } catch (IOException e) {
            throw new UncheckedIOException("deep copy failed", e);
        }
    }

    private void writeAtomically(Path target, byte[] payload) {
        try {
            Files.createDirectories(target.getParent());
            Path tmp = Files.createTempFile(target.getParent(), ".tmp-" + target.getFileName(), ".json");
            try {
                Files.write(tmp, payload, StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                Files.deleteIfExists(tmp);
                throw e;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write " + target, e);
        }
    }

    @Override
    public void insert(WorkflowInstance instance) {
        ReentrantLock lock = lockFor(instance.getInstanceId());
        lock.lock();
        try {
            Path file = instanceFile(instance.getInstanceId());
            if (Files.exists(file)) {
                throw new IllegalStateException("instance already exists: " + instance.getInstanceId());
            }
            byte[] payload = Json.mapper().writeValueAsBytes(instance);
            writeAtomically(file, payload);
        } catch (IOException e) {
            throw new UncheckedIOException("insert failed", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public WorkflowInstance load(String instanceId) {
        ReentrantLock lock = lockFor(instanceId);
        lock.lock();
        try {
            Path file = instanceFile(instanceId);
            if (!Files.exists(file)) {
                return null;
            }
            return readFile(file);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public WorkflowInstance mutate(String instanceId, boolean create,
                                   Function<WorkflowInstance, WorkflowInstance> mutator) {
        ReentrantLock lock = lockFor(instanceId);
        lock.lock();
        try {
            Path file = instanceFile(instanceId);
            WorkflowInstance current = Files.exists(file) ? readFile(file) : null;
            if (current == null && !create) {
                return null;
            }
            WorkflowInstance result = mutator.apply(current);
            if (result == null) {
                return current; // mutator aborted: nothing is persisted
            }
            result.setVersion(current == null ? 1 : current.getVersion() + 1);
            byte[] payload = Json.mapper().writeValueAsBytes(result);
            writeAtomically(file, payload);
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException("mutate failed for " + instanceId, e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] loadEffect(String instanceId, String stepName, String effectKey) {
        Path file = effectFile(instanceId, stepName, effectKey);
        if (!Files.exists(file)) {
            return null;
        }
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read effect " + file, e);
        }
    }

    @Override
    public void saveEffect(String instanceId, String stepName, String effectKey, byte[] payload) {
        writeAtomically(effectFile(instanceId, stepName, effectKey), payload);
    }

    @Override
    public List<String> listInstanceIds() {
        if (!Files.exists(instancesDir)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(instancesDir)) {
            List<String> ids = new ArrayList<>();
            files.filter(Files::isRegularFile)
                    .map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".json") && !n.startsWith("."))
                    .map(n -> n.substring(0, n.length() - ".json".length()))
                    .sorted()
                    .forEach(ids::add);
            return ids;
        } catch (IOException e) {
            throw new UncheckedIOException("list instances failed", e);
        }
    }
}
