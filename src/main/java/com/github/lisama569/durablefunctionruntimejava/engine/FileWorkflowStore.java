package com.github.lisama569.durablefunctionruntimejava.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * 基于本地文件系统的持久化实现：
 * 每个实例一个 JSON 文件，写入采用 临时文件 + 原子移动，避免崩溃产生半截文件。
 * 内存索引由磁盘文件重建，文件是唯一事实来源。
 */
public class FileWorkflowStore implements WorkflowStore {

    private static final Logger log = LoggerFactory.getLogger(FileWorkflowStore.class);

    private final Path instanceDir;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, WorkflowInstance> byId = new ConcurrentHashMap<>();
    /** (workflowName, idempotencyKey) -> instanceId，用于幂等去重。 */
    private final Map<String, String> byIdempotencyKey = new ConcurrentHashMap<>();
    /** 全局写锁：保证 createIfAbsent 的查重与落盘原子完成。 */
    private final ReentrantLock writeLock = new ReentrantLock();

    public FileWorkflowStore(Path dataDir) {
        this.instanceDir = dataDir.resolve("instances");
        try {
            Files.createDirectories(instanceDir);
            loadFromDisk();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to init store at " + dataDir, e);
        }
    }

    private void loadFromDisk() throws IOException {
        try (Stream<Path> files = Files.list(instanceDir)) {
            for (Path file : files.filter(f -> f.getFileName().toString().endsWith(".json")).toList()) {
                WorkflowInstance instance = mapper.readValue(file.toFile(), WorkflowInstance.class);
                byId.put(instance.getId(), instance);
                if (instance.getIdempotencyKey() != null) {
                    byIdempotencyKey.put(idemKey(instance.getWorkflowName(), instance.getIdempotencyKey()), instance.getId());
                }
            }
        }
        log.info("store recovered {} instance(s) from {}", byId.size(), instanceDir);
    }

    private static String idemKey(String workflowName, String idempotencyKey) {
        return workflowName + "" + idempotencyKey;
    }

    private Path fileOf(String instanceId) {
        return instanceDir.resolve(instanceId + ".json");
    }

    /** 原子落盘：先写临时文件再 move，保证重启后读到的要么是旧版本要么是完整新版本。 */
    private void persist(WorkflowInstance instance) {
        try {
            Path tmp = Files.createTempFile(instanceDir, instance.getId(), ".tmp");
            mapper.writeValue(tmp.toFile(), instance);
            Files.move(tmp, fileOf(instance.getId()),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist instance " + instance.getId(), e);
        }
    }

    @Override
    public boolean createIfAbsent(WorkflowInstance instance) {
        writeLock.lock();
        try {
            if (byId.containsKey(instance.getId())) {
                return false;
            }
            if (instance.getIdempotencyKey() != null
                    && byIdempotencyKey.containsKey(idemKey(instance.getWorkflowName(), instance.getIdempotencyKey()))) {
                return false;
            }
            persist(instance);
            byId.put(instance.getId(), instance);
            if (instance.getIdempotencyKey() != null) {
                byIdempotencyKey.put(idemKey(instance.getWorkflowName(), instance.getIdempotencyKey()), instance.getId());
            }
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean update(WorkflowInstance instance) {
        writeLock.lock();
        try {
            WorkflowInstance current = byId.get(instance.getId());
            if (current == null || current.getVersion() != instance.getVersion()) {
                return false;
            }
            instance.setVersion(instance.getVersion() + 1);
            instance.setUpdatedAt(System.currentTimeMillis());
            persist(instance);
            byId.put(instance.getId(), copy(instance));
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /** 返回防御性拷贝，避免调用方修改内存索引中的对象。 */
    private WorkflowInstance copy(WorkflowInstance instance) {
        try {
            return mapper.readValue(mapper.writeValueAsBytes(instance), WorkflowInstance.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Optional<WorkflowInstance> findById(String instanceId) {
        return Optional.ofNullable(byId.get(instanceId)).map(this::copy);
    }

    @Override
    public Optional<WorkflowInstance> findByIdempotencyKey(String workflowName, String idempotencyKey) {
        return Optional.ofNullable(byIdempotencyKey.get(idemKey(workflowName, idempotencyKey)))
                .flatMap(this::findById);
    }

    @Override
    public List<WorkflowInstance> findAll() {
        List<WorkflowInstance> all = new ArrayList<>();
        for (WorkflowInstance instance : byId.values()) {
            all.add(copy(instance));
        }
        return all;
    }

    /** 重启后需要恢复的实例：仍处于 RUNNING 的实例。 */
    public List<WorkflowInstance> findResumable() {
        return findAll().stream()
                .filter(i -> i.getStatus() == WorkflowStatus.RUNNING)
                .toList();
    }
}
