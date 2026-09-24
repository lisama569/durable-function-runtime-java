package com.github.lisama569.durablefunctionruntimejava.engine.tests.support;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.lisama569.durablefunctionruntimejava.engine.DurableWorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.engine.EngineProperties;
import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowRegistry;
import com.github.lisama569.durablefunctionruntimejava.engine.error.NonRetryableFailureException;
import com.github.lisama569.durablefunctionruntimejava.engine.error.RetryableFailureException;
import com.github.lisama569.durablefunctionruntimejava.engine.function.StepDefinition;
import com.github.lisama569.durablefunctionruntimejava.engine.function.WorkflowDefinition;
import com.github.lisama569.durablefunctionruntimejava.engine.json.Json;
import com.github.lisama569.durablefunctionruntimejava.engine.model.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.engine.persistence.FileStateStore;

/**
 * Builds engines over a temp directory with a manually advanced clock and
 * registers the workflows used across the test suite:
 *
 * <ul>
 *   <li>{@code flaky-workflow}: validate (non-retryable guard) -&gt;
 *       flaky (fails the first N times with a stable retryable code,
 *       performs an idempotent side effect) -&gt; echo.</li>
 *   <li>{@code fatal-workflow}: first step throws a non-retryable error.</li>
 *   <li>{@code async-workflow}: manual async step with timeout/retry.</li>
 * </ul>
 */
public class TestHarness {

    /** Shared flakiness state: workflowName:instanceId -&gt; failures remaining. */
    public static final java.util.Map<String, Integer> FAIL_REMAINING = new java.util.concurrent.ConcurrentHashMap<>();

    public static final RetryPolicy FAST_RETRY =
            new RetryPolicy(3, 50L, 2.0, 5_000L, 0L);
    public static final RetryPolicy NO_RETRY = RetryPolicy.NO_RETRY;
    public static final RetryPolicy TIMEOUT_POLICY =
            new RetryPolicy(3, 5L, 1.0, 100L, 50L);

    private final AtomicLong clock = new AtomicLong(1_000_000L);
    private final WorkflowRegistry registry = new WorkflowRegistry();
    private ManualAsync lastAsyncDispatcher;

    public long now() {
        return clock.get();
    }

    public void advance(long ms) {
        clock.addAndGet(ms);
    }

    public WorkflowRegistry registry() {
        return registry;
    }

    /** New engine over the same persistent directory (simulates a restart). */
    public DurableWorkflowEngine engine(Path storageDir) {
        EngineProperties props = new EngineProperties();
        props.setStorageDir(storageDir.toString());
        props.setAutoStart(false);
        FileStateStore store = new FileStateStore(storageDir);
        registerWorkflows();
        DurableWorkflowEngine eng = new DurableWorkflowEngine(store, registry, props) {
            @Override
            protected long nowMs() {
                return clock.get();
            }
        };
        return eng;
    }

    public ManualAsync asyncDispatcher() {
        return lastAsyncDispatcher;
    }

    public void registerWorkflows() {
        // flaky-workflow
        registry.register(WorkflowDefinition.builder("flaky-workflow")
                .addStep(StepDefinition.sync("validate", NO_RETRY, ctx -> {
                    JsonNode in = ctx.input();
                    if (in == null || !in.path("orderId").isTextual()) {
                        throw new NonRetryableFailureException("BAD_ORDER",
                                "input.orderId is required");
                    }
                    return in;
                }))
                .addStep(StepDefinition.sync("flaky", FAST_RETRY, ctx -> {
                    String key = "flaky-workflow:" + ctx.instanceId();
                    int remaining = FAIL_REMAINING.getOrDefault(key, 0);
                    if (remaining > 0) {
                        FAIL_REMAINING.put(key, remaining - 1);
                        throw new RetryableFailureException("UPSTREAM_BUSY",
                                "upstream temporarily unavailable (attempt "
                                        + ctx.attemptNo() + ")");
                    }
                    ObjectNode effect = (ObjectNode) ctx.runOnce("charge",
                            () -> {
                                SideEffectCounters.of("charge").incrementAndGet();
                                ObjectNode n = Json.newObject();
                                n.put("charged", true);
                                n.put("orderId", ctx.input().path("orderId").asText());
                                return n;
                            });
                    ObjectNode out = Json.newObject();
                    out.set("charge", effect);
                    out.put("attempt", ctx.attemptNo());
                    return out;
                }))
                .addStep(StepDefinition.sync("echo", NO_RETRY, ctx -> ctx.input()))
                .build());

        // fatal-workflow
        registry.register(WorkflowDefinition.builder("fatal-workflow")
                .addStep(StepDefinition.sync("alwaysFatal", NO_RETRY,
                        ctx -> {
                            throw new NonRetryableFailureException("POISON",
                                    "this step can never succeed");
                        }))
                .build());

        // async-workflow: each engine registration gets a fresh manual dispatcher
        lastAsyncDispatcher = new ManualAsync();
        registry.register(WorkflowDefinition.builder("async-workflow")
                .addStep(StepDefinition.async("awaitApproval", TIMEOUT_POLICY,
                        lastAsyncDispatcher))
                .addStep(StepDefinition.sync("finish", NO_RETRY, ctx -> ctx.input()))
                .build());
    }
}
