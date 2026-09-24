package com.github.lisama569.durablefunctionruntimejava.demo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowRegistry;
import com.github.lisama569.durablefunctionruntimejava.engine.error.NonRetryableFailureException;
import com.github.lisama569.durablefunctionruntimejava.engine.error.RetryableFailureException;
import com.github.lisama569.durablefunctionruntimejava.engine.function.StepDefinition;
import com.github.lisama569.durablefunctionruntimejava.engine.function.WorkflowDefinition;
import com.github.lisama569.durablefunctionruntimejava.engine.json.Json;
import com.github.lisama569.durablefunctionruntimejava.engine.model.RetryPolicy;

import jakarta.annotation.PostConstruct;

/**
 * Local-only demo workflows registered at startup. No external services are
 * contacted: flakiness is simulated in-process and the "charge" side effect
 * goes through the engine's idempotent {@code runOnce} facility.
 */
@Component
public class DemoWorkflows {

    /** instanceId -> simulated transient failures remaining. */
    static final Map<String, Integer> SIMULATED_FAILURES = new ConcurrentHashMap<>();

    private final WorkflowRegistry registry;

    public DemoWorkflows(WorkflowRegistry registry) {
        this.registry = registry;
    }

    /** Configure how many transient failures the demo instance should see. */
    public static void simulateFailures(String instanceId, int count) {
        SIMULATED_FAILURES.put(instanceId, count);
    }

    @PostConstruct
    void register() {
        RetryPolicy retry = new RetryPolicy(3, 200L, 2.0, 5_000L, 0L);
        RetryPolicy once = new RetryPolicy(1, 0L, 1.0, 0L, 0L);

        registry.register(WorkflowDefinition.builder("demo-order")
                .addStep(StepDefinition.sync("validate", once, ctx -> {
                    String orderId = ctx.input().path("orderId").asText(null);
                    if (orderId == null || orderId.isBlank()) {
                        throw new NonRetryableFailureException("DEMO_BAD_ORDER",
                                "input.orderId is required");
                    }
                    return ctx.input();
                }))
                .addStep(StepDefinition.sync("charge", retry, ctx -> {
                    int remaining = SIMULATED_FAILURES.getOrDefault(ctx.instanceId(), 0);
                    if (remaining > 0) {
                        SIMULATED_FAILURES.put(ctx.instanceId(), remaining - 1);
                        throw new RetryableFailureException("DEMO_UPSTREAM_BUSY",
                                "simulated transient failure on attempt "
                                        + ctx.attemptNo());
                    }
                    return ctx.runOnce("demo-charge", () -> {
                        return Json.newObject()
                                .put("charged", true)
                                .put("orderId", ctx.input().path("orderId").asText())
                                .put("byAttempt", ctx.attemptNo());
                    });
                }))
                .addStep(StepDefinition.sync("receipt", once, ctx -> {
                    var receipt = Json.newObject();
                    receipt.set("result", ctx.input());
                    receipt.put("receipt", "R-" + System.identityHashCode(ctx.input()));
                    return receipt;
                }))
                .build());
    }
}
