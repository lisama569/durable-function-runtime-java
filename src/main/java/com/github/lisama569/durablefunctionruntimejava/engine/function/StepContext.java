package com.github.lisama569.durablefunctionruntimejava.engine.function;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Context handed to step business logic. Inputs are immutable; side effects
 * must go through {@link #runOnce(String, SideEffect)} so duplicates across
 * retries/restarts are suppressed.
 */
public final class StepContext {

    public interface SideEffect {
        JsonNode execute() throws Exception;
    }

    private final String instanceId;
    private final String stepName;
    private final int attemptNo;
    private final JsonNode input;
    private final IdempotenceService idempotence;

    public StepContext(String instanceId, String stepName, int attemptNo, JsonNode input,
                       IdempotenceService idempotence) {
        this.instanceId = instanceId;
        this.stepName = stepName;
        this.attemptNo = attemptNo;
        this.input = input;
        this.idempotence = idempotence;
    }

    public String instanceId() {
        return instanceId;
    }

    public String stepName() {
        return stepName;
    }

    public int attemptNo() {
        return attemptNo;
    }

    public JsonNode input() {
        return input;
    }

    /**
     * Execute an idempotent side effect exactly once per (step, effectKey):
     * retries and post-restart replays return the stored result.
     */
    public JsonNode runOnce(String effectKey, SideEffect effect) throws Exception {
        return idempotence.runOnce(instanceId, stepName, effectKey, effect);
    }
}
