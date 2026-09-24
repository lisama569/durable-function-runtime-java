package com.github.lisama569.durablefunctionruntimejava.engine.function;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Business logic of one step. Implementations must be deterministic with
 * respect to the persisted {@link StepContext#input()} and must route all
 * external effects through {@link StepContext#runOnce}.
 */
@FunctionalInterface
public interface StepFunction {

    JsonNode execute(StepContext context) throws Exception;
}
