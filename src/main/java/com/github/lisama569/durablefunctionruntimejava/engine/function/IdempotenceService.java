package com.github.lisama569.durablefunctionruntimejava.engine.function;

import com.fasterxml.jackson.databind.JsonNode;

/** Persistence-backed suppression of duplicate side effects. */
public interface IdempotenceService {

    JsonNode runOnce(String instanceId, String stepName, String effectKey,
                     StepContext.SideEffect effect) throws Exception;
}
