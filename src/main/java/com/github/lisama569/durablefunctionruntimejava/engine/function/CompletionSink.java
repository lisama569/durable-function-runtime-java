package com.github.lisama569.durablefunctionruntimejava.engine.function;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Sink through which async work reports completion. Implementations must be
 * safe against duplicate/late/out-of-order calls: only the first completion
 * matching the live attempt token has any effect.
 *
 * @return the owning instance id when the token was live, otherwise
 *         {@code null} (stale/duplicate/unknown token — callers should treat
 *         this as a successful no-op)
 */
public interface CompletionSink {

    String complete(String token, boolean success, JsonNode result,
                    String errorCode, String errorMessage, String dedupeKey);
}
