package com.github.lisama569.durablefunctionruntimejava.engine.model;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Completion callback for an async step. {@code token} identifies the exact
 * (instance, step, attempt) that completed; stale or duplicate tokens are
 * rejected without effect.
 */
public class CompletionRequest {

    private String token;
    private boolean success;
    private JsonNode result;
    private String errorCode;
    private String errorMessage;
    private String dedupeKey;

    public String getToken() { return token; }
    public void setToken(String token) { this.token = token; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public JsonNode getResult() { return result; }
    public void setResult(JsonNode result) { this.result = result; }
    public String getErrorCode() { return errorCode; }
    public void setErrorCode(String errorCode) { this.errorCode = errorCode; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public String getDedupeKey() { return dedupeKey; }
    public void setDedupeKey(String dedupeKey) { this.dedupeKey = dedupeKey; }
}
