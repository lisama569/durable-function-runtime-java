package com.github.lisama569.durablefunctionruntimejava.engine.model;

/**
 * Stable classification of a step failure. The {@code code} is persisted so that
 * failure reasons stay stable across retries and restarts.
 */
public enum ErrorType {
    /** Business/config error that must never be retried. */
    NON_RETRYABLE("NON_RETRYABLE"),
    /** Transient error; eligible for retry up to the policy limit. */
    RETRYABLE("RETRYABLE"),
    /** Attempt exceeded its timeout; treated as retryable until attempts run out. */
    TIMEOUT("TIMEOUT"),
    /** An in-flight attempt was interrupted by an engine restart; retryable. */
    INTERRUPTED("INTERRUPTED"),
    /** Retry budget exhausted; terminal wrapper around the last error. */
    RETRIES_EXHAUSTED("RETRIES_EXHAUSTED"),
    /** Instance was terminated by an explicit request. */
    TERMINATED("TERMINATED"),
    NONE("NONE");

    private final String code;

    ErrorType(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }

    public boolean retryable() {
        return this == RETRYABLE || this == TIMEOUT || this == INTERRUPTED;
    }
}
