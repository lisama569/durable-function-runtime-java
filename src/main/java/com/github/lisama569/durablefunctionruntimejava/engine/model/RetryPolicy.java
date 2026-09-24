package com.github.lisama569.durablefunctionruntimejava.engine.model;

/**
 * Retry/timeout policy for a step. All bounds are explicit and hard-capped by
 * the engine regardless of user-supplied values.
 */
public class RetryPolicy {

    public static final int HARD_MAX_ATTEMPTS = 20;
    public static final long HARD_MAX_BACKOFF_MS = 60_000L;
    public static final long HARD_MAX_TIMEOUT_MS = 300_000L;

    public static final RetryPolicy DEFAULT =
            new RetryPolicy(3, 100L, 2.0, 1_000L, 5_000L);
    public static final RetryPolicy NO_RETRY =
            new RetryPolicy(1, 0L, 1.0, 0L, 5_000L);

    private int maxAttempts;
    private long initialBackoffMs;
    private double backoffMultiplier;
    private long maxBackoffMs;
    private long timeoutMs;

    public RetryPolicy() {
    }

    public RetryPolicy(int maxAttempts, long initialBackoffMs, double backoffMultiplier,
                       long maxBackoffMs, long timeoutMs) {
        this.maxAttempts = maxAttempts;
        this.initialBackoffMs = initialBackoffMs;
        this.backoffMultiplier = backoffMultiplier;
        this.maxBackoffMs = maxBackoffMs;
        this.timeoutMs = timeoutMs;
    }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getInitialBackoffMs() { return initialBackoffMs; }
    public void setInitialBackoffMs(long initialBackoffMs) { this.initialBackoffMs = initialBackoffMs; }
    public double getBackoffMultiplier() { return backoffMultiplier; }
    public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
    public long getMaxBackoffMs() { return maxBackoffMs; }
    public void setMaxBackoffMs(long maxBackoffMs) { this.maxBackoffMs = maxBackoffMs; }
    public long getTimeoutMs() { return timeoutMs; }
    public void setTimeoutMs(long timeoutMs) { this.timeoutMs = timeoutMs; }
}
