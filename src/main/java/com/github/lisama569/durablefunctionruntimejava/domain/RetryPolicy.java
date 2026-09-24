package com.github.lisama569.durablefunctionruntimejava.domain;

/**
 * 重试策略：最大尝试次数、初始退避、退避上限与单步超时，均有明确上界。
 */
public record RetryPolicy(int maxAttempts,
                          long initialBackoffMillis,
                          long maxBackoffMillis,
                          long stepTimeoutMillis) {

    public RetryPolicy {
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
        if (initialBackoffMillis < 0) throw new IllegalArgumentException("initialBackoffMillis must be >= 0");
        if (maxBackoffMillis < initialBackoffMillis) throw new IllegalArgumentException("maxBackoffMillis must be >= initialBackoffMillis");
        if (stepTimeoutMillis <= 0) throw new IllegalArgumentException("stepTimeoutMillis must be > 0");
    }

    public static RetryPolicy defaults() {
        return new RetryPolicy(3, 100, 2_000, 30_000);
    }

    /** 第 attempt 次失败后的退避时长（指数退避，封顶 maxBackoffMillis）。 */
    public long backoffForAttempt(int attempt) {
        long backoff = initialBackoffMillis;
        for (int i = 1; i < attempt; i++) {
            backoff = Math.min(backoff * 2, maxBackoffMillis);
        }
        return Math.min(backoff, maxBackoffMillis);
    }
}
