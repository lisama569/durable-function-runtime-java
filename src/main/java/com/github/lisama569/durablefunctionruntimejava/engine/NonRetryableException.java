package com.github.lisama569.durablefunctionruntimejava.engine;

/** 业务主动声明的不可重试失败，立即落入 FAILED 终态。 */
public class NonRetryableException extends RuntimeException {
    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
