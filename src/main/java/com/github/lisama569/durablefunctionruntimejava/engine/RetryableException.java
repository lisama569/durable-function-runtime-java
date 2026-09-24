package com.github.lisama569.durablefunctionruntimejava.engine;

/** 业务主动声明的可重试失败。 */
public class RetryableException extends RuntimeException {
    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
