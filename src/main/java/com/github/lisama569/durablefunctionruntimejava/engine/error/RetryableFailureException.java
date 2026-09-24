package com.github.lisama569.durablefunctionruntimejava.engine.error;

import com.github.lisama569.durablefunctionruntimejava.engine.model.ErrorType;

/** A transient failure that is eligible for retry. */
public class RetryableFailureException extends StepFailureException {

    public RetryableFailureException(String code, String message) {
        super(ErrorType.RETRYABLE, code, message);
    }

    public RetryableFailureException(String code, String message, Throwable cause) {
        super(ErrorType.RETRYABLE, code, message, cause);
    }
}
