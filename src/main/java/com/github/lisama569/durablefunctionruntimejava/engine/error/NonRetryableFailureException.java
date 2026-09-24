package com.github.lisama569.durablefunctionruntimejava.engine.error;

import com.github.lisama569.durablefunctionruntimejava.engine.model.ErrorType;

/** A permanent failure; the engine must fail the instance immediately. */
public class NonRetryableFailureException extends StepFailureException {

    public NonRetryableFailureException(String code, String message) {
        super(ErrorType.NON_RETRYABLE, code, message);
    }

    public NonRetryableFailureException(String code, String message, Throwable cause) {
        super(ErrorType.NON_RETRYABLE, code, message, cause);
    }
}
