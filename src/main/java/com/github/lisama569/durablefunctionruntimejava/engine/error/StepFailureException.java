package com.github.lisama569.durablefunctionruntimejava.engine.error;

import com.github.lisama569.durablefunctionruntimejava.engine.model.ErrorType;

/**
 * Base failure raised by step business logic. Carries a stable {@code code}
 * that distinguishes failure reasons in persisted state.
 */
public class StepFailureException extends RuntimeException {

    private final ErrorType errorType;
    private final String code;

    public StepFailureException(ErrorType errorType, String code, String message) {
        super(message);
        this.errorType = errorType;
        this.code = code;
    }

    public StepFailureException(ErrorType errorType, String code, String message, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
        this.code = code;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public String getCode() {
        return code;
    }
}
