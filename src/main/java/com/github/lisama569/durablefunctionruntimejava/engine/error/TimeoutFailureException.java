package com.github.lisama569.durablefunctionruntimejava.engine.error;

import com.github.lisama569.durablefunctionruntimejava.engine.model.ErrorType;

/** Raised by the engine when an in-flight attempt passes its deadline. */
public class TimeoutFailureException extends StepFailureException {

    public TimeoutFailureException(String message) {
        super(ErrorType.TIMEOUT, "STEP_TIMEOUT", message);
    }
}
