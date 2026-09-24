package com.github.lisama569.durablefunctionruntimejava.workflow;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;

public record StepDefinition(String name, StepHandler handler, RetryPolicy retryPolicy) {
}
