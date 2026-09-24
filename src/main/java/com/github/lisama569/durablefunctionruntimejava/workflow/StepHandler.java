package com.github.lisama569.durablefunctionruntimejava.workflow;

/** 业务步骤逻辑。抛出 RetryableException 表示可重试，其余异常默认不可重试。 */
@FunctionalInterface
public interface StepHandler {
    String execute(StepContext context, String input) throws Exception;
}
