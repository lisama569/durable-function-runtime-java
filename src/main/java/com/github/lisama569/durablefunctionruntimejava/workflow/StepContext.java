package com.github.lisama569.durablefunctionruntimejava.workflow;

/** 步骤执行上下文：为业务副作用提供稳定的幂等键。 */
public record StepContext(String instanceId, String stepName, int attempt) {

    /** 同一实例同一步骤的幂等键，跨重试与重启保持稳定。 */
    public String idempotencyKey() {
        return instanceId + ":" + stepName;
    }
}
