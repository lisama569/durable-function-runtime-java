package com.github.lisama569.durablefunctionruntimejava.workflow;

/** 步骤执行上下文：为业务副作用提供稳定的幂等键。 */
public record StepContext(String instanceId, String nodeId, String stepName, int attempt) {

    /** 兼容旧用法：nodeId 退化为 stepName（串行顶层活动的 nodeId 即其名称）。 */
    public StepContext(String instanceId, String stepName, int attempt) {
        this(instanceId, stepName, stepName, attempt);
    }

    /** 同一实例同一活动节点的幂等键，跨重试与重启保持稳定。 */
    public String idempotencyKey() {
        return instanceId + ":" + nodeId;
    }
}
