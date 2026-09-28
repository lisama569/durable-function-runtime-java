package com.github.lisama569.durablefunctionruntimejava.workflow;

/**
 * 步骤执行上下文：为业务副作用提供稳定的幂等键。
 *
 * @param path 步骤的稳定全路径（如 fanout[0].reserve），跨重试与重启不变
 */
public record StepContext(String instanceId, String stepName, String path, int attempt) {

    /** 兼容纯串行步骤：路径即步骤名。 */
    public StepContext(String instanceId, String stepName, int attempt) {
        this(instanceId, stepName, stepName, attempt);
    }

    /** 同一实例同一步骤（按路径）的幂等键，跨重试与重启保持稳定。 */
    public String idempotencyKey() {
        return instanceId + ":" + path;
    }
}
