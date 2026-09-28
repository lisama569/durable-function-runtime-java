package com.github.lisama569.durablefunctionruntimejava.workflow;

/** 汇合上下文：为汇合副作用提供稳定的幂等键。 */
public record JoinContext(String instanceId, String nodeName) {

    /** 同一实例同一并行节点的汇合幂等键，跨重启保持稳定。 */
    public String idempotencyKey() {
        return instanceId + ":" + nodeName + ":join";
    }
}
