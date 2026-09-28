package com.github.lisama569.durablefunctionruntimejava.workflow.node;

import java.util.List;

/**
 * 分支选择节点：依据上游活动输出，在多条分支中选择唯一一条执行。
 * 未被选中的分支内活动全部标记为 SKIPPED，不执行、不阻塞、也不影响实例终态与结果。
 */
public record ChoiceNode(String name, List<ChoiceBranch> branches) implements WorkflowNode {

    public ChoiceNode {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("choice name must not be blank");
        }
        branches = branches == null ? List.of() : List.copyOf(branches);
        if (branches.isEmpty()) {
            throw new IllegalArgumentException("choice node must have at least one branch");
        }
    }
}
