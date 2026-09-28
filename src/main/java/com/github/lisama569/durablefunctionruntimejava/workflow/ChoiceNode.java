package com.github.lisama569.durablefunctionruntimejava.workflow;

import java.util.List;

/**
 * 分支选择节点：上游任务完成后由 {@link BranchSelector} 依据其输出选出唯一分支执行，
 * 其余分支的所有任务直接标记为 SKIPPED，不执行、不卡住流程、不影响整体状态与结果。
 */
public record ChoiceNode(String name, BranchSelector selector, List<FlowBranch> branches) implements FlowNode {

    public ChoiceNode {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("choice name must not be blank");
        if (selector == null) throw new IllegalArgumentException("branch selector must not be null");
        if (branches == null || branches.size() < 2) {
            throw new IllegalArgumentException("choice must define at least two branches");
        }
        branches = List.copyOf(branches);
    }

    /** 返回指定 label 的分支，未知 label 返回 null。 */
    public FlowBranch branch(String label) {
        for (FlowBranch branch : branches) {
            if (branch.label().equals(label)) return branch;
        }
        return null;
    }
}
