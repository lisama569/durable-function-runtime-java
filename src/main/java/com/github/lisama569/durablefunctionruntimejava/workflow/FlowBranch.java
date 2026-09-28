package com.github.lisama569.durablefunctionruntimejava.workflow;

import java.util.List;

/**
 * 一条分支：唯一 label + 顺序执行的任务列表。
 * 当前版本分支内部只支持 {@link TaskNode}（不支持嵌套分支/并行）。
 */
public record FlowBranch(String label, List<TaskNode> tasks) {

    public FlowBranch {
        if (label == null || label.isBlank()) throw new IllegalArgumentException("branch label must not be blank");
        if (tasks == null || tasks.isEmpty()) throw new IllegalArgumentException("branch must contain at least one task");
        tasks = List.copyOf(tasks);
    }
}
