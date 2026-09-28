package com.github.lisama569.durablefunctionruntimejava.workflow;

import java.util.List;

/**
 * 并行扇出节点：同时拉起所有分支的首个任务，各分支内部顺序推进；
 * 全部分支完成后汇合恰好一次，汇合输入按分支定义顺序排列，与完成先后无关。
 *
 * <p>各路任务沿用各自的重试/超时/失败语义：任一路彻底失败或超时，
 * 整个实例立即落入确定终态，并取消其余仍在运行的任务。</p>
 */
public record ParallelNode(String name, List<FlowBranch> branches, JoinHandler join) implements FlowNode {

    public ParallelNode {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("parallel name must not be blank");
        if (branches == null || branches.size() < 2) {
            throw new IllegalArgumentException("parallel must define at least two branches");
        }
        if (join == null) throw new IllegalArgumentException("join handler must not be null");
        branches = List.copyOf(branches);
    }
}
