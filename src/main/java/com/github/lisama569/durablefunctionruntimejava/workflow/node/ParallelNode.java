package com.github.lisama569.durablefunctionruntimejava.workflow.node;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;

import java.util.List;

/**
 * 并行节点：同时拉起多路分支活动，全部成功后汇合（join）一次再继续。
 *
 * <ul>
 *   <li>任一路彻底失败或超时耗尽 -> 整个实例进入确定终态，其余在跑分支被取消；</li>
 *   <li>汇合结果只依赖各分支定义顺序下的输出，与完成顺序无关；</li>
 *   <li>join 本身是一个带重试/超时语义的活动（joinName + joinRetryPolicy）。</li>
 * </ul>
 */
public record ParallelNode(String name,
                           List<ParallelBranch> branches,
                           String joinName,
                           JoinHandler joinHandler,
                           RetryPolicy joinRetryPolicy) implements WorkflowNode {

    public ParallelNode {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("parallel name must not be blank");
        }
        branches = branches == null ? List.of() : List.copyOf(branches);
        if (branches.isEmpty()) {
            throw new IllegalArgumentException("parallel node must have at least one branch");
        }
        if (joinName == null || joinName.isBlank()) {
            throw new IllegalArgumentException("join name must not be blank");
        }
        if (joinHandler == null) {
            throw new IllegalArgumentException("joinHandler must not be null");
        }
        if (joinRetryPolicy == null) {
            throw new IllegalArgumentException("joinRetryPolicy must not be null");
        }
    }
}
