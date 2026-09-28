package com.github.lisama569.durablefunctionruntimejava.workflow.node;

import com.github.lisama569.durablefunctionruntimejava.workflow.StepContext;

/**
 * 汇合逻辑：所有并行分支完成后恰好执行一次。
 * 入参 {@link BranchResults} 按分支定义顺序排列，与各分支完成先后无关，
 * 因此同样的分支输出必然得到同样的汇合结果。
 */
@FunctionalInterface
public interface JoinHandler {
    String join(StepContext context, BranchResults results) throws Exception;
}
