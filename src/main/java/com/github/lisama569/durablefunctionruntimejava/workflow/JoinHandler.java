package com.github.lisama569.durablefunctionruntimejava.workflow;

import java.util.List;

/**
 * 并行汇合逻辑：在事件循环线程同步执行，且整个并行节点生命周期内最多执行一次。
 *
 * <p>引擎保证传入 {@code outputs} 时按<b>分支定义顺序</b>排列，
 * 因此结果与各路实际完成的先后顺序无关。</p>
 */
@FunctionalInterface
public interface JoinHandler {
    String join(JoinContext context, List<String> outputsInBranchOrder) throws Exception;
}
