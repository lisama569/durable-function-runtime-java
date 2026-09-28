package com.github.lisama569.durablefunctionruntimejava.workflow;

/**
 * 工作流节点：构成 {@link FlowDefinition} 的执行单元。
 *
 * <ul>
 *   <li>{@link TaskNode}：一个实际执行的业务步骤；</li>
 *   <li>{@link ChoiceNode}：依据上游任务输出选择唯一分支，其余分支整体跳过；</li>
 *   <li>{@link ParallelNode}：同时拉起多个分支并行执行，全部完成后汇合一次。</li>
 * </ul>
 */
public sealed interface FlowNode permits TaskNode, ChoiceNode, ParallelNode {

    /** 节点内唯一标识：用于持久化执行位置与匹配完成通知。 */
    String name();
}
