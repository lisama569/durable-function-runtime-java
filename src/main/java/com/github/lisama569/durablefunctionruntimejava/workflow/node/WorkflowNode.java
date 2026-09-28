package com.github.lisama569.durablefunctionruntimejava.workflow.node;

/**
 * 工作流定义中的节点（控制流图的组成单元）。
 *
 * <ul>
 *   <li>{@link ActivityNode}：执行一段业务逻辑（与原有串行步骤语义一致）；</li>
 *   <li>{@link ChoiceNode}：依据上一个活动的输出选择唯一一条分支，未选分支整体跳过；</li>
 *   <li>{@link ParallelNode}：同时拉起多路活动，全部完成后汇合一次，汇合结果与完成顺序无关。</li>
 * </ul>
 */
public sealed interface WorkflowNode
        permits ActivityNode, ChoiceNode, ParallelNode {

    /** 节点在定义中的展示名（同一父节点下唯一，用于审计与日志）。 */
    String name();
}
