package com.github.lisama569.durablefunctionruntimejava.workflow;

import com.github.lisama569.durablefunctionruntimejava.workflow.node.WorkflowNode;

import java.util.List;

/**
 * 工作流定义的统一视图：名称 + 顶层节点序列。
 *
 * <p>既可以是旧的纯串行 {@link WorkflowDefinition}（每个 step 即一个顶层活动节点），
 * 也可以是含分支/并行节点的新定义，引擎对两者使用同一套执行机制。
 */
public interface WorkflowDef {

    String name();

    List<WorkflowNode> nodes();
}
