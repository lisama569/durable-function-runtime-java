package com.github.lisama569.durablefunctionruntimejava.workflow;

import java.util.List;

/**
 * 支持分支选择与并行汇合的工作流定义：顶层节点按顺序推进。
 *
 * <p>{@link WorkflowDefinition}（纯串行步骤列表）继续保留并被兼容，
 * 新能力通过本定义表达。</p>
 */
public record FlowDefinition(String name, List<FlowNode> nodes) {

    public FlowDefinition {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("workflow name must not be blank");
        if (nodes == null || nodes.isEmpty()) throw new IllegalArgumentException("workflow must contain nodes");
        nodes = List.copyOf(nodes);
    }

    /** 便捷工厂：从既有纯串行步骤定义构造（顺序执行，无分支）。 */
    public static FlowDefinition ofSteps(String name, List<StepDefinition> steps) {
        List<FlowNode> nodes = steps.stream()
                .map(s -> (FlowNode) new TaskNode(s.name(), s.handler(), s.retryPolicy()))
                .toList();
        return new FlowDefinition(name, nodes);
    }
}
