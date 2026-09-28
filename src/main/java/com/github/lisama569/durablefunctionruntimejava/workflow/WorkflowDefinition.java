package com.github.lisama569.durablefunctionruntimejava.workflow;

import com.github.lisama569.durablefunctionruntimejava.workflow.node.ActivityNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.WorkflowNode;

import java.util.List;

/**
 * 一个工作流 = 名称 + 按顺序推进的步骤列表（旧版纯串行定义，保持兼容）。
 *
 * <p>适配为统一视图后，每个 {@link StepDefinition} 即一个顶层 {@link ActivityNode}，
 * 因此旧定义在新引擎中的行为与原先完全一致。
 */
public record WorkflowDefinition(String name, List<StepDefinition> steps) implements WorkflowDef {

    public WorkflowDefinition {
        steps = steps == null ? List.of() : List.copyOf(steps);
    }

    @Override
    public List<WorkflowNode> nodes() {
        return steps.stream()
                .map(s -> (WorkflowNode) new ActivityNode(s.name(), s.handler(), s.retryPolicy()))
                .toList();
    }
}
