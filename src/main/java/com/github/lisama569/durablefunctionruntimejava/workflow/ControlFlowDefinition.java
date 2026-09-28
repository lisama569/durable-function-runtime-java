package com.github.lisama569.durablefunctionruntimejava.workflow;

import com.github.lisama569.durablefunctionruntimejava.workflow.node.WorkflowNode;

import java.util.ArrayList;
import java.util.List;

/**
 * 含分支选择与并行汇合的工作流定义。
 * 通过 {@link #name(String)} 起步、{@link Builder#build()} 收尾，顶层节点按添加顺序推进。
 */
public record ControlFlowDefinition(String name, List<WorkflowNode> nodes) implements WorkflowDef {

    public ControlFlowDefinition {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("workflow name must not be blank");
        }
        nodes = nodes == null ? List.of() : List.copyOf(nodes);
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("workflow must have at least one node");
        }
    }

    public static Builder name(String name) {
        return new Builder(name);
    }

    /** 顶层节点构建器：按添加顺序执行；分支/并行节点作为一个整体占位。 */
    public static final class Builder {
        private final String name;
        private final List<WorkflowNode> nodes = new ArrayList<>();

        private Builder(String name) {
            this.name = name;
        }

        /** 追加一个顶层节点（活动 / 分支 / 并行）。 */
        public Builder then(WorkflowNode node) {
            nodes.add(node);
            return this;
        }

        public ControlFlowDefinition build() {
            return new ControlFlowDefinition(name, nodes);
        }
    }
}
