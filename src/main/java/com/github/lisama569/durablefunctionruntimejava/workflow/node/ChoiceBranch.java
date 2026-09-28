package com.github.lisama569.durablefunctionruntimejava.workflow.node;

import java.util.List;
import java.util.function.Predicate;

/**
 * 条件分支：when 命中（或 when 为 null 表示兜底分支）时，按顺序执行 body。
 * 同一 ChoiceNode 内 label 唯一；至多一条兜底分支。
 */
public record ChoiceBranch(String label, Predicate<String> when, List<WorkflowNode> body) {

    public ChoiceBranch {
        if (label == null || label.isBlank()) {
            throw new IllegalArgumentException("choice branch label must not be blank");
        }
        body = body == null ? List.of() : List.copyOf(body);
    }

    /** 是否为兜底（default）分支。 */
    public boolean isFallback() {
        return when == null;
    }
}
