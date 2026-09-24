package com.github.lisama569.durablefunctionruntimejava.workflow;

import java.util.List;

/** 一个工作流 = 名称 + 按顺序推进的步骤列表。 */
public record WorkflowDefinition(String name, List<StepDefinition> steps) {
}
