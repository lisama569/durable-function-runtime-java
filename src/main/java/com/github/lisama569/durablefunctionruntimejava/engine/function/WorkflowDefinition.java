package com.github.lisama569.durablefunctionruntimejava.engine.function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;

/** An ordered list of steps, optionally with an output projection. */
public class WorkflowDefinition {

    private final String name;
    private final List<StepDefinition> steps;
    private final Function<JsonNode, JsonNode> outputBuilder;

    public WorkflowDefinition(String name, List<StepDefinition> steps,
                              Function<JsonNode, JsonNode> outputBuilder) {
        this.name = name;
        this.steps = Collections.unmodifiableList(new ArrayList<>(steps));
        this.outputBuilder = outputBuilder;
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    public String name() { return name; }
    public List<StepDefinition> steps() { return steps; }
    public Function<JsonNode, JsonNode> outputBuilder() { return outputBuilder; }

    public static class Builder {
        private final String name;
        private final List<StepDefinition> steps = new ArrayList<>();
        private Function<JsonNode, JsonNode> outputBuilder;

        public Builder(String name) {
            this.name = name;
        }

        public Builder addStep(StepDefinition step) {
            this.steps.add(step);
            return this;
        }

        public Builder output(Function<JsonNode, JsonNode> outputBuilder) {
            this.outputBuilder = outputBuilder;
            return this;
        }

        public WorkflowDefinition build() {
            return new WorkflowDefinition(name, steps, outputBuilder);
        }
    }
}
