package com.github.lisama569.durablefunctionruntimejava.engine.function;

import com.github.lisama569.durablefunctionruntimejava.engine.model.RetryPolicy;

import java.util.function.Function;

/** Definition of one step: name, retry policy, sync function or async dispatcher. */
public class StepDefinition {

    private final String name;
    private final RetryPolicy retryPolicy;
    private final StepFunction function;
    private final AsyncDispatcher dispatcher;
    private final Function<com.fasterxml.jackson.databind.JsonNode, com.fasterxml.jackson.databind.JsonNode> inputBuilder;
    private final boolean async;

    private StepDefinition(String name, RetryPolicy retryPolicy, StepFunction function,
                           AsyncDispatcher dispatcher, boolean async,
                           Function<com.fasterxml.jackson.databind.JsonNode,
                                   com.fasterxml.jackson.databind.JsonNode> inputBuilder) {
        this.name = name;
        this.retryPolicy = retryPolicy;
        this.function = function;
        this.dispatcher = dispatcher;
        this.async = async;
        this.inputBuilder = inputBuilder;
    }

    public static StepDefinition sync(String name, RetryPolicy retryPolicy, StepFunction function) {
        return new StepDefinition(name, retryPolicy, function, null, false, null);
    }

    public static StepDefinition sync(String name, RetryPolicy retryPolicy,
                                      Function<com.fasterxml.jackson.databind.JsonNode,
                                              com.fasterxml.jackson.databind.JsonNode> inputBuilder,
                                      StepFunction function) {
        return new StepDefinition(name, retryPolicy, function, null, false, inputBuilder);
    }

    public static StepDefinition async(String name, RetryPolicy retryPolicy, AsyncDispatcher dispatcher) {
        return new StepDefinition(name, retryPolicy, null, dispatcher, true, null);
    }

    public String name() { return name; }
    public RetryPolicy retryPolicy() { return retryPolicy; }
    public StepFunction function() { return function; }
    public AsyncDispatcher dispatcher() { return dispatcher; }
    public boolean async() { return async; }
    public Function<com.fasterxml.jackson.databind.JsonNode,
            com.fasterxml.jackson.databind.JsonNode> inputBuilder() { return inputBuilder; }
}
