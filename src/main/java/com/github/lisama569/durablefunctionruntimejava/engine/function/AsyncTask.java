package com.github.lisama569.durablefunctionruntimejava.engine.function;

import com.fasterxml.jackson.databind.JsonNode;

/** Description of one dispatched attempt of an async step. */
public class AsyncTask {

    private final String token;
    private final String instanceId;
    private final String stepName;
    private final int attemptNo;
    private final JsonNode input;
    private final CompletionSink completionSink;

    public AsyncTask(String token, String instanceId, String stepName, int attemptNo,
                     JsonNode input, CompletionSink completionSink) {
        this.token = token;
        this.instanceId = instanceId;
        this.stepName = stepName;
        this.attemptNo = attemptNo;
        this.input = input;
        this.completionSink = completionSink;
    }

    public String token() { return token; }
    public String instanceId() { return instanceId; }
    public String stepName() { return stepName; }
    public int attemptNo() { return attemptNo; }
    public JsonNode input() { return input; }
    public CompletionSink completionSink() { return completionSink; }
}
