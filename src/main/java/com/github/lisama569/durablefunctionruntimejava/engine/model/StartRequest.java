package com.github.lisama569.durablefunctionruntimejava.engine.model;

import com.fasterxml.jackson.databind.JsonNode;

/** Request to start a workflow instance. */
public class StartRequest {

    private String workflowName;
    private String instanceId;
    private JsonNode input;
    private String dedupeKey;

    public String getWorkflowName() { return workflowName; }
    public void setWorkflowName(String workflowName) { this.workflowName = workflowName; }
    public String getInstanceId() { return instanceId; }
    public void setInstanceId(String instanceId) { this.instanceId = instanceId; }
    public JsonNode getInput() { return input; }
    public void setInput(JsonNode input) { this.input = input; }
    public String getDedupeKey() { return dedupeKey; }
    public void setDedupeKey(String dedupeKey) { this.dedupeKey = dedupeKey; }
}
