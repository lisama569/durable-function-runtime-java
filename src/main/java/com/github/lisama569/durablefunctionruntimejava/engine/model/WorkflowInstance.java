package com.github.lisama569.durablefunctionruntimejava.engine.model;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Root persisted aggregate: one run of a workflow. */
public class WorkflowInstance {

    private String instanceId;
    private String workflowName;
    private InstanceStatus status = InstanceStatus.RUNNING;
    private int currentStepIndex;
    private int version;
    private long createdAtMs;
    private long updatedAtMs;
    private long completedAtMs;
    private JsonNode input;
    private JsonNode output;
    private String errorType;
    private String errorCode;
    private String errorMessage;
    private Map<String, StepState> steps = new LinkedHashMap<>();
    private List<WorkflowEvent> events = new ArrayList<>();
    private List<String> dedupeKeys = new ArrayList<>();

    public WorkflowInstance() {
    }

    public StepState stepAt(int index) {
        for (StepState s : steps.values()) {
            if (index == 0) {
                return s;
            }
            index--;
        }
        return null;
    }

    public boolean isTerminal() {
        return status.isTerminal();
    }

    public String getInstanceId() { return instanceId; }
    public void setInstanceId(String instanceId) { this.instanceId = instanceId; }
    public String getWorkflowName() { return workflowName; }
    public void setWorkflowName(String workflowName) { this.workflowName = workflowName; }
    public InstanceStatus getStatus() { return status; }
    public void setStatus(InstanceStatus status) { this.status = status; }
    public int getCurrentStepIndex() { return currentStepIndex; }
    public void setCurrentStepIndex(int currentStepIndex) { this.currentStepIndex = currentStepIndex; }
    public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }
    public long getCreatedAtMs() { return createdAtMs; }
    public void setCreatedAtMs(long createdAtMs) { this.createdAtMs = createdAtMs; }
    public long getUpdatedAtMs() { return updatedAtMs; }
    public void setUpdatedAtMs(long updatedAtMs) { this.updatedAtMs = updatedAtMs; }
    public long getCompletedAtMs() { return completedAtMs; }
    public void setCompletedAtMs(long completedAtMs) { this.completedAtMs = completedAtMs; }
    public JsonNode getInput() { return input; }
    public void setInput(JsonNode input) { this.input = input; }
    public JsonNode getOutput() { return output; }
    public void setOutput(JsonNode output) { this.output = output; }
    public String getErrorType() { return errorType; }
    public void setErrorType(String errorType) { this.errorType = errorType; }
    public String getErrorCode() { return errorCode; }
    public void setErrorCode(String errorCode) { this.errorCode = errorCode; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public Map<String, StepState> getSteps() { return steps; }
    public void setSteps(Map<String, StepState> steps) { this.steps = steps; }
    public List<WorkflowEvent> getEvents() { return events; }
    public void setEvents(List<WorkflowEvent> events) { this.events = events; }
    public List<String> getDedupeKeys() { return dedupeKeys; }
    public void setDedupeKeys(List<String> dedupeKeys) { this.dedupeKeys = dedupeKeys; }
}
