package com.github.lisama569.durablefunctionruntimejava.engine.model;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/** Persisted state of one step of one workflow instance. */
public class StepState {

    private String stepName;
    private StepStatus status = StepStatus.PENDING;
    private int currentAttempt;
    private JsonNode input;
    private JsonNode output;
    private String errorType;
    private String errorCode;
    private String errorMessage;
    private long notBeforeMs;
    private long attemptDeadlineMs;
    private String activeToken;
    private final List<AttemptRecord> attempts = new ArrayList<>();

    public StepState() {
    }

    public StepState(String stepName) {
        this.stepName = stepName;
    }

    public boolean isDone() {
        return status == StepStatus.COMPLETED || status == StepStatus.SKIPPED;
    }

    public String getStepName() { return stepName; }
    public void setStepName(String stepName) { this.stepName = stepName; }
    public StepStatus getStatus() { return status; }
    public void setStatus(StepStatus status) { this.status = status; }
    public int getCurrentAttempt() { return currentAttempt; }
    public void setCurrentAttempt(int currentAttempt) { this.currentAttempt = currentAttempt; }
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
    public long getNotBeforeMs() { return notBeforeMs; }
    public void setNotBeforeMs(long notBeforeMs) { this.notBeforeMs = notBeforeMs; }
    public long getAttemptDeadlineMs() { return attemptDeadlineMs; }
    public void setAttemptDeadlineMs(long attemptDeadlineMs) { this.attemptDeadlineMs = attemptDeadlineMs; }
    public String getActiveToken() { return activeToken; }
    public void setActiveToken(String activeToken) { this.activeToken = activeToken; }
    public List<AttemptRecord> getAttempts() { return attempts; }
}
