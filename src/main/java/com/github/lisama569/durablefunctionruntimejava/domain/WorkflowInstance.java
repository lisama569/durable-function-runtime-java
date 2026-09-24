package com.github.lisama569.durablefunctionruntimejava.domain;

import java.util.ArrayList;
import java.util.List;

/** 工作流实例的持久化状态。version 用于乐观并发控制。 */
public class WorkflowInstance {
    private String id;
    private String workflowName;
    private String idempotencyKey;
    private WorkflowStatus status = WorkflowStatus.RUNNING;
    private int currentStepIndex;
    private String input;
    private List<StepRecord> steps = new ArrayList<>();
    private long createdAt;
    private long updatedAt;
    private long version;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getWorkflowName() { return workflowName; }
    public void setWorkflowName(String workflowName) { this.workflowName = workflowName; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public WorkflowStatus getStatus() { return status; }
    public void setStatus(WorkflowStatus status) { this.status = status; }
    public int getCurrentStepIndex() { return currentStepIndex; }
    public void setCurrentStepIndex(int currentStepIndex) { this.currentStepIndex = currentStepIndex; }
    public String getInput() { return input; }
    public void setInput(String input) { this.input = input; }
    public List<StepRecord> getSteps() { return steps; }
    public void setSteps(List<StepRecord> steps) { this.steps = steps; }
    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
}
