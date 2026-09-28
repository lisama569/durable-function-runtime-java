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
    /** 分支/并行模型的执行位置；旧串行实例（历史数据）为 null，恢复时惰性迁移。 */
    private ExecutionState executionState;
    /** 实例级失败归因（如分支选择器/汇合逻辑抛出异常）。 */
    private ErrorKind errorKind;
    private String errorMessage;
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
    public ExecutionState getExecutionState() { return executionState; }
    public void setExecutionState(ExecutionState executionState) { this.executionState = executionState; }
    public ErrorKind getErrorKind() { return errorKind; }
    public void setErrorKind(ErrorKind errorKind) { this.errorKind = errorKind; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
}
