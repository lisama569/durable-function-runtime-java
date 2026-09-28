package com.github.lisama569.durablefunctionruntimejava.domain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 工作流实例的持久化状态。version 用于乐观并发控制。
 *
 * <p>控制流状态：
 * <ul>
 *   <li>{@code readyNodeIds}：已就绪、待派发的活动节点 id 队列（顺序写入，支持并行扇出多个就绪项）；</li>
 *   <li>{@code choices}：分支选择节点 id -> 命中的分支标签（只记录被选中的那一条）；</li>
 *   <li>{@code openParallels}：已扇出但尚未汇合的并行节点 id；</li>
 *   <li>{@code branchOutputs}：并行节点 id -> (分支标签 -> 输出)，按定义顺序，用于确定性汇合与重启恢复。</li>
 * </ul>
 */
public class WorkflowInstance {
    private String id;
    private String workflowName;
    private String idempotencyKey;
    private WorkflowStatus status = WorkflowStatus.RUNNING;
    private int currentStepIndex;
    private String input;
    private List<StepRecord> steps = new ArrayList<>();
    private List<String> readyNodeIds = new ArrayList<>();
    private Map<String, String> choices = new LinkedHashMap<>();
    private List<String> openParallels = new ArrayList<>();
    private Map<String, Map<String, String>> branchOutputs = new LinkedHashMap<>();
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
    public List<String> getReadyNodeIds() { return readyNodeIds; }
    public void setReadyNodeIds(List<String> readyNodeIds) { this.readyNodeIds = readyNodeIds; }
    public Map<String, String> getChoices() { return choices; }
    public void setChoices(Map<String, String> choices) { this.choices = choices; }
    public List<String> getOpenParallels() { return openParallels; }
    public void setOpenParallels(List<String> openParallels) { this.openParallels = openParallels; }
    public Map<String, Map<String, String>> getBranchOutputs() { return branchOutputs; }
    public void setBranchOutputs(Map<String, Map<String, String>> branchOutputs) { this.branchOutputs = branchOutputs; }
    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
}
