package com.github.lisama569.durablefunctionruntimejava.domain;

/**
 * 并行节点中单条分支的执行游标（持久化）。
 * 分支内任务顺序推进；status 记录分支整体状态。
 */
public class ParallelCursor {

    /** 分支在节点定义中的序号（汇合排序依据，与完成先后无关）。 */
    private int branchIndex;
    /** 分支 label。 */
    private String branchLabel;
    /** 分支内下一个待执行任务的序号。 */
    private int taskIndex;
    private BranchRunStatus status = BranchRunStatus.RUNNING;

    public ParallelCursor() {
    }

    public ParallelCursor(int branchIndex, String branchLabel, int taskIndex) {
        this.branchIndex = branchIndex;
        this.branchLabel = branchLabel;
        this.taskIndex = taskIndex;
    }

    public int getBranchIndex() { return branchIndex; }
    public void setBranchIndex(int branchIndex) { this.branchIndex = branchIndex; }
    public String getBranchLabel() { return branchLabel; }
    public void setBranchLabel(String branchLabel) { this.branchLabel = branchLabel; }
    public int getTaskIndex() { return taskIndex; }
    public void setTaskIndex(int taskIndex) { this.taskIndex = taskIndex; }
    public BranchRunStatus getStatus() { return status; }
    public void setStatus(BranchRunStatus status) { this.status = status; }
}
