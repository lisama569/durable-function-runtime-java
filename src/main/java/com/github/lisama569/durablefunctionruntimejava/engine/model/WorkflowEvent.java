package com.github.lisama569.durablefunctionruntimejava.engine.model;

/**
 * Append-only audit event. Events form a stable, human-readable trail
 * (instance / step / attempt) that is never rewritten.
 */
public class WorkflowEvent {

    private long seq;
    private long atMs;
    private String type;
    private String stepName;
    private Integer attemptNo;
    private String detail;

    public WorkflowEvent() {
    }

    public WorkflowEvent(long seq, long atMs, String type, String stepName,
                         Integer attemptNo, String detail) {
        this.seq = seq;
        this.atMs = atMs;
        this.type = type;
        this.stepName = stepName;
        this.attemptNo = attemptNo;
        this.detail = detail;
    }

    public long getSeq() { return seq; }
    public void setSeq(long seq) { this.seq = seq; }
    public long getAtMs() { return atMs; }
    public void setAtMs(long atMs) { this.atMs = atMs; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getStepName() { return stepName; }
    public void setStepName(String stepName) { this.stepName = stepName; }
    public Integer getAttemptNo() { return attemptNo; }
    public void setAttemptNo(Integer attemptNo) { this.attemptNo = attemptNo; }
    public String getDetail() { return detail; }
    public void setDetail(String detail) { this.detail = detail; }
}
