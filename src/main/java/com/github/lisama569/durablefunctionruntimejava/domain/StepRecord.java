package com.github.lisama569.durablefunctionruntimejava.domain;

/** 单个活动的持久化记录：节点 id、输入、输出、尝试次数与失败原因。 */
public class StepRecord {
    private String nodeId;
    private String name;
    private StepStatus status = StepStatus.PENDING;
    private int attempt;
    private String input;
    private String output;
    private ErrorKind errorKind;
    private String errorMessage;

    public StepRecord() {
    }

    public StepRecord(String name) {
        this.name = name;
    }

    public StepRecord(String nodeId, String name) {
        this.nodeId = nodeId;
        this.name = name;
    }

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public StepStatus getStatus() { return status; }
    public void setStatus(StepStatus status) { this.status = status; }
    public int getAttempt() { return attempt; }
    public void setAttempt(int attempt) { this.attempt = attempt; }
    public String getInput() { return input; }
    public void setInput(String input) { this.input = input; }
    public String getOutput() { return output; }
    public void setOutput(String output) { this.output = output; }
    public ErrorKind getErrorKind() { return errorKind; }
    public void setErrorKind(ErrorKind errorKind) { this.errorKind = errorKind; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}
