package com.github.lisama569.durablefunctionruntimejava.engine.model;

/** Persisted record of one attempt of a step (part of the retry audit trail). */
public class AttemptRecord {

    private int attemptNo;
    private long startedAtMs;
    private long finishedAtMs;
    private String outcome; // SUCCESS / FAILED / TIMED_OUT / INTERRUPTED
    private String token;   // async completion token bound to this attempt (if any)
    private String errorType;
    private String errorCode;
    private String errorMessage;

    public AttemptRecord() {
    }

    public static AttemptRecord start(int attemptNo, long nowMs) {
        AttemptRecord r = new AttemptRecord();
        r.attemptNo = attemptNo;
        r.startedAtMs = nowMs; // caller (engine) supplies the clock
        r.outcome = "RUNNING";
        r.errorType = ErrorType.NONE.code();
        return r;
    }

    public void finish(String outcome, ErrorType type, String code, String message, long nowMs) {
        this.outcome = outcome;
        this.errorType = type == null ? ErrorType.NONE.code() : type.code();
        this.errorCode = code;
        this.errorMessage = message;
        this.finishedAtMs = nowMs;
    }

    public int getAttemptNo() { return attemptNo; }
    public void setAttemptNo(int attemptNo) { this.attemptNo = attemptNo; }
    public long getStartedAtMs() { return startedAtMs; }
    public void setStartedAtMs(long startedAtMs) { this.startedAtMs = startedAtMs; }
    public long getFinishedAtMs() { return finishedAtMs; }
    public void setFinishedAtMs(long finishedAtMs) { this.finishedAtMs = finishedAtMs; }
    public String getOutcome() { return outcome; }
    public void setOutcome(String outcome) { this.outcome = outcome; }
    public String getToken() { return token; }
    public void setToken(String token) { this.token = token; }
    public String getErrorType() { return errorType; }
    public void setErrorType(String errorType) { this.errorType = errorType; }
    public String getErrorCode() { return errorCode; }
    public void setErrorCode(String errorCode) { this.errorCode = errorCode; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}
