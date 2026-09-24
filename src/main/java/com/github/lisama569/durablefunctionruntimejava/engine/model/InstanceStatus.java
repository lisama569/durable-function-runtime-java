package com.github.lisama569.durablefunctionruntimejava.engine.model;

/** Lifecycle status of a workflow instance. */
public enum InstanceStatus {
    RUNNING,
    COMPLETED,
    FAILED,
    TERMINATED;

    public boolean isTerminal() {
        return this != RUNNING;
    }
}
