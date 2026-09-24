package com.github.lisama569.durablefunctionruntimejava.engine.persistence;

import java.util.List;
import java.util.function.Function;

import com.github.lisama569.durablefunctionruntimejava.engine.model.WorkflowInstance;

/**
 * Persistent storage for workflow instances.
 *
 * <p>{@link #mutate} executes the mutation while holding an exclusive
 * per-instance lock and commits the aggregate atomically (temp-file + rename
 * on local disk). The mutator receives {@code null} when the instance does
 * not exist; returning {@code null} aborts the commit without any side
 * effect. All state transitions of an instance go through this method, so
 * callbacks, drives and recoveries are serialized per instance while
 * different instances never block each other.
 */
public interface StateStore {

    /** Insert a new instance. Fails if an instance with the same id exists. */
    void insert(WorkflowInstance instance);

    /** Load an instance, or {@code null} if absent. */
    WorkflowInstance load(String instanceId);

    /**
     * Lock, load, mutate and atomically persist an instance.
     *
     * @param create if true the mutator may return a brand-new instance when
     *               none existed; if false absent instances yield {@code null}
     */
    WorkflowInstance mutate(String instanceId, boolean create,
                            Function<WorkflowInstance, WorkflowInstance> mutator);

    /** Persisted side-effect payload, or {@code null} if never recorded. */
    byte[] loadEffect(String instanceId, String stepName, String effectKey);

    /** Persist a side-effect payload (idempotent overwrite). */
    void saveEffect(String instanceId, String stepName, String effectKey, byte[] payload);

    /** List all known instance ids. */
    List<String> listInstanceIds();
}
