package com.github.lisama569.durablefunctionruntimejava.engine;

import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;

import java.util.List;
import java.util.Optional;

/** 实例状态持久化抽象。 */
public interface WorkflowStore {

    /** 原子创建：同 id 或同幂等键已存在时返回 false。 */
    boolean createIfAbsent(WorkflowInstance instance);

    /** 基于 version 的乐观并发更新，版本冲突返回 false。 */
    boolean update(WorkflowInstance instance);

    Optional<WorkflowInstance> findById(String instanceId);

    Optional<WorkflowInstance> findByIdempotencyKey(String workflowName, String idempotencyKey);

    List<WorkflowInstance> findAll();
}
