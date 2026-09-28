package com.github.lisama569.durablefunctionruntimejava.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.durablefunctionruntimejava.domain.ErrorKind;
import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.domain.StepRecord;
import com.github.lisama569.durablefunctionruntimejava.domain.StepStatus;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowStatus;
import com.github.lisama569.durablefunctionruntimejava.workflow.StepContext;
import com.github.lisama569.durablefunctionruntimejava.workflow.WorkflowDef;
import com.github.lisama569.durablefunctionruntimejava.workflow.WorkflowDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.node.BranchResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 持久化工作流执行引擎（串行 + 条件分支 + 并行汇合）。
 *
 * 执行模型：
 * <ul>
 *   <li>所有状态流转只发生在单线程事件循环中，同一实例的状态迁移唯一、有序；</li>
 *   <li>业务活动在 worker 线程池执行，完成后向事件循环投递携带
 *       (instanceId, nodeId, attempt) 的完成通知；迟到/重复通知因状态或 attempt 不匹配被丢弃；</li>
 *   <li>每次状态迁移先持久化再推进，重启后已完成的活动（含并行分支）不会重复执行；</li>
 *   <li>并行分支共享全局在途名额 {@code maxParallelInFlight}（FIFO 回退队列），
 *       单实例扇出宽度受 {@code maxBranchesPerInstance} 约束，避免饿死其他实例。</li>
 * </ul>
 */
public class WorkflowEngine {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    /** 单个实例允许同时扇出的并行分支数上限（定义期校验的默认值）。 */
    public static final int DEFAULT_MAX_BRANCHES_PER_INSTANCE = 16;
    /** 整个引擎同时在跑的并行分支活动数上限。 */
    public static final int DEFAULT_MAX_PARALLEL_IN_FLIGHT = 8;

    private enum EventKind { RUN_NODE, NODE_SUCCEEDED, NODE_FAILED, NODE_TIMED_OUT, TERMINATE, PUMP, REPAIR }

    private record EngineEvent(EventKind kind, String instanceId, String nodeId, int attempt,
                               String output, ErrorKind errorKind, String errorMessage,
                               boolean holdsPermit) {
        static EngineEvent runNode(String instanceId, String nodeId, int attempt, boolean holdsPermit) {
            return new EngineEvent(EventKind.RUN_NODE, instanceId, nodeId, attempt,
                    null, null, null, holdsPermit);
        }

        static EngineEvent succeeded(String instanceId, String nodeId, int attempt, String output) {
            return new EngineEvent(EventKind.NODE_SUCCEEDED, instanceId, nodeId, attempt, output, null, null, false);
        }

        static EngineEvent failed(String instanceId, String nodeId, int attempt, ErrorKind kind, String message) {
            return new EngineEvent(EventKind.NODE_FAILED, instanceId, nodeId, attempt, null, kind, message, false);
        }

        static EngineEvent timedOut(String instanceId, String nodeId, int attempt) {
            return new EngineEvent(EventKind.NODE_TIMED_OUT, instanceId, nodeId, attempt,
                    null, ErrorKind.TIMEOUT, "node timed out", false);
        }

        static EngineEvent terminate(String instanceId) {
            return new EngineEvent(EventKind.TERMINATE, instanceId, null, -1, null, null, null, false);
        }

        /** worker 释放并行名额后唤醒派发（仅触发 pumpDispatch，无实际载荷）。 */
        static EngineEvent pumpTick() {
            return new EngineEvent(EventKind.PUMP, null, null, -1, null, null, null, false);
        }

        /** 重启后对一个实例做控制流自愈（在事件循环线程执行）。 */
        static EngineEvent repair(String instanceId) {
            return new EngineEvent(EventKind.REPAIR, instanceId, null, -1, null, null, null, false);
        }
    }

    private final WorkflowStore store;
    private final int maxBranchesPerInstance;
    private final int maxParallelInFlight;
    private final Map<String, CompiledFlow> flows = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<EngineEvent> eventQueue = new LinkedBlockingQueue<>();
    private final ExecutorService eventLoop;
    private final ExecutorService workers;
    private final ScheduledExecutorService scheduler;
    /** 运行中的活动 future：key = instanceId:nodeId:attempt，用于超时/失败/终止时取消。 */
    private final Map<String, Future<?>> runningTasks = new ConcurrentHashMap<>();
    /** 待派发的就绪活动（FIFO，跨线程生产、事件循环消费）。 */
    private final java.util.concurrent.LinkedBlockingDeque<ReadyActivity> dispatchBacklog =
            new java.util.concurrent.LinkedBlockingDeque<>();
    /** 当前持有并行名额的在途活动总数。 */
    private final AtomicInteger parallelInFlight = new AtomicInteger();
    private volatile boolean running = true;

    private record ReadyActivity(String instanceId, String nodeId) {
    }

    public WorkflowEngine(WorkflowStore store) {
        this(store, DEFAULT_MAX_BRANCHES_PER_INSTANCE, DEFAULT_MAX_PARALLEL_IN_FLIGHT);
    }

    public WorkflowEngine(WorkflowStore store, int maxBranchesPerInstance, int maxParallelInFlight) {
        if (maxBranchesPerInstance < 1) {
            throw new IllegalArgumentException("maxBranchesPerInstance must be >= 1");
        }
        if (maxParallelInFlight < 1) {
            throw new IllegalArgumentException("maxParallelInFlight must be >= 1");
        }
        this.store = store;
        this.maxBranchesPerInstance = maxBranchesPerInstance;
        this.maxParallelInFlight = maxParallelInFlight;
        this.eventLoop = Executors.newSingleThreadExecutor(r -> new Thread(r, "workflow-event-loop"));
        this.workers = Executors.newCachedThreadPool(r -> new Thread(r, "workflow-worker"));
        this.scheduler = Executors.newScheduledThreadPool(2, r -> new Thread(r, "workflow-scheduler"));
        this.eventLoop.submit(this::drainEvents);
    }

    /** 注册旧版纯串行定义（兼容入口）。 */
    public void register(WorkflowDefinition definition) {
        registerFlow(definition);
    }

    /** 注册含分支/并行节点的定义。 */
    public void register(WorkflowDef definition) {
        registerFlow(definition);
    }

    private void registerFlow(WorkflowDef definition) {
        CompiledFlow flow = CompiledFlow.compile(definition);
        validateBranchLimits(flow);
        flows.put(definition.name(), flow);
    }

    private void validateBranchLimits(CompiledFlow flow) {
        // allActivityIds 只含活动；这里通过根序列递归覆盖所有控制节点（含分支体内嵌套）。
        for (String rootId : flow.rootIds()) {
            validateNode(flow, rootId);
        }
    }

    private void validateNode(CompiledFlow flow, String nodeId) {
        CompiledFlow.NodeInfo info = flow.node(nodeId);
        switch (info.kind) {
            case ACTIVITY -> { }
            case PARALLEL -> {
                if (info.parallelArms.size() > maxBranchesPerInstance) {
                    throw new IllegalArgumentException(
                            "parallel node " + info.id + " fans out " + info.parallelArms.size()
                                    + " branches, exceeds maxBranchesPerInstance=" + maxBranchesPerInstance);
                }
            }
            case CHOICE -> {
                for (CompiledFlow.ChoiceArm arm : info.choiceArms) {
                    for (String childId : arm.childIds) {
                        validateNode(flow, childId);
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------- 对外 API

    /** 幂等启动：同 (workflowName, idempotencyKey) 重复提交返回已有实例，不产生新副作用。 */
    public WorkflowInstance start(String workflowName, String idempotencyKey, String input) {
        CompiledFlow flow = requireFlow(workflowName);
        Optional<WorkflowInstance> existing = store.findByIdempotencyKey(workflowName, idempotencyKey);
        if (existing.isPresent()) {
            log.info("[instance={}] duplicate start ignored (idempotencyKey={})", existing.get().getId(), idempotencyKey);
            return existing.get();
        }

        WorkflowInstance instance = new WorkflowInstance();
        instance.setId(UUID.randomUUID().toString());
        instance.setWorkflowName(workflowName);
        instance.setIdempotencyKey(idempotencyKey);
        instance.setInput(input);
        instance.setStatus(WorkflowStatus.RUNNING);
        instance.setCreatedAt(System.currentTimeMillis());
        instance.setUpdatedAt(instance.getCreatedAt());
        // 预创建全部活动记录（与旧串行模型一致：steps 数量 = 活动数量）。
        for (String nodeId : flow.allActivityIds()) {
            CompiledFlow.NodeInfo info = flow.node(nodeId);
            instance.getSteps().add(new StepRecord(nodeId, info.activity.displayName));
        }
        // 激活根序列首节点（活动 / 分支 / 并行），先把全部就绪状态在内存中算好，再一次性落盘。
        List<String> toEnqueue = new ArrayList<>();
        if (!flow.rootIds().isEmpty()) {
            activateInitialNode(instance, flow, flow.rootIds().get(0), input, toEnqueue);
        }
        if (!store.createIfAbsent(instance)) {
            // 并发下同幂等键被抢先创建，返回已存在实例。
            return store.findByIdempotencyKey(workflowName, idempotencyKey).orElseThrow();
        }
        log.info("[instance={}] workflow={} started, activities={}",
                instance.getId(), workflowName, flow.allActivityIds().size());
        // 实例落盘后再入派发队列并唤醒事件循环，避免 PUMP 被提前消费导致永久停摆。
        for (String nodeId : toEnqueue) {
            enqueueReady(instance.getId(), nodeId);
        }
        eventQueue.add(EngineEvent.pumpTick());
        return instance;
    }

    /** 启动时激活根序列首节点：普通活动直接就绪；分支做选择；并行扇出。 */
    private void activateInitialNode(WorkflowInstance instance, CompiledFlow flow, String nodeId,
                                     String input, List<String> toEnqueue) {
        CompiledFlow.NodeInfo node = flow.node(nodeId);
        switch (node.kind) {
            case ACTIVITY -> {
                requireStep(instance, nodeId).setInput(input);
                instance.getReadyNodeIds().add(nodeId);
                toEnqueue.add(nodeId);
            }
            case CHOICE -> chooseBranch(instance, node, input, toEnqueue);
            case PARALLEL -> fanOut(instance, node, input, toEnqueue);
        }
    }

    public Optional<WorkflowInstance> get(String instanceId) {
        return store.findById(instanceId);
    }

    /** 请求终止：实例立即进入 TERMINATED 终态，取消全部在跑活动，已完成活动不回滚。 */
    public boolean terminate(String instanceId) {
        return store.findById(instanceId)
                .filter(i -> i.getStatus() == WorkflowStatus.RUNNING)
                .map(i -> {
                    eventQueue.add(EngineEvent.terminate(instanceId));
                    return true;
                })
                .orElse(false);
    }

    /** 重启恢复：未完结实例从持久化的就绪/在途状态继续，已完成活动不重复执行。返回恢复的实例数。 */
    public int recover() {
        int count = 0;
        for (WorkflowInstance loaded : store.findAll()) {
            if (loaded.getStatus() != WorkflowStatus.RUNNING) {
                continue;
            }
            WorkflowInstance instance = migrateIfLegacy(loaded);
            reconcileRunningInstance(instance);
            // 自愈在事件循环中进行（可能需要继续推进已选择/已汇合但未派发的节点）。
            eventQueue.add(EngineEvent.repair(instance.getId()));
            count++;
        }
        // 仅事件循环线程可以操作派发队列，这里通过事件唤醒。
        eventQueue.add(EngineEvent.pumpTick());
        return count;
    }

    /**
     * 兼容旧版持久化文件：旧实例只有 steps + currentStepIndex，没有控制流字段。
     * 为每个活动补 nodeId，并按旧的当前步骤重建就绪队列。
     */
    private WorkflowInstance migrateIfLegacy(WorkflowInstance instance) {
        if (instance.getReadyNodeIds() != null && !instance.getReadyNodeIds().isEmpty()) {
            return instance;
        }
        CompiledFlow flow = requireFlow(instance.getWorkflowName());
        boolean legacy = instance.getSteps().stream().anyMatch(s -> s.getNodeId() == null);
        if (!legacy) {
            return instance;
        }
        List<String> activityIds = flow.allActivityIds();
        for (int i = 0; i < instance.getSteps().size() && i < activityIds.size(); i++) {
            StepRecord record = instance.getSteps().get(i);
            String nodeId = activityIds.get(i);
            record.setNodeId(nodeId);
            if (record.getName() == null) {
                record.setName(flow.node(nodeId).activity.displayName);
            }
        }
        int index = Math.min(instance.getCurrentStepIndex(), activityIds.size() - 1);
        instance.getReadyNodeIds().add(activityIds.get(index));
        instance.setOpenParallels(new ArrayList<>());
        instance.setChoices(new LinkedHashMap<>());
        instance.setBranchOutputs(new LinkedHashMap<>());
        store.update(instance);
        log.info("[instance={}] migrated legacy state, ready={}", instance.getId(), activityIds.get(index));
        return instance;
    }

    /**
     * 恢复一个 RUNNING 实例：崩溃时处于 RUNNING 的活动作为新 attempt 重跑；
     * PENDING 的就绪活动重新入派发队列。只改内存/队列，由调用方统一落盘。
     */
    private void reconcileRunningInstance(WorkflowInstance instance) {
        for (StepRecord record : instance.getSteps()) {
            if (record.getStatus() == StepStatus.RUNNING) {
                record.setStatus(StepStatus.PENDING);
                record.setErrorKind(ErrorKind.TIMEOUT);
                record.setErrorMessage("interrupted by restart");
                record.setAttempt(record.getAttempt() + 1);
                String nodeId = record.getNodeId();
                if (!instance.getReadyNodeIds().contains(nodeId)) {
                    instance.getReadyNodeIds().add(nodeId);
                }
                log.info("[instance={}] activity={} interrupted, resume as attempt={}",
                        instance.getId(), nodeId, record.getAttempt());
            }
        }
        for (String nodeId : new ArrayList<>(instance.getReadyNodeIds())) {
            StepRecord record = requireStep(instance, nodeId);
            if (record.getStatus() == StepStatus.PENDING) {
                enqueueReady(instance.getId(), nodeId);
            }
        }
        store.update(instance);
    }

    /** 测试辅助：等待实例进入终态，超时返回 null。 */
    public WorkflowInstance awaitTerminal(String instanceId, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            Optional<WorkflowInstance> instance = store.findById(instanceId);
            if (instance.isPresent() && instance.get().getStatus() != WorkflowStatus.RUNNING) {
                return instance.get();
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    public void shutdown() {
        running = false;
        eventLoop.shutdownNow();
        workers.shutdownNow();
        scheduler.shutdownNow();
    }

    // ---------------------------------------------------------------- 事件循环

    private void drainEvents() {
        while (running || !eventQueue.isEmpty()) {
            EngineEvent event;
            try {
                event = eventQueue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                handle(event);
            } catch (Exception e) {
                log.error("[instance={}] event {} handling failed", event.instanceId(), event.kind(), e);
            }
            pumpDispatch();
        }
    }

    private void handle(EngineEvent event) {
        switch (event.kind()) {
            case RUN_NODE -> onRunNode(event);
            case NODE_SUCCEEDED -> onNodeSucceeded(event);
            case NODE_FAILED -> onNodeFailed(event);
            case NODE_TIMED_OUT -> onNodeTimedOut(event);
            case TERMINATE -> onTerminate(event.instanceId());
            case PUMP -> { /* 仅用于唤醒 pumpDispatch */ }
            case REPAIR -> onRepair(event.instanceId());
        }
    }

    /**
     * 控制流自愈：处理「状态已落盘但派发事件随崩溃丢失」的边界。
     * 若仍有就绪/在跑活动，无需修复；否则根据已完成的活动重建下一个应激活的节点。
     */
    private void onRepair(String instanceId) {
        Optional<WorkflowInstance> opt = store.findById(instanceId);
        if (opt.isEmpty() || opt.get().getStatus() != WorkflowStatus.RUNNING) {
            return;
        }
        WorkflowInstance instance = opt.get();
        boolean hasRunning = instance.getSteps().stream()
                .anyMatch(s -> s.getStatus() == StepStatus.RUNNING);
        if (hasRunning || !instance.getReadyNodeIds().isEmpty()) {
            return; // reconcile 已覆盖
        }
        CompiledFlow flow = requireFlow(instance.getWorkflowName());
        List<String> toEnqueue = new ArrayList<>();
        CompiledFlow.NodeInfo anchor = findRepairAnchor(instance, flow);
        if (anchor == null) {
            // 没有可识别的完成锚点（可能整个流程未开始，或仅完成了一个空选择节点）。
            CompiledFlow.NodeInfo selectedEmpty = findSelectedEmptyChoice(instance, flow, flow.rootIds());
            if (selectedEmpty != null) {
                continueAfterControl(instance, selectedEmpty, instance.getInput(), toEnqueue);
            } else if (!flow.rootIds().isEmpty()) {
                repairActivate(instance, flow, flow.rootIds().get(0), instance.getInput(), toEnqueue);
            }
        } else if (anchor.kind == CompiledFlow.NodeKind.PARALLEL) {
            String joinOutput = requireStep(instance, anchor.joinId).getOutput();
            activateSuccessor(instance, anchor, joinOutput != null ? joinOutput : instance.getInput(), toEnqueue);
        } else if (anchor.kind == CompiledFlow.NodeKind.ACTIVITY
                && anchor.activity.scope == CompiledFlow.ActivityScope.PARALLEL_BRANCH) {
            repairAfterBranch(instance, flow, anchor, toEnqueue);
        } else {
            String output = anchor.kind == CompiledFlow.NodeKind.ACTIVITY
                    ? requireStep(instance, anchor.id).getOutput() : instance.getInput();
            activateSuccessor(instance, anchor, output != null ? output : instance.getInput(), toEnqueue);
        }
        if (instance.getStatus() == WorkflowStatus.RUNNING && toEnqueue.isEmpty()
                && instance.getSteps().stream().noneMatch(s -> s.getStatus() == StepStatus.RUNNING)) {
            // 仍无法定位下一步：兜底完结，避免实例永久挂起（理论上不应发生）。
            instance.setStatus(WorkflowStatus.COMPLETED);
            markPendingSkippedAtCompletion(instance);
            log.warn("[instance={}] repair could not locate next node, forcing COMPLETED", instanceId);
        }
        persistOrThrow(instance);
        for (String nodeId : toEnqueue) {
            enqueueReady(instanceId, nodeId);
        }
    }

    /** 找到自愈锚点：根序列上最后一个已完成的顶层活动/控制区域。 */
    private CompiledFlow.NodeInfo findRepairAnchor(WorkflowInstance instance, CompiledFlow flow) {
        for (int i = flow.rootIds().size() - 1; i >= 0; i--) {
            CompiledFlow.NodeInfo candidate = lastCompletedIn(instance, flow, flow.node(flow.rootIds().get(i)));
            if (candidate != null) {
                return candidate;
            }
        }
        return null;
    }

    /** 返回子树中「可作为推进起点」的最后完成节点；若整个子树尚未开始或未完成则返回 null。 */
    private CompiledFlow.NodeInfo lastCompletedIn(WorkflowInstance instance, CompiledFlow flow,
                                                  CompiledFlow.NodeInfo root) {
        switch (root.kind) {
            case ACTIVITY -> {
                StepRecord record = findStep(instance, root.id);
                if (record != null && record.getStatus() == StepStatus.COMPLETED) {
                    return root;
                }
                return null;
            }
            case PARALLEL -> {
                // 已汇合则以并行节点为锚点；仅部分分支完成不算该区域完成（交给 repairAfterBranch 不适用，
                // 因为部分完成时 readyNodeIds 中必然还有分支，onRepair 已提前返回）。
                StepRecord joinRecord = findStep(instance, root.joinId);
                if (joinRecord != null && joinRecord.getStatus() == StepStatus.COMPLETED) {
                    return root;
                }
                return null;
            }
            case CHOICE -> {
                String selectedLabel = instance.getChoices().get(root.id);
                if (selectedLabel == null) {
                    return null;
                }
                for (CompiledFlow.ChoiceArm arm : root.choiceArms) {
                    if (!arm.label.equals(selectedLabel)) {
                        continue;
                    }
                    for (int i = arm.childIds.size() - 1; i >= 0; i--) {
                        CompiledFlow.NodeInfo candidate =
                                lastCompletedIn(instance, flow, flow.node(arm.childIds.get(i)));
                        if (candidate != null) {
                            return candidate;
                        }
                    }
                    return null;
                }
                return null;
            }
        }
        return null;
    }

    /** 在根序列中找到「已选择、分支体为空或体内活动都未完成」的最靠前选择节点（用于选择后崩溃的自愈）。 */
    private CompiledFlow.NodeInfo findSelectedEmptyChoice(WorkflowInstance instance, CompiledFlow flow,
                                                          List<String> rootIds) {
        for (String rootId : rootIds) {
            CompiledFlow.NodeInfo node = flow.node(rootId);
            if (node.kind == CompiledFlow.NodeKind.CHOICE
                    && instance.getChoices().containsKey(node.id)) {
                String label = instance.getChoices().get(node.id);
                boolean bodyPending = node.choiceArms.stream()
                        .filter(a -> a.label.equals(label))
                        .flatMap(a -> a.childIds.stream())
                        .allMatch(childId -> {
                            CompiledFlow.NodeInfo child = flow.node(childId);
                            StepRecord record = child.kind == CompiledFlow.NodeKind.ACTIVITY
                                    ? findStep(instance, child.id) : null;
                            return record == null || record.getStatus() != StepStatus.COMPLETED;
                        });
                if (bodyPending) {
                    return node;
                }
            }
        }
        return null;
    }

    private void repairActivate(WorkflowInstance instance, CompiledFlow flow, String nodeId,
                                String input, List<String> toEnqueue) {
        CompiledFlow.NodeInfo node = flow.node(nodeId);
        switch (node.kind) {
            case ACTIVITY -> {
                requireStep(instance, nodeId).setInput(input);
                instance.getReadyNodeIds().add(nodeId);
                toEnqueue.add(nodeId);
            }
            case CHOICE -> chooseBranch(instance, node, input, toEnqueue);
            case PARALLEL -> fanOut(instance, node, input, toEnqueue);
        }
    }

    /** 分支完成后的自愈：未齐则等待（不做任何事），已齐则补汇合；join 已完成则推进后继。 */
    private void repairAfterBranch(WorkflowInstance instance, CompiledFlow flow,
                                   CompiledFlow.NodeInfo branchActivity, List<String> toEnqueue) {
        String parallelId = branchActivity.activity.parallelId;
        CompiledFlow.NodeInfo parallel = flow.node(parallelId);
        StepRecord joinRecord = findStep(instance, parallel.joinId);
        if (joinRecord != null && joinRecord.getStatus() == StepStatus.COMPLETED) {
            activateSuccessor(instance, parallel, joinRecord.getOutput(), toEnqueue);
            return;
        }
        Map<String, String> outputs = instance.getBranchOutputs().get(parallelId);
        boolean allDone = outputs != null && parallel.parallelArms.stream()
                .allMatch(arm -> outputs.containsKey(arm.label));
        if (allDone && !instance.getReadyNodeIds().contains(parallel.joinId)) {
            prepareJoin(instance, parallel, toEnqueue);
        }
    }

    private void onRunNode(EngineEvent event) {
        Optional<WorkflowInstance> opt = store.findById(event.instanceId());
        boolean dispatchable = opt.isPresent()
                && opt.get().getStatus() == WorkflowStatus.RUNNING
                && opt.get().getReadyNodeIds().contains(event.nodeId());
        StepRecord step = dispatchable ? findStep(opt.get(), event.nodeId()) : null;
        dispatchable = dispatchable && step != null && step.getStatus() == StepStatus.PENDING;
        if (!dispatchable) {
            // 重复/过期的调度事件：若持有并行名额必须释放，避免名额泄漏导致饿死。
            if (event.holdsPermit()) {
                parallelInFlight.decrementAndGet();
            }
            return;
        }
        WorkflowInstance instance = opt.get();
        step.setStatus(StepStatus.RUNNING);
        step.setAttempt(event.attempt());
        step.setErrorKind(null);
        step.setErrorMessage(null);
        instance.getReadyNodeIds().remove(event.nodeId());
        persistOrThrow(instance);
        // RUNNING 已持久化但尚未真正派发：崩溃后按在途活动恢复（新 attempt 重跑，业务需幂等）。
        dispatch(instance, event.nodeId(), event.attempt(), event.holdsPermit());
    }

    private void onNodeSucceeded(EngineEvent event) {
        Optional<WorkflowInstance> opt = applicable(event, StepStatus.RUNNING);
        if (opt.isEmpty()) return; // 迟到/重复通知丢弃
        WorkflowInstance instance = opt.get();
        StepRecord step = requireStep(instance, event.nodeId());
        step.setStatus(StepStatus.COMPLETED);
        step.setOutput(event.output());
        step.setErrorKind(null);
        step.setErrorMessage(null);
        log.info("[instance={}] activity={} attempt={} completed",
                instance.getId(), event.nodeId(), event.attempt());
        List<String> toEnqueue = new ArrayList<>();
        boolean terminal;
        try {
            terminal = advance(instance, event.nodeId(), event.output(), toEnqueue);
        } catch (BranchSelectionException e) {
            failInstance(instance, ErrorKind.NON_RETRYABLE, e.getMessage());
            return;
        }
        // 活动完成 + 后继就绪 + 汇合/跳过/终态 一次性原子落盘，杜绝「写一半崩溃卡死」。
        persistOrThrow(instance);
        if (terminal) {
            log.info("[instance={}] workflow {}", instance.getId(), instance.getStatus());
        }
        for (String nodeId : toEnqueue) {
            enqueueReady(instance.getId(), nodeId);
        }
    }

    /** 直接把实例置入确定终态（用于推进阶段的不可重试错误），取消同实例在跑活动。 */
    private void failInstance(WorkflowInstance instance, ErrorKind kind, String message) {
        instance.setStatus(WorkflowStatus.FAILED);
        instance.getReadyNodeIds().clear();
        cancelAllRunningForInstance(instance.getId());
        for (StepRecord other : instance.getSteps()) {
            if (other.getStatus() == StepStatus.RUNNING) {
                other.setStatus(StepStatus.FAILED);
                other.setErrorMessage("cancelled due to workflow failure");
            }
        }
        persistOrThrow(instance);
        log.warn("[instance={}] workflow FAILED ({}): {}", instance.getId(), kind, message);
    }

    private void onNodeFailed(EngineEvent event) {
        Optional<WorkflowInstance> opt = applicable(event, StepStatus.RUNNING);
        if (opt.isEmpty()) return;
        WorkflowInstance instance = opt.get();
        StepRecord step = requireStep(instance, event.nodeId());
        step.setErrorKind(event.errorKind());
        step.setErrorMessage(event.errorMessage());
        failOrRetry(instance, event.nodeId(), event.attempt(), event.errorKind(), event.errorMessage());
    }

    private void onNodeTimedOut(EngineEvent event) {
        Optional<WorkflowInstance> opt = applicable(event, StepStatus.RUNNING);
        if (opt.isEmpty()) return;
        WorkflowInstance instance = opt.get();
        cancelRunningTask(event.instanceId(), event.nodeId(), event.attempt());
        StepRecord step = requireStep(instance, event.nodeId());
        step.setErrorKind(ErrorKind.TIMEOUT);
        step.setErrorMessage("activity exceeded timeout");
        failOrRetry(instance, event.nodeId(), event.attempt(), ErrorKind.TIMEOUT, "activity exceeded timeout");
    }

    private void onTerminate(String instanceId) {
        Optional<WorkflowInstance> opt = store.findById(instanceId);
        if (opt.isEmpty()) return;
        WorkflowInstance instance = opt.get();
        if (instance.getStatus() != WorkflowStatus.RUNNING) return; // 终态不可逆
        cancelAllRunningForInstance(instanceId);
        for (StepRecord record : instance.getSteps()) {
            if (record.getStatus() == StepStatus.RUNNING) {
                record.setStatus(StepStatus.FAILED);
                record.setErrorMessage("terminated");
            }
        }
        instance.getReadyNodeIds().clear();
        instance.setStatus(WorkflowStatus.TERMINATED);
        persistOrThrow(instance);
        log.info("[instance={}] workflow TERMINATED", instanceId);
    }

    // ---------------------------------------------------------------- 推进逻辑

    /**
     * 活动完成后的统一推进（仅修改内存，不持久化；由调用方在最后统一原子落盘）。
     * 并行分支 -> 记录输出，全齐则汇合；其他活动 -> 按节点种类选择/并行/后继继续。
     *
     * @return true 表示实例已进入终态
     */
    private boolean advance(WorkflowInstance instance, String completedNodeId,
                            String output, List<String> toEnqueue) {
        CompiledFlow flow = requireFlow(instance.getWorkflowName());
        CompiledFlow.NodeInfo completed = flow.node(completedNodeId);
        CompiledFlow.ActivitySpec spec = completed.activity;

        if (spec.scope == CompiledFlow.ActivityScope.PARALLEL_BRANCH) {
            String parallelId = spec.parallelId;
            Map<String, String> outputs =
                    instance.getBranchOutputs().computeIfAbsent(parallelId, k -> new LinkedHashMap<>());
            outputs.put(spec.branchLabel, output);
            CompiledFlow.NodeInfo parallel = flow.node(parallelId);
            boolean allDone = true;
            for (CompiledFlow.ParallelArm arm : parallel.parallelArms) {
                if (!outputs.containsKey(arm.label)) {
                    allDone = false;
                    break;
                }
            }
            if (allDone) {
                prepareJoin(instance, parallel, toEnqueue);
            }
            return false;
        }

        CompiledFlow.NodeInfo current;
        if (spec.scope == CompiledFlow.ActivityScope.JOIN) {
            current = flow.node(spec.parallelId);
            instance.getOpenParallels().remove(spec.parallelId);
        } else {
            current = completed;
        }
        return activateSuccessor(instance, current, output, toEnqueue);
    }

    /** 内存中准备汇合：join 输入按分支定义顺序构造（与完成先后无关），join 只入队一次。 */
    private void prepareJoin(WorkflowInstance instance, CompiledFlow.NodeInfo parallel, List<String> toEnqueue) {
        Map<String, String> raw = instance.getBranchOutputs().get(parallel.id);
        List<String> labels = new ArrayList<>();
        List<String> values = new ArrayList<>();
        // 用定义顺序重建持久化 map：落盘的分支输出也必须与完成先后无关。
        Map<String, String> ordered = new LinkedHashMap<>();
        for (CompiledFlow.ParallelArm arm : parallel.parallelArms) {
            labels.add(arm.label);
            values.add(raw.get(arm.label));
            ordered.put(arm.label, raw.get(arm.label));
        }
        instance.getBranchOutputs().put(parallel.id, ordered);
        String joinInput = encodeJoinInput(labels, values);
        requireStep(instance, parallel.joinId).setInput(joinInput);
        instance.getReadyNodeIds().add(parallel.joinId);
        toEnqueue.add(parallel.joinId);
    }

    /** 激活节点的后继（仅改内存）：分支选择 / 并行扇出 / 普通活动派发；无后继则完结。 */
    private boolean activateSuccessor(WorkflowInstance instance, CompiledFlow.NodeInfo finished,
                                      String output, List<String> toEnqueue) {
        CompiledFlow flow = requireFlow(instance.getWorkflowName());
        String nextId = finished.successorId;
        if (nextId == null && finished.parentId != null) {
            // 分支体末尾：继续外层控制节点的后继。
            nextId = flow.node(finished.parentId).successorId;
        }
        if (nextId == null) {
            instance.setStatus(WorkflowStatus.COMPLETED);
            markPendingSkippedAtCompletion(instance);
            return true;
        }
        CompiledFlow.NodeInfo next = flow.node(nextId);
        switch (next.kind) {
            case ACTIVITY -> {
                requireStep(instance, nextId).setInput(output);
                instance.getReadyNodeIds().add(nextId);
                toEnqueue.add(nextId);
            }
            case CHOICE -> chooseBranch(instance, next, output, toEnqueue);
            case PARALLEL -> fanOut(instance, next, output, toEnqueue);
        }
        return false;
    }

    /** 条件选择（仅改内存）：唯一命中分支执行，其余分支活动标记 SKIPPED。 */
    private void chooseBranch(WorkflowInstance instance, CompiledFlow.NodeInfo choice,
                              String input, List<String> toEnqueue) {
        CompiledFlow.ChoiceArm selected;
        try {
            selected = selectArm(choice, input);
        } catch (RuntimeException e) {
            // 谓词本身抛错：归入不可重试失败，落入确定终态。
            throw new BranchSelectionException(choice.id, e);
        }
        if (selected == null) {
            throw new BranchSelectionException(choice.id, null);
        }
        instance.getChoices().put(choice.id, selected.label);
        for (CompiledFlow.ChoiceArm arm : choice.choiceArms) {
            if (!arm.label.equals(selected.label)) {
                for (String descendantId : arm.childIds) {
                    markSubtreeSkipped(instance, requireFlow(instance.getWorkflowName()).node(descendantId));
                }
            }
        }
        log.info("[instance={}] choice={} selected branch={}", instance.getId(), choice.id, selected.label);
        if (selected.childIds.isEmpty()) {
            // 空分支：直接继续选择节点的后继。
            continueAfterControl(instance, choice, input, toEnqueue);
            return;
        }
        activateFirstChild(instance, selected.childIds.get(0), input, toEnqueue);
    }

    private CompiledFlow.ChoiceArm selectArm(CompiledFlow.NodeInfo choice, String input) {
        CompiledFlow.ChoiceArm fallback = null;
        for (CompiledFlow.ChoiceArm arm : choice.choiceArms) {
            if (arm.isFallback()) {
                fallback = arm;
            } else if (Boolean.TRUE.equals(arm.when.test(input))) {
                return arm;
            }
        }
        return fallback;
    }

    /** 激活某条已选分支体的第一个节点（活动 / 嵌套分支 / 并行）。 */
    private void activateFirstChild(WorkflowInstance instance, String firstId,
                                    String input, List<String> toEnqueue) {
        CompiledFlow.NodeInfo first = requireFlow(instance.getWorkflowName()).node(firstId);
        switch (first.kind) {
            case ACTIVITY -> {
                requireStep(instance, firstId).setInput(input);
                instance.getReadyNodeIds().add(firstId);
                toEnqueue.add(firstId);
            }
            case CHOICE -> chooseBranch(instance, first, input, toEnqueue);
            case PARALLEL -> fanOut(instance, first, input, toEnqueue);
        }
    }

    /** 控制节点（如空分支体的 choice）结束后，推进它的后继；为 null 则完结。 */
    private void continueAfterControl(WorkflowInstance instance, CompiledFlow.NodeInfo control,
                                      String input, List<String> toEnqueue) {
        String nextId = control.successorId;
        if (nextId == null) {
            if (control.parentId != null) {
                nextId = requireFlow(instance.getWorkflowName()).node(control.parentId).successorId;
            }
        }
        if (nextId == null) {
            instance.setStatus(WorkflowStatus.COMPLETED);
            markPendingSkippedAtCompletion(instance);
            return;
        }
        CompiledFlow.NodeInfo next = requireFlow(instance.getWorkflowName()).node(nextId);
        switch (next.kind) {
            case ACTIVITY -> {
                requireStep(instance, nextId).setInput(input);
                instance.getReadyNodeIds().add(nextId);
                toEnqueue.add(nextId);
            }
            case CHOICE -> chooseBranch(instance, next, input, toEnqueue);
            case PARALLEL -> fanOut(instance, next, input, toEnqueue);
        }
    }

    /** 并行扇出（仅改内存）：一次性把全部分支活动置为就绪（受全局名额约束排队，不丢任何一路）。 */
    private void fanOut(WorkflowInstance instance, CompiledFlow.NodeInfo parallel,
                        String input, List<String> toEnqueue) {
        instance.getOpenParallels().add(parallel.id);
        for (CompiledFlow.ParallelArm arm : parallel.parallelArms) {
            requireStep(instance, arm.activityId).setInput(input);
            instance.getReadyNodeIds().add(arm.activityId);
            toEnqueue.add(arm.activityId);
        }
        log.info("[instance={}] parallel={} fanned out {} branches",
                instance.getId(), parallel.id, parallel.parallelArms.size());
    }

    private void markSubtreeSkipped(WorkflowInstance instance, CompiledFlow.NodeInfo root) {
        CompiledFlow flow = requireFlow(instance.getWorkflowName());
        for (String activityId : flow.descendantActivityIds(root.id)) {
            StepRecord record = findStep(instance, activityId);
            if (record != null && record.getStatus() == StepStatus.PENDING) {
                record.setStatus(StepStatus.SKIPPED);
            }
        }
    }

    /** 实例完成时，任何仍 PENDING 的活动（理论上只可能是未触及节点）标记 SKIPPED，保持计数确定。 */
    private void markPendingSkippedAtCompletion(WorkflowInstance instance) {
        for (StepRecord record : instance.getSteps()) {
            if (record.getStatus() == StepStatus.PENDING) {
                record.setStatus(StepStatus.SKIPPED);
            }
        }
    }

    // ---------------------------------------------------------------- 失败语义

    /** 失败处理：可重试且未达上限则退避重试；否则实例进入确定终态并取消同实例其他在跑活动。 */
    private void failOrRetry(WorkflowInstance instance, String nodeId, int attempt,
                             ErrorKind kind, String message) {
        CompiledFlow flow = requireFlow(instance.getWorkflowName());
        CompiledFlow.NodeInfo info = flow.node(nodeId);
        RetryPolicy policy = info.activity.retryPolicy;
        StepRecord step = requireStep(instance, nodeId);

        if (kind != ErrorKind.NON_RETRYABLE && attempt < policy.maxAttempts()) {
            long backoff = policy.backoffForAttempt(attempt);
            int nextAttempt = attempt + 1;
            step.setStatus(StepStatus.PENDING);
            step.setAttempt(nextAttempt);
            instance.getReadyNodeIds().add(nodeId);
            persistOrThrow(instance);
            log.warn("[instance={}] activity={} attempt={} failed ({}), retry in {}ms: {}",
                    instance.getId(), nodeId, attempt, kind, backoff, message);
            // 退避结束后回到 FIFO 派发队列：并行分支重试同样需要重新申请全局名额。
            scheduler.schedule(() -> {
                enqueueReady(instance.getId(), nodeId);
                eventQueue.add(EngineEvent.pumpTick());
            }, backoff, TimeUnit.MILLISECONDS);
            return;
        }
        // 彻底失败 / 超时耗尽：终态确定，取消同实例其他在跑活动，不留后台任务改状态。
        step.setStatus(StepStatus.FAILED);
        WorkflowStatus terminal = kind == ErrorKind.TIMEOUT ? WorkflowStatus.TIMED_OUT : WorkflowStatus.FAILED;
        instance.setStatus(terminal);
        instance.getReadyNodeIds().clear();
        cancelAllRunningForInstance(instance.getId());
        for (StepRecord other : instance.getSteps()) {
            if (other.getStatus() == StepStatus.RUNNING && other != step) {
                other.setStatus(StepStatus.FAILED);
                other.setErrorMessage("cancelled due to sibling failure");
            }
        }
        persistOrThrow(instance);
        log.warn("[instance={}] activity={} exhausted {} attempt(s) ({}), workflow {}",
                instance.getId(), nodeId, attempt, kind, terminal);
    }

    // ---------------------------------------------------------------- 派发与名额

    private void enqueueReady(String instanceId, String nodeId) {
        dispatchBacklog.add(new ReadyActivity(instanceId, nodeId));
    }

    /**
     * 按 FIFO 派发就绪活动：
     * 顺序/汇合活动不占并行名额；并行分支活动需持有全局名额，名额不足则留在队首等待。
     * 已不满足派发前提（实例终态、活动不再 PENDING/就绪）的条目直接丢弃。
     */
    private void pumpDispatch() {
        int guard = 0;
        int maxGuard = dispatchBacklog.size() + maxParallelInFlight + 1;
        while (guard++ < maxGuard && !dispatchBacklog.isEmpty()) {
            ReadyActivity ready = dispatchBacklog.peekFirst();
            WorkflowInstance instance = store.findById(ready.instanceId()).orElse(null);
            if (instance == null || instance.getStatus() != WorkflowStatus.RUNNING) {
                dispatchBacklog.pollFirst();
                continue;
            }
            StepRecord step = findStep(instance, ready.nodeId());
            boolean stillReady = step != null && step.getStatus() == StepStatus.PENDING
                    && instance.getReadyNodeIds().contains(ready.nodeId());
            if (!stillReady) {
                dispatchBacklog.pollFirst();
                continue;
            }
            CompiledFlow.NodeInfo info = requireFlow(instance.getWorkflowName()).node(ready.nodeId());
            boolean holdsPermit = false;
            if (info.activity.scope == CompiledFlow.ActivityScope.PARALLEL_BRANCH) {
                if (parallelInFlight.get() >= maxParallelInFlight) {
                    return; // 名额耗尽：保留在队首，后续事件/释放后再尝试
                }
                parallelInFlight.incrementAndGet();
                holdsPermit = true;
            }
            dispatchBacklog.pollFirst();
            int attempt = Math.max(1, step.getAttempt());
            // 名额所有权随事件移交：若事件被丢弃（终止/状态不符），由 onRunNode 释放，避免泄漏。
            eventQueue.add(EngineEvent.runNode(ready.instanceId(), ready.nodeId(), attempt, holdsPermit));
        }
    }

    /** 将活动交给 worker 执行，并安排超时看门狗。 */
    private void submitActivity(WorkflowInstance instance, String nodeId, int attempt, boolean holdsPermit) {
        CompiledFlow flow = requireFlow(instance.getWorkflowName());
        CompiledFlow.NodeInfo info = flow.node(nodeId);
        CompiledFlow.ActivitySpec spec = info.activity;
        StepRecord step = requireStep(instance, nodeId);
        String input = step.getInput() != null ? step.getInput() : instance.getInput();
        String instanceId = instance.getId();
        StepContext context = new StepContext(instanceId, nodeId, spec.displayName, attempt);

        Runnable work;
        if (spec.scope == CompiledFlow.ActivityScope.JOIN) {
            BranchResults results = decodeJoinInput(input);
            work = () -> {
                try {
                    String output = spec.joinHandler.join(context, results);
                    eventQueue.add(EngineEvent.succeeded(instanceId, nodeId, attempt, output));
                } catch (RetryableException e) {
                    eventQueue.add(EngineEvent.failed(instanceId, nodeId, attempt, ErrorKind.RETRYABLE, e.getMessage()));
                } catch (NonRetryableException e) {
                    eventQueue.add(EngineEvent.failed(instanceId, nodeId, attempt, ErrorKind.NON_RETRYABLE, e.getMessage()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // 超时/终止取消：不投递通知。
                } catch (Exception e) {
                    eventQueue.add(EngineEvent.failed(instanceId, nodeId, attempt, ErrorKind.NON_RETRYABLE, e.toString()));
                }
            };
        } else {
            work = () -> {
                try {
                    String output = spec.handler.execute(context, input);
                    eventQueue.add(EngineEvent.succeeded(instanceId, nodeId, attempt, output));
                } catch (RetryableException e) {
                    eventQueue.add(EngineEvent.failed(instanceId, nodeId, attempt, ErrorKind.RETRYABLE, e.getMessage()));
                } catch (NonRetryableException e) {
                    eventQueue.add(EngineEvent.failed(instanceId, nodeId, attempt, ErrorKind.NON_RETRYABLE, e.getMessage()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    eventQueue.add(EngineEvent.failed(instanceId, nodeId, attempt, ErrorKind.NON_RETRYABLE, e.toString()));
                }
            };
        }

        Runnable guarded = () -> {
            try {
                work.run();
            } finally {
                // 并行名额由实际执行的 worker 持有到结束；结束后释放并唤醒派发。
                if (holdsPermit) {
                    parallelInFlight.decrementAndGet();
                    eventQueue.add(EngineEvent.pumpTick());
                }
                runningTasks.remove(taskKey(instanceId, nodeId, attempt));
            }
        };

        Future<?> task = workers.submit(guarded);
        runningTasks.put(taskKey(instanceId, nodeId, attempt), task);
        scheduler.schedule(() -> eventQueue.add(EngineEvent.timedOut(instanceId, nodeId, attempt)),
                spec.retryPolicy.stepTimeoutMillis(), TimeUnit.MILLISECONDS);
    }

    /** 事件循环内派发（RUN_NODE 持久化后调用），保证状态已落盘。 */
    private void dispatch(WorkflowInstance instance, String nodeId, int attempt, boolean holdsPermit) {
        submitActivity(instance, nodeId, attempt, holdsPermit);
    }

    private void cancelRunningTask(String instanceId, String nodeId, int attempt) {
        Future<?> task = runningTasks.remove(taskKey(instanceId, nodeId, attempt));
        if (task != null) {
            task.cancel(true);
        }
    }

    /** 取消一个实例所有在跑活动（并行名额由 worker 的 finally 自行释放）。 */
    private void cancelAllRunningForInstance(String instanceId) {
        String prefix = instanceId + ":";
        for (Map.Entry<String, Future<?>> entry : new ArrayList<>(runningTasks.entrySet())) {
            if (entry.getKey().startsWith(prefix)) {
                entry.getValue().cancel(true);
                runningTasks.remove(entry.getKey());
            }
        }
    }

    private static String taskKey(String instanceId, String nodeId, int attempt) {
        return instanceId + ":" + nodeId + ":" + attempt;
    }

    // ---------------------------------------------------------------- 工具

    /** 校验通知是否适用于当前状态：实例 RUNNING、活动存在、状态与 attempt 匹配。 */
    private Optional<WorkflowInstance> applicable(EngineEvent event, StepStatus expected) {
        return store.findById(event.instanceId())
                .filter(i -> i.getStatus() == WorkflowStatus.RUNNING)
                .filter(i -> {
                    StepRecord step = findStep(i, event.nodeId());
                    return step != null && step.getStatus() == expected && step.getAttempt() == event.attempt();
                });
    }

    private static StepRecord findStep(WorkflowInstance instance, String nodeId) {
        for (StepRecord step : instance.getSteps()) {
            if (nodeId.equals(step.getNodeId())) {
                return step;
            }
        }
        return null;
    }

    private static StepRecord requireStep(WorkflowInstance instance, String nodeId) {
        StepRecord step = findStep(instance, nodeId);
        if (step == null) {
            throw new IllegalStateException("no step record for node " + nodeId
                    + " in instance " + instance.getId());
        }
        return step;
    }

    private void persistOrThrow(WorkflowInstance instance) {
        if (!store.update(instance)) {
            throw new IllegalStateException("concurrent modification on instance " + instance.getId());
        }
    }

    private CompiledFlow requireFlow(String workflowName) {
        CompiledFlow flow = flows.get(workflowName);
        if (flow == null) {
            throw new IllegalArgumentException("unknown workflow: " + workflowName);
        }
        return flow;
    }

    /** 汇合输入：按分支定义顺序编码为确定性 JSON，与完成先后无关。 */
    private static String encodeJoinInput(List<String> labels, List<String> outputs) {
        try {
            Map<String, String> ordered = new LinkedHashMap<>();
            for (int i = 0; i < labels.size(); i++) {
                ordered.put(labels.get(i), outputs.get(i));
            }
            return JSON.writeValueAsString(ordered);
        } catch (Exception e) {
            throw new IllegalStateException("failed to encode join input", e);
        }
    }

    private static BranchResults decodeJoinInput(String json) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, String> ordered = JSON.readValue(json, Map.class);
            return BranchResults.inDefinitionOrder(new ArrayList<>(ordered.keySet()),
                    new ArrayList<>(ordered.values()));
        } catch (Exception e) {
            throw new IllegalStateException("failed to decode join input: " + json, e);
        }
    }
}
