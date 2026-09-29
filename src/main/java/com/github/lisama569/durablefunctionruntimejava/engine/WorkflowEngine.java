package com.github.lisama569.durablefunctionruntimejava.engine;

import com.github.lisama569.durablefunctionruntimejava.domain.BranchRunStatus;
import com.github.lisama569.durablefunctionruntimejava.domain.ErrorKind;
import com.github.lisama569.durablefunctionruntimejava.domain.ExecutionState;
import com.github.lisama569.durablefunctionruntimejava.domain.ParallelCursor;
import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.domain.StepRecord;
import com.github.lisama569.durablefunctionruntimejava.domain.StepStatus;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowStatus;
import com.github.lisama569.durablefunctionruntimejava.workflow.ChoiceNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.FlowBranch;
import com.github.lisama569.durablefunctionruntimejava.workflow.FlowDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.FlowNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.JoinContext;
import com.github.lisama569.durablefunctionruntimejava.workflow.JoinHandler;
import com.github.lisama569.durablefunctionruntimejava.workflow.ParallelNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.StepContext;
import com.github.lisama569.durablefunctionruntimejava.workflow.TaskNode;
import com.github.lisama569.durablefunctionruntimejava.workflow.WorkflowDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 持久化工作流执行引擎（支持分支选择与并行汇合）。
 *
 * 执行模型：
 * - 所有状态流转只发生在单线程事件循环中，保证同一实例的状态迁移唯一、有序；
 * - 业务任务与汇合逻辑在有界 worker 线程池执行（汇合不占用事件循环，其慢/阻塞不会拖住其他实例），
 *   完成后向事件循环投递携带 (instanceId, path, attempt) 的通知；
 * - 迟到/重复通知因路径、状态或 attempt 不匹配被丢弃，不导致计数错乱或重复推进；
 * - 分支选择未命中的分支任务原子标记 SKIPPED；并行各路独立推进，汇合成功恰好执行一次；
 * - 汇合沿用与普通任务一致的退避重试、超时与失败语义；终态失败/超时/终止会取消全部在跑任务（含阻塞中的汇合）；
 * - 并行分支数上限在注册期校验，超限定义在流程开跑前即被拒绝；
 * - 每次状态迁移先持久化再推进，重启后已完成的分支/任务/汇合不会重复执行，只补跑未完成部分。
 */
public class WorkflowEngine {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);

    /** 默认 worker 线程数：给并行执行设全局上限，避免无限铺开拖垮同一进程内的其他实例。 */
    static final int DEFAULT_WORKER_THREADS = 16;
    /** 单个实例一次并行扇出的分支数上限。 */
    static final int DEFAULT_MAX_PARALLEL_BRANCHES = 8;

    private enum PathKind { TASK, CHOICE_TASK, PARALLEL_TASK, JOIN }

    /**
     * 一条步骤记录在定义中的位置。
     * taskIndex：分支内任务序号（TASK 节点为 0，JOIN 记录为 -1）。
     */
    private record PathLocation(PathKind kind, int nodeIndex, int branchIndex, int taskIndex) {
    }

    private record RecordSpec(String name, String path, PathLocation location) {
    }

    /** 注册期编译：拍平步骤记录布局并校验路径/命名唯一。 */
    private record CompiledFlow(FlowDefinition definition, List<RecordSpec> specs,
                                Map<String, Integer> indexByPath,
                                Map<String, PathLocation> locationByPath) {
    }

    private enum EventKind { RUN_TASK, RUN_JOIN, TASK_SUCCEEDED, TASK_FAILED, TASK_TIMED_OUT, RESUME, TERMINATE }

    private record EngineEvent(EventKind kind, String instanceId, String path, int attempt,
                               String output, ErrorKind errorKind, String errorMessage) {
        static EngineEvent runTask(String instanceId, String path) {
            return new EngineEvent(EventKind.RUN_TASK, instanceId, path, 0, null, null, null);
        }

        static EngineEvent runJoin(String instanceId, String path) {
            return new EngineEvent(EventKind.RUN_JOIN, instanceId, path, 0, null, null, null);
        }

        static EngineEvent succeeded(String instanceId, String path, int attempt, String output) {
            return new EngineEvent(EventKind.TASK_SUCCEEDED, instanceId, path, attempt, output, null, null);
        }

        static EngineEvent failed(String instanceId, String path, int attempt, ErrorKind kind, String message) {
            return new EngineEvent(EventKind.TASK_FAILED, instanceId, path, attempt, null, kind, message);
        }

        static EngineEvent timedOut(String instanceId, String path, int attempt) {
            return new EngineEvent(EventKind.TASK_TIMED_OUT, instanceId, path, attempt, null, ErrorKind.TIMEOUT, "step timed out");
        }

        static EngineEvent resume(String instanceId) {
            return new EngineEvent(EventKind.RESUME, instanceId, null, 0, null, null, null);
        }

        static EngineEvent terminate(String instanceId) {
            return new EngineEvent(EventKind.TERMINATE, instanceId, null, 0, null, null, null);
        }
    }

    /** 待派发的一项任务（drive 中可能一次并行派发多项，统一置 RUNNING 后一次性落盘）。 */
    private record DispatchItem(StepRecord record, TaskNode task, String input, int attempt) {
    }

    private final WorkflowStore store;
    private final Map<String, CompiledFlow> flows = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<EngineEvent> eventQueue = new LinkedBlockingQueue<>();
    private final ExecutorService eventLoop;
    private final ExecutorService workers;
    private final ScheduledExecutorService scheduler;
    private final int maxParallelBranches;
    /** 运行中的任务 future，用于超时/终态失败/终止时取消。key = instanceId:path:attempt */
    private final Map<String, Future<?>> runningTasks = new ConcurrentHashMap<>();
    private volatile boolean running = true;

    public WorkflowEngine(WorkflowStore store) {
        this(store, DEFAULT_WORKER_THREADS, DEFAULT_MAX_PARALLEL_BRANCHES);
    }

    public WorkflowEngine(WorkflowStore store, int workerThreads, int maxParallelBranchesPerInstance) {
        if (workerThreads < 1) throw new IllegalArgumentException("workerThreads must be >= 1");
        if (maxParallelBranchesPerInstance < 1) throw new IllegalArgumentException("maxParallelBranches must be >= 1");
        this.store = store;
        this.maxParallelBranches = maxParallelBranchesPerInstance;
        this.eventLoop = Executors.newSingleThreadExecutor(named("workflow-event-loop"));
        // 有界 worker + FIFO 无界队列：并行总量有全局天花板，任务排队而不是在事件循环上内联执行。
        this.workers = new ThreadPoolExecutor(workerThreads, workerThreads,
                60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), named("workflow-worker"));
        this.scheduler = Executors.newScheduledThreadPool(4, named("workflow-scheduler"));
        this.eventLoop.submit(this::drainEvents);
    }

    private static ThreadFactory named(String prefix) {
        AtomicInteger seq = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /** 注册既有纯串行定义（语义不变，内部转换为节点模型）。 */
    public void register(WorkflowDefinition definition) {
        registerFlow(FlowDefinition.ofSteps(definition.name(), definition.steps()));
    }

    /** 注册支持分支/并行的节点式定义。 */
    public void registerFlow(FlowDefinition definition) {
        flows.put(definition.name(), compile(definition));
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
        instance.setCurrentStepIndex(0);
        instance.setExecutionState(ExecutionState.fresh());
        instance.setCreatedAt(System.currentTimeMillis());
        instance.setUpdatedAt(instance.getCreatedAt());
        for (RecordSpec spec : flow.specs()) {
            instance.getSteps().add(new StepRecord(spec.name(), spec.path()));
        }
        if (!store.createIfAbsent(instance)) {
            // 并发下同幂等键被其他线程抢先创建，返回已存在实例。
            return store.findByIdempotencyKey(workflowName, idempotencyKey).orElseThrow();
        }
        log.info("[instance={}] workflow={} started, records={}", instance.getId(), workflowName, flow.specs().size());
        eventQueue.add(EngineEvent.resume(instance.getId()));
        return instance;
    }

    public Optional<WorkflowInstance> get(String instanceId) {
        return store.findById(instanceId);
    }

    /** 请求终止：实例立即进入 TERMINATED 终态，所有在跑任务被取消，后续步骤不再执行。 */
    public boolean terminate(String instanceId) {
        return store.findById(instanceId)
                .filter(i -> i.getStatus() == WorkflowStatus.RUNNING)
                .map(i -> {
                    eventQueue.add(EngineEvent.terminate(instanceId));
                    return true;
                })
                .orElse(false);
    }

    /** 重启恢复：未完结实例从执行位置继续，已完成任务/分支不重复执行。返回恢复的实例数。 */
    public int recover() {
        List<WorkflowInstance> resumable = store.findAll().stream()
                .filter(i -> i.getStatus() == WorkflowStatus.RUNNING)
                .toList();
        for (WorkflowInstance instance : resumable) {
            prepareRecovery(instance);
            store.update(instance);
            log.info("[instance={}] recovery scheduled at rootPc={}",
                    instance.getId(),
                    instance.getExecutionState() == null ? -1 : instance.getExecutionState().getRootPc());
            eventQueue.add(EngineEvent.resume(instance.getId()));
        }
        return resumable.size();
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

    // ---------------------------------------------------------------- 注册期编译

    private CompiledFlow compile(FlowDefinition definition) {
        List<RecordSpec> specs = new ArrayList<>();
        Set<String> seenPaths = new HashSet<>();
        Set<String> nodeNames = new HashSet<>();
        int nodeIndex = 0;
        for (FlowNode node : definition.nodes()) {
            if (!nodeNames.add(node.name())) {
                throw new IllegalArgumentException("duplicate node name: " + node.name());
            }
            if (node instanceof TaskNode task) {
                addSpec(specs, seenPaths, new RecordSpec(task.name(), task.name(),
                        new PathLocation(PathKind.TASK, nodeIndex, -1, 0)));
            } else if (node instanceof ChoiceNode choice) {
                compileChoice(choice, nodeIndex, specs, seenPaths);
            } else if (node instanceof ParallelNode parallel) {
                compileParallel(parallel, nodeIndex, specs, seenPaths);
            }
            nodeIndex++;
        }
        Map<String, Integer> indexByPath = new HashMap<>();
        Map<String, PathLocation> locationByPath = new HashMap<>();
        for (int i = 0; i < specs.size(); i++) {
            indexByPath.put(specs.get(i).path(), i);
            locationByPath.put(specs.get(i).path(), specs.get(i).location());
        }
        return new CompiledFlow(definition, List.copyOf(specs), Map.copyOf(indexByPath), Map.copyOf(locationByPath));
    }

    private void compileChoice(ChoiceNode choice, int nodeIndex,
                               List<RecordSpec> specs, Set<String> seenPaths) {
        Set<String> labels = new HashSet<>();
        for (int bi = 0; bi < choice.branches().size(); bi++) {
            FlowBranch branch = choice.branches().get(bi);
            if (!labels.add(branch.label())) {
                throw new IllegalArgumentException("duplicate branch label '" + branch.label() + "' in choice " + choice.name());
            }
            int taskIndex = 0;
            for (TaskNode task : branch.tasks()) {
                String path = choicePath(choice.name(), branch.label(), task.name());
                addSpec(specs, seenPaths, new RecordSpec(task.name(), path,
                        new PathLocation(PathKind.CHOICE_TASK, nodeIndex, bi, taskIndex)));
                taskIndex++;
            }
        }
    }

    private void compileParallel(ParallelNode parallel, int nodeIndex,
                                 List<RecordSpec> specs, Set<String> seenPaths) {
        // 注册期（流程真正开跑之前）即拒绝超限配置：结果确定可预期，绝不先执行部分步骤再报错。
        if (parallel.branches().size() > maxParallelBranches) {
            throw new IllegalArgumentException("parallel '" + parallel.name() + "' defines "
                    + parallel.branches().size() + " branches, exceeding per-instance cap "
                    + maxParallelBranches);
        }
        Set<String> labels = new HashSet<>();
        for (int bi = 0; bi < parallel.branches().size(); bi++) {
            FlowBranch branch = parallel.branches().get(bi);
            if (!labels.add(branch.label())) {
                throw new IllegalArgumentException("duplicate branch label '" + branch.label() + "' in parallel " + parallel.name());
            }
            int taskIndex = 0;
            for (TaskNode task : branch.tasks()) {
                String path = parallelPath(parallel.name(), bi, task.name());
                addSpec(specs, seenPaths, new RecordSpec(task.name(), path,
                        new PathLocation(PathKind.PARALLEL_TASK, nodeIndex, bi, taskIndex)));
                taskIndex++;
            }
        }
        String joinPath = joinPath(parallel.name());
        addSpec(specs, seenPaths, new RecordSpec(joinPath, joinPath,
                new PathLocation(PathKind.JOIN, nodeIndex, -1, -1)));
    }

    private static void addSpec(List<RecordSpec> specs, Set<String> seenPaths, RecordSpec spec) {
        if (!seenPaths.add(spec.path())) {
            throw new IllegalArgumentException("duplicate step path: " + spec.path());
        }
        specs.add(spec);
    }

    static String choicePath(String node, String label, String task) {
        return node + "." + label + "." + task;
    }

    static String parallelPath(String node, int branchIndex, String task) {
        return node + "[" + branchIndex + "]." + task;
    }

    static String joinPath(String node) {
        return node + ".join";
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
        }
    }

    private void handle(EngineEvent event) {
        switch (event.kind()) {
            case RUN_TASK -> onRunTask(event);
            case RUN_JOIN -> onRunJoin(event);
            case TASK_SUCCEEDED -> onTaskSucceeded(event);
            case TASK_FAILED -> onTaskFailed(event);
            case TASK_TIMED_OUT -> onTaskTimedOut(event);
            case RESUME -> onResume(event.instanceId());
            case TERMINATE -> onTerminate(event.instanceId());
        }
    }

    private void onResume(String instanceId) {
        Optional<WorkflowInstance> opt = store.findById(instanceId);
        if (opt.isEmpty()) return;
        WorkflowInstance instance = opt.get();
        if (instance.getStatus() != WorkflowStatus.RUNNING) return;
        drive(instance);
    }

    private void onRunTask(EngineEvent event) {
        Optional<WorkflowInstance> opt = store.findById(event.instanceId());
        if (opt.isEmpty()) return;
        WorkflowInstance instance = opt.get();
        if (instance.getStatus() != WorkflowStatus.RUNNING) return;
        CompiledFlow flow = requireFlow(instance.getWorkflowName());
        PathLocation location = flow.locationByPath().get(event.path());
        if (location == null) return;
        ExecutionState es = state(instance);
        if (!isExpectedHead(es, flow, location, event.path())) return;

        StepRecord record = recordAt(flow, instance, event.path());
        if (record.getStatus() != StepStatus.PENDING) return; // 重复/迟到的调度
        TaskNode task = resolveTask(flow, location);
        if (task == null) return;
        String input = resolveInput(flow, instance, location);
        int attempt = record.getAttempt() + 1;
        dispatch(instance, List.of(new DispatchItem(record, task, input, attempt)));
    }

    private void onTaskSucceeded(EngineEvent event) {
        Optional<WorkflowInstance> opt = applicable(event);
        if (opt.isEmpty()) return; // 迟到/重复的完成通知，直接丢弃
        WorkflowInstance instance = opt.get();
        CompiledFlow flow = requireFlow(instance.getWorkflowName());
        PathLocation location = flow.locationByPath().get(event.path());
        StepRecord record = recordAt(flow, instance, event.path());
        record.setStatus(StepStatus.COMPLETED);
        record.setOutput(event.output());
        record.setErrorKind(null);
        record.setErrorMessage(null);
        log.info("[instance={}] {}={} attempt={} completed",
                instance.getId(), location.kind() == PathKind.JOIN ? "join" : "task",
                event.path(), event.attempt());

        ExecutionState es = state(instance);
        switch (location.kind()) {
            case TASK -> {
                es.setRootPc(location.nodeIndex() + 1);
                syncStepIndex(instance, es);
                persistOrThrow(instance);
                drive(instance);
            }
            case CHOICE_TASK -> {
                es.setChoiceTaskPc(es.getChoiceTaskPc() + 1);
                persistOrThrow(instance);
                drive(instance);
            }
            case PARALLEL_TASK -> {
                ParallelCursor cursor = es.getParallelCursors().get(location.branchIndex());
                FlowBranch branch = parallelAt(flow, location.nodeIndex()).branches().get(cursor.getBranchIndex());
                cursor.setTaskIndex(cursor.getTaskIndex() + 1);
                if (cursor.getTaskIndex() >= branch.tasks().size()) {
                    cursor.setStatus(BranchRunStatus.COMPLETED);
                }
                if (allBranchesCompleted(es)) {
                    startJoin(instance, flow, location.nodeIndex());
                } else {
                    persistOrThrow(instance);
                    drive(instance);
                }
            }
            case JOIN -> {
                // 汇合成功与推进到下一顶层节点在同一次持久化中落盘（at-least-once 下汇合副作用需幂等）。
                es.setActiveParallelNode(-1);
                es.setParallelCursors(new ArrayList<>());
                es.setRootPc(location.nodeIndex() + 1);
                syncStepIndex(instance, es);
                persistOrThrow(instance);
                log.info("[instance={}] parallel={} joined once from {} branches",
                        instance.getId(), parallelAt(flow, location.nodeIndex()).name(),
                        parallelAt(flow, location.nodeIndex()).branches().size());
                drive(instance);
            }
        }
    }

    private void onTaskFailed(EngineEvent event) {
        Optional<WorkflowInstance> opt = applicable(event);
        if (opt.isEmpty()) return;
        WorkflowInstance instance = opt.get();
        CompiledFlow flow = requireFlow(instance.getWorkflowName());
        StepRecord record = recordAt(flow, instance, event.path());
        record.setErrorKind(event.errorKind());
        record.setErrorMessage(event.errorMessage());
        failOrRetry(instance, flow, event.path(), event.attempt(), event.errorKind(), event.errorMessage());
    }

    private void onTaskTimedOut(EngineEvent event) {
        Optional<WorkflowInstance> opt = applicable(event);
        if (opt.isEmpty()) return;
        WorkflowInstance instance = opt.get();
        cancelRunningTask(event.instanceId(), event.path(), event.attempt());
        CompiledFlow flow = requireFlow(instance.getWorkflowName());
        PathLocation location = flow.locationByPath().get(event.path());
        boolean isJoin = location != null && location.kind() == PathKind.JOIN;
        String message = isJoin ? "join exceeded timeout" : "step exceeded timeout";
        StepRecord record = recordAt(flow, instance, event.path());
        record.setErrorKind(ErrorKind.TIMEOUT);
        record.setErrorMessage(message);
        failOrRetry(instance, flow, event.path(), event.attempt(), ErrorKind.TIMEOUT, message);
    }

    private void onTerminate(String instanceId) {
        Optional<WorkflowInstance> opt = store.findById(instanceId);
        if (opt.isEmpty()) return;
        WorkflowInstance instance = opt.get();
        if (instance.getStatus() != WorkflowStatus.RUNNING) return; // 终态不可逆
        cancelAllRunningTasks(instanceId);
        for (StepRecord record : instance.getSteps()) {
            if (record.getStatus() == StepStatus.RUNNING || record.getStatus() == StepStatus.PENDING) {
                record.setStatus(StepStatus.CANCELLED);
                record.setErrorMessage("terminated");
            }
        }
        instance.setStatus(WorkflowStatus.TERMINATED);
        persistOrThrow(instance);
        log.info("[instance={}] workflow TERMINATED", instanceId);
    }

    // ---------------------------------------------------------------- 执行驱动

    /**
     * 从当前执行位置推进：选择分支、扇出并行、派发处于头部的待执行任务。
     * 每个状态分支至多派发一批任务后返回，完成后由通知事件再次驱动。
     */
    private void drive(WorkflowInstance instance) {
        if (instance.getStatus() != WorkflowStatus.RUNNING) return;
        CompiledFlow flow = requireFlow(instance.getWorkflowName());
        ExecutionState es = state(instance);
        List<FlowNode> nodes = flow.definition().nodes();

        int pc = es.getRootPc();
        if (pc >= nodes.size()) {
            instance.setStatus(WorkflowStatus.COMPLETED);
            syncStepIndex(instance, es);
            persistOrThrow(instance);
            log.info("[instance={}] workflow COMPLETED", instance.getId());
            return;
        }

        FlowNode node = nodes.get(pc);
        if (node instanceof TaskNode task) {
            StepRecord record = recordAt(flow, instance, task.name());
            if (record.getStatus() == StepStatus.PENDING) {
                String input = nodeInput(flow, instance, pc);
                dispatch(instance, List.of(new DispatchItem(record, task, input, record.getAttempt() + 1)));
            }
            return;
        }

        if (node instanceof ChoiceNode choice) {
            if (es.getActiveChoiceNode() == pc) {
                FlowBranch branch = choice.branches().get(es.getActiveChoiceBranch());
                int taskPc = es.getChoiceTaskPc();
                if (taskPc >= branch.tasks().size()) {
                    // 分支走完：复位选择状态，推进到下一顶层节点。
                    es.setActiveChoiceNode(-1);
                    es.setActiveChoiceBranch(-1);
                    es.setChoiceTaskPc(0);
                    es.setRootPc(pc + 1);
                    syncStepIndex(instance, es);
                    persistOrThrow(instance);
                    drive(instance);
                    return;
                }
                TaskNode task = branch.tasks().get(taskPc);
                String path = choicePath(choice.name(), branch.label(), task.name());
                StepRecord record = recordAt(flow, instance, path);
                if (record.getStatus() == StepStatus.PENDING) {
                    String input = taskPc == 0 ? nodeInput(flow, instance, pc)
                            : recordAt(flow, instance, choicePath(choice.name(), branch.label(),
                                    branch.tasks().get(taskPc - 1).name())).getOutput();
                    dispatch(instance, List.of(new DispatchItem(record, task, input, record.getAttempt() + 1)));
                }
                return;
            }

            // 首次到达：运行选择器，未选分支整体 SKIPPED。
            String upstream = nodeInput(flow, instance, pc);
            String label;
            try {
                label = choice.selector().select(upstream);
            } catch (Exception e) {
                failInstance(instance, WorkflowStatus.FAILED, ErrorKind.NON_RETRYABLE,
                        "branch selector failed: " + e, null);
                return;
            }
            FlowBranch chosen = choice.branch(label);
            if (chosen == null) {
                failInstance(instance, WorkflowStatus.FAILED, ErrorKind.NON_RETRYABLE,
                        "branch selector returned unknown label: " + label, null);
                return;
            }
            int chosenIndex = choice.branches().indexOf(chosen);
            for (int bi = 0; bi < choice.branches().size(); bi++) {
                FlowBranch branch = choice.branches().get(bi);
                for (TaskNode task : branch.tasks()) {
                    StepRecord record = recordAt(flow, instance, choicePath(choice.name(), branch.label(), task.name()));
                    if (bi == chosenIndex) {
                        if (record.getStatus() == StepStatus.PENDING) {
                            record.setInput(upstream);
                        }
                    } else {
                        record.setStatus(StepStatus.SKIPPED);
                    }
                }
            }
            es.setActiveChoiceNode(pc);
            es.setActiveChoiceBranch(chosenIndex);
            es.setChoiceTaskPc(0);
            persistOrThrow(instance);
            log.info("[instance={}] choice={} selected branch={}", instance.getId(), choice.name(), label);
            drive(instance);
            return;
        }

        if (node instanceof ParallelNode parallel) {
            if (es.getActiveParallelNode() != pc) {
                // 首次到达：初始化每条分支的游标（分支数上限已在注册期校验，超限定义无法注册）。
                String upstream = nodeInput(flow, instance, pc);
                List<ParallelCursor> cursors = new ArrayList<>();
                for (int bi = 0; bi < parallel.branches().size(); bi++) {
                    FlowBranch branch = parallel.branches().get(bi);
                    StepRecord first = recordAt(flow, instance, parallelPath(parallel.name(), bi, branch.tasks().get(0).name()));
                    if (first.getStatus() == StepStatus.PENDING) {
                        first.setInput(upstream);
                    }
                    cursors.add(new ParallelCursor(bi, branch.label(), 0));
                }
                es.setActiveParallelNode(pc);
                es.setParallelCursors(cursors);
                persistOrThrow(instance);
                log.info("[instance={}] parallel={} fanned out {} branches",
                        instance.getId(), parallel.name(), parallel.branches().size());
            }
            // 派发所有处于分支头部的 PENDING 任务（各路同时拉起）。
            List<DispatchItem> items = new ArrayList<>();
            for (ParallelCursor cursor : es.getParallelCursors()) {
                if (cursor.getStatus() != BranchRunStatus.RUNNING) continue;
                FlowBranch branch = parallel.branches().get(cursor.getBranchIndex());
                if (cursor.getTaskIndex() >= branch.tasks().size()) continue;
                TaskNode task = branch.tasks().get(cursor.getTaskIndex());
                String path = parallelPath(parallel.name(), cursor.getBranchIndex(), task.name());
                StepRecord record = recordAt(flow, instance, path);
                if (record.getStatus() == StepStatus.PENDING) {
                    String input = cursor.getTaskIndex() == 0
                            ? record.getInput()
                            : recordAt(flow, instance, parallelPath(parallel.name(), cursor.getBranchIndex(),
                                    branch.tasks().get(cursor.getTaskIndex() - 1).name())).getOutput();
                    items.add(new DispatchItem(record, task, input, record.getAttempt() + 1));
                }
            }
            if (!items.isEmpty()) {
                dispatch(instance, items);
                return;
            }
            // 没有处于头部的待执行任务：若所有分支都已完成（恢复/补跑场景），补发起尚未执行的汇合。
            if (allBranchesCompleted(es)) {
                StepRecord joinRecord = recordAt(flow, instance, joinPath(parallel.name()));
                if (joinRecord.getStatus() == StepStatus.PENDING) {
                    startJoin(instance, flow, pc);
                }
            }
        }
    }

    /**
     * 全部分支完成后启动汇合：输出按分支定义顺序聚合（与完成先后无关），汇合记录置 RUNNING 后落盘，
     * 再把汇合交给 worker 线程池执行，<b>不阻塞事件循环</b>——某个实例的汇合再慢甚至永久阻塞，
     * 同进程其他实例的事件仍能被事件循环及时处理。重试/超时/失败语义与普通任务一致。
     */
    private void startJoin(WorkflowInstance instance, CompiledFlow flow, int nodeIndex) {
        ParallelNode parallel = parallelAt(flow, nodeIndex);
        StepRecord joinRecord = recordAt(flow, instance, joinPath(parallel.name()));
        if (joinRecord.getStatus() != StepStatus.PENDING) {
            return; // 汇合已派发/已完成（防御性检查，保证恰好一次派发）
        }
        List<String> outputs = collectJoinOutputs(flow, instance, parallel);
        int attempt = joinRecord.getAttempt() + 1;
        joinRecord.setStatus(StepStatus.RUNNING);
        joinRecord.setAttempt(attempt);
        joinRecord.setErrorKind(null);
        joinRecord.setErrorMessage(null);
        persistOrThrow(instance);
        submitJoin(instance, parallel, outputs, attempt);
        log.info("[instance={}] join={} attempt={} started", instance.getId(), joinRecord.getPath(), attempt);
    }

    /** 汇合退避后重跑（RUN_JOIN 事件在事件循环中处理，校验仍位于汇合头后再次派发）。 */
    private void onRunJoin(EngineEvent event) {
        Optional<WorkflowInstance> opt = store.findById(event.instanceId());
        if (opt.isEmpty()) return;
        WorkflowInstance instance = opt.get();
        if (instance.getStatus() != WorkflowStatus.RUNNING) return;
        CompiledFlow flow = requireFlow(instance.getWorkflowName());
        PathLocation location = flow.locationByPath().get(event.path());
        if (location == null || location.kind() != PathKind.JOIN) return;
        ExecutionState es = state(instance);
        if (!isExpectedHead(es, flow, location, event.path())) return;
        StepRecord joinRecord = recordAt(flow, instance, event.path());
        if (joinRecord.getStatus() != StepStatus.PENDING) return; // 重复/迟到的调度
        startJoin(instance, flow, location.nodeIndex());
    }

    private static List<String> collectJoinOutputs(CompiledFlow flow, WorkflowInstance instance, ParallelNode parallel) {
        List<String> outputs = new ArrayList<>();
        for (int bi = 0; bi < parallel.branches().size(); bi++) {
            FlowBranch branch = parallel.branches().get(bi);
            TaskNode last = branch.tasks().get(branch.tasks().size() - 1);
            outputs.add(recordAt(flow, instance, parallelPath(parallel.name(), bi, last.name())).getOutput());
        }
        return outputs;
    }

    private void submitJoin(WorkflowInstance instance, ParallelNode parallel, List<String> outputs, int attempt) {
        String instanceId = instance.getId();
        String path = joinPath(parallel.name());
        JoinContext context = new JoinContext(instanceId, parallel.name());
        JoinHandler handler = parallel.join();
        RetryPolicy policy = parallel.joinRetryPolicy();

        Future<?> future = workers.submit(() -> {
            try {
                String joined = handler.join(context, outputs);
                eventQueue.add(EngineEvent.succeeded(instanceId, path, attempt, joined));
            } catch (RetryableException e) {
                eventQueue.add(EngineEvent.failed(instanceId, path, attempt, ErrorKind.RETRYABLE, e.getMessage()));
            } catch (NonRetryableException e) {
                eventQueue.add(EngineEvent.failed(instanceId, path, attempt, ErrorKind.NON_RETRYABLE, e.getMessage()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // 被超时/终态失败/终止取消：不投递通知，状态已由事件循环推进。
            } catch (Exception e) {
                // 未分类异常默认不可重试，避免无限重试放大故障。
                eventQueue.add(EngineEvent.failed(instanceId, path, attempt, ErrorKind.NON_RETRYABLE, e.toString()));
            } finally {
                runningTasks.remove(taskKey(instanceId, path, attempt));
            }
        });
        runningTasks.put(taskKey(instanceId, path, attempt), future);
        scheduler.schedule(() -> eventQueue.add(EngineEvent.timedOut(instanceId, path, attempt)),
                policy.stepTimeoutMillis(), TimeUnit.MILLISECONDS);
    }

    // ---------------------------------------------------------------- 失败/重试

    /** 失败处理：可重试且未达上限则按退避策略重试，否则取消兄弟任务并落入确定终态。 */
    private void failOrRetry(WorkflowInstance instance, CompiledFlow flow, String path, int attempt,
                             ErrorKind kind, String message) {
        StepRecord record = recordAt(flow, instance, path);
        PathLocation location = flow.locationByPath().get(path);
        RetryPolicy policy = policyForPath(flow, location);
        boolean isJoin = location != null && location.kind() == PathKind.JOIN;
        String attributedMessage = isJoin ? "parallel join failed: " + message : message;

        if (kind == ErrorKind.NON_RETRYABLE) {
            failInstance(instance, WorkflowStatus.FAILED, kind, attributedMessage, record);
            return;
        }
        if (attempt < policy.maxAttempts()) {
            long backoff = policy.backoffForAttempt(attempt);
            record.setStatus(StepStatus.PENDING);
            record.setErrorMessage(message);
            persistOrThrow(instance);
            log.warn("[instance={}] {}={} attempt={} failed ({}), retry in {}ms: {}",
                    instance.getId(), isJoin ? "join" : "task", path, attempt, kind, backoff, message);
            // 汇合与任务走各自的重跑事件；迟到的旧 attempt 通知仍会被状态/attempt/执行头校验丢弃。
            EngineEvent retry = isJoin
                    ? EngineEvent.runJoin(instance.getId(), path)
                    : EngineEvent.runTask(instance.getId(), path);
            scheduler.schedule(() -> eventQueue.add(retry), backoff, TimeUnit.MILLISECONDS);
            return;
        }
        WorkflowStatus terminal = kind == ErrorKind.TIMEOUT ? WorkflowStatus.TIMED_OUT : WorkflowStatus.FAILED;
        failInstance(instance, terminal, kind, attributedMessage, record);
    }

    /** 任务路径取任务自身策略；汇合路径取并行节点的汇合策略。 */
    private RetryPolicy policyForPath(CompiledFlow flow, PathLocation location) {
        if (location == null) return RetryPolicy.defaults();
        if (location.kind() == PathKind.JOIN) {
            return parallelAt(flow, location.nodeIndex()).joinRetryPolicy();
        }
        return resolveTask(flow, location).retryPolicy();
    }

    /**
     * 实例落入终态：触发任务标记 FAILED，其余在跑任务立即取消、待执行任务标记 CANCELLED，
     * 不留下任何后台任务继续修改状态。
     */
    private void failInstance(WorkflowInstance instance, WorkflowStatus terminal, ErrorKind kind,
                              String message, StepRecord failedRecord) {
        cancelAllRunningTasks(instance.getId());
        for (StepRecord record : instance.getSteps()) {
            if (record == failedRecord) {
                record.setStatus(StepStatus.FAILED);
            } else if (record.getStatus() == StepStatus.RUNNING || record.getStatus() == StepStatus.PENDING) {
                record.setStatus(StepStatus.CANCELLED);
            }
        }
        instance.setStatus(terminal);
        instance.setErrorKind(kind);
        instance.setErrorMessage(message);
        persistOrThrow(instance);
        log.warn("[instance={}] workflow {} ({}): {}", instance.getId(), terminal, kind, message);
    }

    // ---------------------------------------------------------------- 任务派发

    /** 统一置 RUNNING 后一次性落盘，再把任务交给 worker，并为每个任务安排超时看门狗。 */
    private void dispatch(WorkflowInstance instance, List<DispatchItem> items) {
        for (DispatchItem item : items) {
            StepRecord record = item.record();
            record.setStatus(StepStatus.RUNNING);
            record.setAttempt(item.attempt());
            record.setInput(item.input());
            record.setErrorKind(null);
            record.setErrorMessage(null);
        }
        persistOrThrow(instance);
        for (DispatchItem item : items) {
            submitTask(instance, item);
        }
    }

    private void submitTask(WorkflowInstance instance, DispatchItem item) {
        String instanceId = instance.getId();
        String path = item.record().getPath();
        int attempt = item.attempt();
        TaskNode task = item.task();
        StepContext context = new StepContext(instanceId, task.name(), path, attempt);
        String input = item.input();

        Future<?> future = workers.submit(() -> {
            try {
                String output = task.handler().execute(context, input);
                eventQueue.add(EngineEvent.succeeded(instanceId, path, attempt, output));
            } catch (RetryableException e) {
                eventQueue.add(EngineEvent.failed(instanceId, path, attempt, ErrorKind.RETRYABLE, e.getMessage()));
            } catch (NonRetryableException e) {
                eventQueue.add(EngineEvent.failed(instanceId, path, attempt, ErrorKind.NON_RETRYABLE, e.getMessage()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // 被超时/终态失败/终止取消：不投递通知，状态已由事件循环推进。
            } catch (Exception e) {
                // 未分类异常默认不可重试，避免无限重试放大故障。
                eventQueue.add(EngineEvent.failed(instanceId, path, attempt, ErrorKind.NON_RETRYABLE, e.toString()));
            } finally {
                runningTasks.remove(taskKey(instanceId, path, attempt));
            }
        });
        runningTasks.put(taskKey(instanceId, path, attempt), future);
        scheduler.schedule(() -> eventQueue.add(EngineEvent.timedOut(instanceId, path, attempt)),
                task.retryPolicy().stepTimeoutMillis(), TimeUnit.MILLISECONDS);
        log.info("[instance={}] task={} attempt={} started", instanceId, path, attempt);
    }

    private void cancelRunningTask(String instanceId, String path, int attempt) {
        Future<?> task = runningTasks.remove(taskKey(instanceId, path, attempt));
        if (task != null) {
            task.cancel(true);
        }
    }

    private void cancelAllRunningTasks(String instanceId) {
        String prefix = instanceId + ":";
        for (String key : new ArrayList<>(runningTasks.keySet())) {
            if (key.startsWith(prefix)) {
                Future<?> task = runningTasks.remove(key);
                if (task != null) {
                    task.cancel(true);
                }
            }
        }
    }

    private static String taskKey(String instanceId, String path, int attempt) {
        return instanceId + ":" + path + ":" + attempt;
    }

    // ---------------------------------------------------------------- 恢复

    /** 迁移旧串行实例状态，并把崩溃时处于 RUNNING 的任务退回 PENDING（以新 attempt 补跑）。 */
    private void prepareRecovery(WorkflowInstance instance) {
        if (instance.getExecutionState() == null) {
            ExecutionState es = ExecutionState.fresh();
            es.setRootPc(instance.getCurrentStepIndex());
            instance.setExecutionState(es);
            for (StepRecord record : instance.getSteps()) {
                if (record.getPath() == null) {
                    record.setPath(record.getName());
                }
            }
        }
        for (StepRecord record : instance.getSteps()) {
            if (record.getStatus() == StepStatus.RUNNING) {
                record.setStatus(StepStatus.PENDING);
            }
        }
    }

    // ---------------------------------------------------------------- 定位辅助

    /** 校验通知是否适用于当前状态：实例 RUNNING + 记录 RUNNING + attempt 匹配 + 位于当前执行头。 */
    private Optional<WorkflowInstance> applicable(EngineEvent event) {
        return store.findById(event.instanceId())
                .filter(i -> i.getStatus() == WorkflowStatus.RUNNING)
                .flatMap(i -> {
                    CompiledFlow flow = requireFlow(i.getWorkflowName());
                    PathLocation location = flow.locationByPath().get(event.path());
                    if (location == null) return Optional.empty();
                    StepRecord record = recordAt(flow, i, event.path());
                    if (record.getStatus() != StepStatus.RUNNING || record.getAttempt() != event.attempt()) {
                        return Optional.empty();
                    }
                    return isExpectedHead(state(i), flow, location, event.path()) ? Optional.of(i) : Optional.empty();
                });
    }

    /** 判断某路径是否是当前执行位置上允许调度/完成的任务头。 */
    private boolean isExpectedHead(ExecutionState es, CompiledFlow flow, PathLocation location, String path) {
        return switch (location.kind()) {
            case TASK -> es.getRootPc() == location.nodeIndex();
            case CHOICE_TASK -> es.getActiveChoiceNode() == location.nodeIndex()
                    && es.getActiveChoiceBranch() == location.branchIndex()
                    && choiceHeadPath(flow, location.nodeIndex(), es).equals(path);
            case PARALLEL_TASK -> {
                if (es.getActiveParallelNode() != location.nodeIndex()) yield false;
                ParallelCursor cursor = es.getParallelCursors().get(location.branchIndex());
                if (cursor == null || cursor.getStatus() != BranchRunStatus.RUNNING) yield false;
                yield parallelHeadPath(flow, location.nodeIndex(), cursor).equals(path);
            }
            case JOIN ->
                // 汇合头：并行节点仍激活（汇合尚未成功落盘）且所有分支都已完成。
                // 正在执行/退避重试中的汇合同样满足，便于通知与重跑事件匹配。
                es.getActiveParallelNode() == location.nodeIndex() && allBranchesCompleted(es);
        };
    }

    private String choiceHeadPath(CompiledFlow flow, int nodeIndex, ExecutionState es) {
        ChoiceNode choice = choiceAt(flow, nodeIndex);
        FlowBranch branch = choice.branches().get(es.getActiveChoiceBranch());
        return choicePath(choice.name(), branch.label(), branch.tasks().get(es.getChoiceTaskPc()).name());
    }

    private String parallelHeadPath(CompiledFlow flow, int nodeIndex, ParallelCursor cursor) {
        ParallelNode parallel = parallelAt(flow, nodeIndex);
        FlowBranch branch = parallel.branches().get(cursor.getBranchIndex());
        return parallelPath(parallel.name(), cursor.getBranchIndex(),
                branch.tasks().get(cursor.getTaskIndex()).name());
    }

    private TaskNode resolveTask(CompiledFlow flow, PathLocation location) {
        FlowNode node = flow.definition().nodes().get(location.nodeIndex());
        if (node instanceof TaskNode task) {
            return task;
        }
        if (node instanceof ChoiceNode choice) {
            return choice.branches().get(location.branchIndex()).tasks().get(location.taskIndex());
        }
        if (node instanceof ParallelNode parallel) {
            return parallel.branches().get(location.branchIndex()).tasks().get(location.taskIndex());
        }
        return null;
    }

    private String resolveInput(CompiledFlow flow, WorkflowInstance instance, PathLocation location) {
        return switch (location.kind()) {
            case TASK -> nodeInput(flow, instance, location.nodeIndex());
            case CHOICE_TASK ->
                    recordAt(flow, instance, choiceHeadPath(flow, location.nodeIndex(), state(instance))).getInput();
            case PARALLEL_TASK -> {
                ParallelCursor cursor = state(instance).getParallelCursors().get(location.branchIndex());
                yield recordAt(flow, instance, parallelHeadPath(flow, location.nodeIndex(), cursor)).getInput();
            }
            case JOIN -> null;
        };
    }

    /** 计算顶层节点 pc 的输入：pc==0 取实例输入，否则取上一节点的输出。 */
    private String nodeInput(CompiledFlow flow, WorkflowInstance instance, int pc) {
        if (pc == 0) {
            return instance.getInput();
        }
        FlowNode previous = flow.definition().nodes().get(pc - 1);
        String output = nodeOutput(flow, instance, previous);
        return output != null ? output : instance.getInput();
    }

    /** 取一个已完成节点的输出（选择节点取选中分支末任务输出；并行节点取汇合输出）。 */
    private String nodeOutput(CompiledFlow flow, WorkflowInstance instance, FlowNode node) {
        if (node instanceof TaskNode task) {
            return recordAt(flow, instance, task.name()).getOutput();
        }
        if (node instanceof ChoiceNode choice) {
            for (FlowBranch branch : choice.branches()) {
                TaskNode first = branch.tasks().get(0);
                StepRecord firstRecord = recordAt(flow, instance, choicePath(choice.name(), branch.label(), first.name()));
                if (firstRecord.getStatus() != StepStatus.SKIPPED) {
                    TaskNode last = branch.tasks().get(branch.tasks().size() - 1);
                    return recordAt(flow, instance, choicePath(choice.name(), branch.label(), last.name())).getOutput();
                }
            }
            return null;
        }
        if (node instanceof ParallelNode parallel) {
            return recordAt(flow, instance, joinPath(parallel.name())).getOutput();
        }
        return null;
    }

    private boolean allBranchesCompleted(ExecutionState es) {
        for (ParallelCursor cursor : es.getParallelCursors()) {
            if (cursor.getStatus() != BranchRunStatus.COMPLETED) return false;
        }
        return !es.getParallelCursors().isEmpty();
    }

    private static ExecutionState state(WorkflowInstance instance) {
        ExecutionState es = instance.getExecutionState();
        if (es == null) throw new IllegalStateException("instance " + instance.getId() + " lacks execution state");
        return es;
    }

    private static void syncStepIndex(WorkflowInstance instance, ExecutionState es) {
        instance.setCurrentStepIndex(Math.min(es.getRootPc(), instance.getSteps().size()));
    }

    private static StepRecord recordAt(CompiledFlow flow, WorkflowInstance instance, String path) {
        Integer index = flow.indexByPath().get(path);
        if (index == null) throw new IllegalArgumentException("unknown step path: " + path);
        return instance.getSteps().get(index);
    }

    private static ChoiceNode choiceAt(CompiledFlow flow, int nodeIndex) {
        return (ChoiceNode) flow.definition().nodes().get(nodeIndex);
    }

    private static ParallelNode parallelAt(CompiledFlow flow, int nodeIndex) {
        return (ParallelNode) flow.definition().nodes().get(nodeIndex);
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
}
