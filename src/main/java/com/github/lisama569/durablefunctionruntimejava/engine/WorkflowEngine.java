package com.github.lisama569.durablefunctionruntimejava.engine;

import com.github.lisama569.durablefunctionruntimejava.domain.ErrorKind;
import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.domain.StepRecord;
import com.github.lisama569.durablefunctionruntimejava.domain.StepStatus;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowStatus;
import com.github.lisama569.durablefunctionruntimejava.workflow.StepContext;
import com.github.lisama569.durablefunctionruntimejava.workflow.StepDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.WorkflowDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * 持久化工作流执行引擎。
 *
 * 执行模型：
 * - 所有状态流转只发生在单线程事件循环中，保证同一实例的状态迁移唯一、有序；
 * - 业务步骤在 worker 线程池执行，完成后向事件循环投递完成通知；
 * - 完成通知携带 (instanceId, stepIndex, attempt)，迟到/重复的通知因 attempt 或状态不匹配被丢弃，
 *   不会导致计数错乱或重复推进；
 * - 每次状态迁移先持久化再推进，重启后从持久化状态恢复，已完成步骤不会被重复执行。
 */
public class WorkflowEngine {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);

    private enum EventKind { RUN_STEP, STEP_SUCCEEDED, STEP_FAILED, STEP_TIMED_OUT, TERMINATE }

    private record EngineEvent(EventKind kind, String instanceId, int stepIndex, int attempt,
                               String output, ErrorKind errorKind, String errorMessage) {
        static EngineEvent runStep(String instanceId, int stepIndex, int attempt) {
            return new EngineEvent(EventKind.RUN_STEP, instanceId, stepIndex, attempt, null, null, null);
        }

        static EngineEvent succeeded(String instanceId, int stepIndex, int attempt, String output) {
            return new EngineEvent(EventKind.STEP_SUCCEEDED, instanceId, stepIndex, attempt, output, null, null);
        }

        static EngineEvent failed(String instanceId, int stepIndex, int attempt, ErrorKind errorKind, String message) {
            return new EngineEvent(EventKind.STEP_FAILED, instanceId, stepIndex, attempt, null, errorKind, message);
        }

        static EngineEvent timedOut(String instanceId, int stepIndex, int attempt) {
            return new EngineEvent(EventKind.STEP_TIMED_OUT, instanceId, stepIndex, attempt, null, ErrorKind.TIMEOUT, "step timed out");
        }

        static EngineEvent terminate(String instanceId) {
            return new EngineEvent(EventKind.TERMINATE, instanceId, -1, -1, null, null, null);
        }
    }

    private final WorkflowStore store;
    private final Map<String, WorkflowDefinition> definitions = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<EngineEvent> eventQueue = new LinkedBlockingQueue<>();
    private final ExecutorService eventLoop;
    private final ExecutorService workers;
    private final ScheduledExecutorService scheduler;
    /** 运行中的步骤 future，用于超时/终止时取消。key = instanceId:stepIndex:attempt */
    private final Map<String, Future<?>> runningTasks = new ConcurrentHashMap<>();
    private volatile boolean running = true;

    public WorkflowEngine(WorkflowStore store) {
        this.store = store;
        this.eventLoop = Executors.newSingleThreadExecutor(r -> new Thread(r, "workflow-event-loop"));
        this.workers = Executors.newCachedThreadPool(r -> new Thread(r, "workflow-worker"));
        this.scheduler = Executors.newScheduledThreadPool(2, r -> new Thread(r, "workflow-scheduler"));
        this.eventLoop.submit(this::drainEvents);
    }

    public void register(WorkflowDefinition definition) {
        definitions.put(definition.name(), definition);
    }

    // ---------------------------------------------------------------- 对外 API

    /** 幂等启动：同 (workflowName, idempotencyKey) 重复提交返回已有实例，不产生新副作用。 */
    public WorkflowInstance start(String workflowName, String idempotencyKey, String input) {
        WorkflowDefinition definition = requireDefinition(workflowName);
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
        instance.setCreatedAt(System.currentTimeMillis());
        instance.setUpdatedAt(instance.getCreatedAt());
        for (StepDefinition step : definition.steps()) {
            instance.getSteps().add(new StepRecord(step.name()));
        }
        if (!store.createIfAbsent(instance)) {
            // 并发下同幂等键被其他线程抢先创建，返回已存在实例。
            return store.findByIdempotencyKey(workflowName, idempotencyKey).orElseThrow();
        }
        log.info("[instance={}] workflow={} started, steps={}", instance.getId(), workflowName, definition.steps().size());
        eventQueue.add(EngineEvent.runStep(instance.getId(), 0, 1));
        return instance;
    }

    public Optional<WorkflowInstance> get(String instanceId) {
        return store.findById(instanceId);
    }

    /** 请求终止：实例在步骤边界进入 TERMINATED 终态，已完成步骤不回滚。 */
    public boolean terminate(String instanceId) {
        return store.findById(instanceId)
                .filter(i -> i.getStatus() == WorkflowStatus.RUNNING)
                .map(i -> {
                    eventQueue.add(EngineEvent.terminate(instanceId));
                    return true;
                })
                .orElse(false);
    }

    /** 重启恢复：未完结实例从中断步骤继续，已完成步骤不重复执行。返回恢复的实例数。 */
    public int recover() {
        List<WorkflowInstance> resumable = store.findAll().stream()
                .filter(i -> i.getStatus() == WorkflowStatus.RUNNING)
                .toList();
        for (WorkflowInstance instance : resumable) {
            StepRecord current = instance.getSteps().get(instance.getCurrentStepIndex());
            int nextAttempt = current.getAttempt() + 1;
            log.info("[instance={}] recovering at step={} ({}) attempt={}",
                    instance.getId(), instance.getCurrentStepIndex(), current.getName(), nextAttempt);
            current.setStatus(StepStatus.PENDING);
            store.update(instance);
            eventQueue.add(EngineEvent.runStep(instance.getId(), instance.getCurrentStepIndex(), nextAttempt));
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
            case RUN_STEP -> onRunStep(event);
            case STEP_SUCCEEDED -> onStepSucceeded(event);
            case STEP_FAILED -> onStepFailed(event, event.errorKind(), event.errorMessage());
            case STEP_TIMED_OUT -> onStepTimedOut(event);
            case TERMINATE -> onTerminate(event.instanceId());
        }
    }

    private void onRunStep(EngineEvent event) {
        Optional<WorkflowInstance> opt = store.findById(event.instanceId());
        if (opt.isEmpty()) return;
        WorkflowInstance instance = opt.get();
        if (instance.getStatus() != WorkflowStatus.RUNNING) return;
        if (instance.getCurrentStepIndex() != event.stepIndex()) return;
        StepRecord step = instance.getSteps().get(event.stepIndex());
        if (step.getStatus() != StepStatus.PENDING) return; // 重复的调度事件

        step.setStatus(StepStatus.RUNNING);
        step.setAttempt(event.attempt());
        step.setErrorKind(null);
        step.setErrorMessage(null);
        persistOrThrow(instance);
        log.info("[instance={}] step={} ({}) attempt={} started",
                instance.getId(), event.stepIndex(), step.getName(), event.attempt());
        dispatch(instance, event.stepIndex(), event.attempt());
    }

    private void onStepSucceeded(EngineEvent event) {
        Optional<WorkflowInstance> opt = applicable(event, StepStatus.RUNNING);
        if (opt.isEmpty()) return; // 迟到/重复的完成通知，直接丢弃
        WorkflowInstance instance = opt.get();
        StepRecord step = instance.getSteps().get(event.stepIndex());
        step.setStatus(StepStatus.COMPLETED);
        step.setOutput(event.output());
        log.info("[instance={}] step={} ({}) attempt={} completed",
                instance.getId(), event.stepIndex(), step.getName(), event.attempt());

        int next = event.stepIndex() + 1;
        WorkflowDefinition definition = requireDefinition(instance.getWorkflowName());
        if (next >= definition.steps().size()) {
            instance.setStatus(WorkflowStatus.COMPLETED);
            persistOrThrow(instance);
            log.info("[instance={}] workflow COMPLETED", instance.getId());
        } else {
            instance.setCurrentStepIndex(next);
            String output = event.output();
            instance.getSteps().get(next).setInput(output != null ? output : instance.getInput());
            persistOrThrow(instance);
            eventQueue.add(EngineEvent.runStep(instance.getId(), next, 1));
        }
    }

    private void onStepFailed(EngineEvent event, ErrorKind kind, String message) {
        Optional<WorkflowInstance> opt = applicable(event, StepStatus.RUNNING);
        if (opt.isEmpty()) return;
        WorkflowInstance instance = opt.get();
        StepRecord step = instance.getSteps().get(event.stepIndex());
        step.setErrorKind(kind);
        step.setErrorMessage(message);
        failOrRetry(instance, event.stepIndex(), event.attempt(), kind, message);
    }

    private void onStepTimedOut(EngineEvent event) {
        Optional<WorkflowInstance> opt = applicable(event, StepStatus.RUNNING);
        if (opt.isEmpty()) return;
        WorkflowInstance instance = opt.get();
        cancelRunningTask(event.instanceId(), event.stepIndex(), event.attempt());
        StepRecord step = instance.getSteps().get(event.stepIndex());
        step.setErrorKind(ErrorKind.TIMEOUT);
        step.setErrorMessage("step exceeded timeout");
        failOrRetry(instance, event.stepIndex(), event.attempt(), ErrorKind.TIMEOUT, "step exceeded timeout");
    }

    private void onTerminate(String instanceId) {
        Optional<WorkflowInstance> opt = store.findById(instanceId);
        if (opt.isEmpty()) return;
        WorkflowInstance instance = opt.get();
        if (instance.getStatus() != WorkflowStatus.RUNNING) return; // 终态不可逆
        StepRecord current = instance.getSteps().get(instance.getCurrentStepIndex());
        if (current.getStatus() == StepStatus.RUNNING) {
            cancelRunningTask(instanceId, instance.getCurrentStepIndex(), current.getAttempt());
            current.setStatus(StepStatus.FAILED);
            current.setErrorMessage("terminated");
        }
        instance.setStatus(WorkflowStatus.TERMINATED);
        persistOrThrow(instance);
        log.info("[instance={}] workflow TERMINATED at step={}", instanceId, instance.getCurrentStepIndex());
    }

    // ---------------------------------------------------------------- 内部机制

    /** 校验通知是否适用于当前状态：状态、步骤序号、attempt 三者必须同时匹配，否则为迟到/重复通知。 */
    private Optional<WorkflowInstance> applicable(EngineEvent event, StepStatus expected) {
        return store.findById(event.instanceId())
                .filter(i -> i.getStatus() == WorkflowStatus.RUNNING)
                .filter(i -> i.getCurrentStepIndex() == event.stepIndex())
                .filter(i -> {
                    StepRecord step = i.getSteps().get(event.stepIndex());
                    return step.getStatus() == expected && step.getAttempt() == event.attempt();
                });
    }

    /** 失败处理：可重试且未达上限则按退避策略重试，否则落入确定终态。 */
    private void failOrRetry(WorkflowInstance instance, int stepIndex, int attempt, ErrorKind kind, String message) {
        StepRecord step = instance.getSteps().get(stepIndex);
        StepDefinition definition = requireDefinition(instance.getWorkflowName()).steps().get(stepIndex);
        RetryPolicy policy = definition.retryPolicy();

        if (kind == ErrorKind.NON_RETRYABLE) {
            step.setStatus(StepStatus.FAILED);
            instance.setStatus(WorkflowStatus.FAILED);
            persistOrThrow(instance);
            log.warn("[instance={}] step={} ({}) NON_RETRYABLE failure, workflow FAILED: {}",
                    instance.getId(), stepIndex, step.getName(), message);
            return;
        }
        if (attempt < policy.maxAttempts()) {
            long backoff = policy.backoffForAttempt(attempt);
            step.setStatus(StepStatus.PENDING);
            persistOrThrow(instance);
            log.warn("[instance={}] step={} ({}) attempt={} failed ({}), retry in {}ms: {}",
                    instance.getId(), stepIndex, step.getName(), attempt, kind, backoff, message);
            scheduler.schedule(
                    () -> eventQueue.add(EngineEvent.runStep(instance.getId(), stepIndex, attempt + 1)),
                    backoff, TimeUnit.MILLISECONDS);
            return;
        }
        // 重试耗尽或超时耗尽：落入确定终态，不停留中间态。
        step.setStatus(StepStatus.FAILED);
        instance.setStatus(kind == ErrorKind.TIMEOUT ? WorkflowStatus.TIMED_OUT : WorkflowStatus.FAILED);
        persistOrThrow(instance);
        log.warn("[instance={}] step={} ({}) exhausted {} attempt(s) ({}), workflow {}",
                instance.getId(), stepIndex, step.getName(), attempt, kind, instance.getStatus());
    }

    /** 将步骤交给 worker 线程执行，并安排超时看门狗。 */
    private void dispatch(WorkflowInstance instance, int stepIndex, int attempt) {
        WorkflowDefinition definition = requireDefinition(instance.getWorkflowName());
        StepDefinition stepDef = definition.steps().get(stepIndex);
        StepRecord step = instance.getSteps().get(stepIndex);
        String input = step.getInput() != null ? step.getInput() : instance.getInput();
        StepContext context = new StepContext(instance.getId(), stepDef.name(), attempt);
        String instanceId = instance.getId();

        Future<?> task = workers.submit(() -> {
            try {
                String output = stepDef.handler().execute(context, input);
                eventQueue.add(EngineEvent.succeeded(instanceId, stepIndex, attempt, output));
            } catch (RetryableException e) {
                eventQueue.add(EngineEvent.failed(instanceId, stepIndex, attempt, ErrorKind.RETRYABLE, e.getMessage()));
            } catch (NonRetryableException e) {
                eventQueue.add(EngineEvent.failed(instanceId, stepIndex, attempt, ErrorKind.NON_RETRYABLE, e.getMessage()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // 被超时/终止取消：不投递通知，状态已由事件循环推进。
            } catch (Exception e) {
                // 未分类异常默认不可重试，避免无限重试放大故障。
                eventQueue.add(EngineEvent.failed(instanceId, stepIndex, attempt, ErrorKind.NON_RETRYABLE, e.toString()));
            } finally {
                runningTasks.remove(taskKey(instanceId, stepIndex, attempt));
            }
        });
        runningTasks.put(taskKey(instanceId, stepIndex, attempt), task);
        scheduler.schedule(() -> eventQueue.add(EngineEvent.timedOut(instanceId, stepIndex, attempt)),
                stepDef.retryPolicy().stepTimeoutMillis(), TimeUnit.MILLISECONDS);
    }

    private void cancelRunningTask(String instanceId, int stepIndex, int attempt) {
        Future<?> task = runningTasks.remove(taskKey(instanceId, stepIndex, attempt));
        if (task != null) {
            task.cancel(true);
        }
    }

    private static String taskKey(String instanceId, int stepIndex, int attempt) {
        return instanceId + ":" + stepIndex + ":" + attempt;
    }

    private void persistOrThrow(WorkflowInstance instance) {
        if (!store.update(instance)) {
            throw new IllegalStateException("concurrent modification on instance " + instance.getId());
        }
    }

    private WorkflowDefinition requireDefinition(String workflowName) {
        WorkflowDefinition definition = definitions.get(workflowName);
        if (definition == null) {
            throw new IllegalArgumentException("unknown workflow: " + workflowName);
        }
        return definition;
    }
}
