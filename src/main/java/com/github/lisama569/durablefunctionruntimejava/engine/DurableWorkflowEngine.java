package com.github.lisama569.durablefunctionruntimejava.engine;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.lisama569.durablefunctionruntimejava.engine.error.NonRetryableFailureException;
import com.github.lisama569.durablefunctionruntimejava.engine.error.RetryableFailureException;
import com.github.lisama569.durablefunctionruntimejava.engine.error.StepFailureException;
import com.github.lisama569.durablefunctionruntimejava.engine.function.AsyncTask;
import com.github.lisama569.durablefunctionruntimejava.engine.function.CompletionSink;
import com.github.lisama569.durablefunctionruntimejava.engine.function.IdempotenceService;
import com.github.lisama569.durablefunctionruntimejava.engine.function.StepContext;
import com.github.lisama569.durablefunctionruntimejava.engine.function.StepDefinition;
import com.github.lisama569.durablefunctionruntimejava.engine.function.WorkflowDefinition;
import com.github.lisama569.durablefunctionruntimejava.engine.json.Json;
import com.github.lisama569.durablefunctionruntimejava.engine.model.AttemptRecord;
import com.github.lisama569.durablefunctionruntimejava.engine.model.CompletionRequest;
import com.github.lisama569.durablefunctionruntimejava.engine.model.ErrorType;
import com.github.lisama569.durablefunctionruntimejava.engine.model.InstanceStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.model.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StartRequest;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StepState;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StepStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.model.WorkflowEvent;
import com.github.lisama569.durablefunctionruntimejava.engine.model.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.engine.persistence.StateStore;

/**
 * The durable execution engine.
 *
 * <p>Concurrency model: every state transition of an instance happens inside
 * {@link StateStore#mutate}, which holds an exclusive per-instance lock and
 * commits the whole aggregate atomically. Different instances are fully
 * independent. Synchronous steps are executed while that lock is held, so
 * callbacks (completion / terminate / timeout) can never interleave with a
 * running step. Async steps leave the lock as soon as work has been
 * dispatched and resume only through a token-checked completion or the
 * timeout sweep.
 *
 * <p>State-flow invariants:
 * <ul>
 *   <li>COMPLETED / FAILED / TERMINATED are terminal; transitions out are
 *       rejected, so state can never roll back after crash or duplicate
 *       notification.</li>
 *   <li>A step is executed at most once to completion: completed steps are
 *       skipped on every replay; in-flight attempts are token-bound and
 *       stale callbacks are ignored.</li>
 *   <li>Every failure path ends in either RETRY_WAIT/WAITING with a deadline
 *       or a terminal status — nothing is ever left in a limbo state.</li>
 * </ul>
 */
public class DurableWorkflowEngine implements IdempotenceService, CompletionSink {

    private static final Logger log = LoggerFactory.getLogger("DURABLE-WORKFLOW");

    private final StateStore store;
    private final WorkflowRegistry registry;
    private final EngineProperties properties;

    private ExecutorService stepWorkers;
    private ScheduledExecutorService ticker;
    private volatile boolean running;

    public DurableWorkflowEngine(StateStore store, WorkflowRegistry registry) {
        this(store, registry, new EngineProperties());
    }

    public DurableWorkflowEngine(StateStore store, WorkflowRegistry registry,
                                 EngineProperties properties) {
        this.store = store;
        this.registry = registry;
        this.properties = properties;
    }

    /** Overridable clock (tests use deterministic time). */
    protected long nowMs() {
        return System.currentTimeMillis();
    }

    // ------------------------------------------------------------ bookkeeping

    private void addEvent(WorkflowInstance inst, String type, String step,
                          Integer attempt, String detail) {
        if (inst == null) {
            log.info("(no-instance) event={} step={} detail={}", type, step, detail);
            return;
        }
        long seq = inst.getEvents().size() + 1L;
        inst.getEvents().add(new WorkflowEvent(seq, nowMs(), type, step, attempt, detail));
        log.info("[{}] step={} attempt={} event={} detail={}",
                inst.getInstanceId(), step == null ? "-" : step,
                attempt == null ? "-" : attempt, type, detail);
    }

    private static String newInstanceId() {
        return UUID.randomUUID().toString();
    }

    static String tokenOf(String instanceId, String stepName, int attempt) {
        String raw = instanceId + "|" + stepName + "|" + attempt + "|"
                + Long.toHexString((instanceId + stepName + attempt).hashCode() & 0xffffffffL);
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(raw.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest).substring(0, 24);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static long cappedMaxAttempts(RetryPolicy p) {
        return Math.max(1L, Math.min(p.getMaxAttempts(), RetryPolicy.HARD_MAX_ATTEMPTS));
    }

    static long cappedTimeout(RetryPolicy p) {
        return Math.max(0L, Math.min(p.getTimeoutMs(), RetryPolicy.HARD_MAX_TIMEOUT_MS));
    }

    static long backoffMs(RetryPolicy p, int nextAttemptNo) {
        // Wait before attempt N (1-based): initial * multiplier^(N-2), capped.
        long initial = Math.max(0L, p.getInitialBackoffMs());
        long cap = Math.min(Math.max(0L, p.getMaxBackoffMs()), RetryPolicy.HARD_MAX_BACKOFF_MS);
        double mult = Math.max(1.0, p.getBackoffMultiplier());
        long delay = initial;
        for (int i = 2; i < nextAttemptNo; i++) {
            if (delay >= cap) {
                return cap;
            }
            delay = (long) Math.min(cap, delay * mult);
        }
        return Math.min(delay, cap);
    }

    private static StepFailureException classify(Throwable t) {
        if (t instanceof StepFailureException sfe) {
            return sfe;
        }
        return new RetryableFailureException("UNEXPECTED_ERROR",
                t.getClass().getSimpleName() + ": " + String.valueOf(t.getMessage()));
    }

    private void failInstance(WorkflowInstance inst, ErrorType type, String code, String message) {
        inst.setStatus(InstanceStatus.FAILED);
        inst.setErrorType(type.code());
        inst.setErrorCode(code);
        inst.setErrorMessage(message);
        inst.setCompletedAtMs(nowMs());
    }

    // ------------------------------------------------------------------ start

    /**
     * Start a workflow instance. Duplicate submissions (same instanceId)
     * return the existing instance unchanged: no step is re-run and no side
     * effect is duplicated.
     */
    public WorkflowInstance start(StartRequest request) {
        if (request == null || request.getWorkflowName() == null) {
            throw new IllegalArgumentException("workflowName is required");
        }
        WorkflowDefinition def = registry.get(request.getWorkflowName());
        if (def == null) {
            throw new IllegalArgumentException("unknown workflow: " + request.getWorkflowName());
        }
        String instanceId = request.getInstanceId() != null ? request.getInstanceId() : newInstanceId();
        String dedupe = request.getDedupeKey() == null ? null : "START:" + request.getDedupeKey();

        WorkflowInstance committed = store.mutate(instanceId, true, existing -> {
            if (existing != null) {
                if (dedupe != null && existing.getDedupeKeys().contains(dedupe)) {
                    addEvent(existing, "START_DEDUPED", null, null,
                            "duplicate submission ignored key=" + request.getDedupeKey());
                    return existing;
                }
                addEvent(existing, "START_DEDUPED", null, null,
                        "instanceId reused; existing state returned untouched");
                return existing;
            }
            WorkflowInstance inst = new WorkflowInstance();
            inst.setInstanceId(instanceId);
            inst.setWorkflowName(def.name());
            inst.setStatus(InstanceStatus.RUNNING);
            inst.setCurrentStepIndex(0);
            inst.setCreatedAtMs(nowMs());
            inst.setUpdatedAtMs(nowMs());
            inst.setInput(request.getInput());
            for (StepDefinition sd : def.steps()) {
                inst.getSteps().put(sd.name(), new StepState(sd.name()));
            }
            if (dedupe != null) {
                inst.getDedupeKeys().add(dedupe);
            }
            addEvent(inst, "STARTED", null, null, "workflow=" + def.name());
            return inst;
        });

        driveAsync(instanceId);
        return store.load(instanceId);
    }

    // ------------------------------------------------------------------ read

    public WorkflowInstance get(String instanceId) {
        WorkflowInstance inst = store.load(instanceId);
        if (inst == null) {
            throw new IllegalArgumentException("unknown instance: " + instanceId);
        }
        return inst;
    }

    public List<String> listInstances() {
        return store.listInstanceIds();
    }

    // ------------------------------------------------------------- terminate

    /**
     * Request termination. Terminal instances are returned unchanged. Any
     * in-flight attempt is recorded as INTERRUPTED and the instance settles
     * in the TERMINATED state; already completed steps stay completed.
     */
    public WorkflowInstance terminate(String instanceId, String reason) {
        WorkflowInstance committed = store.mutate(instanceId, false, inst -> {
            if (inst == null) {
                return null;
            }
            if (inst.isTerminal()) {
                addEvent(inst, "TERMINATE_IGNORED", null, null,
                        "instance already " + inst.getStatus());
                return inst;
            }
            WorkflowDefinition def = registry.get(inst.getWorkflowName());
            if (def != null && inst.getCurrentStepIndex() < def.steps().size()) {
                StepState step = inst.stepAt(inst.getCurrentStepIndex());
                if (step != null && (step.getStatus() == StepStatus.RUNNING
                        || step.getStatus() == StepStatus.WAITING
                        || step.getStatus() == StepStatus.RETRY_WAIT)) {
                    if (step.getCurrentAttempt() > 0) {
                        AttemptRecord rec = lastAttempt(step);
                        if (rec != null && "RUNNING".equals(rec.getOutcome())) {
                            rec.finish("INTERRUPTED", ErrorType.INTERRUPTED,
                                    "INTERRUPTED", "terminated: " + reason, nowMs());
                        }
                    }
                    step.setStatus(StepStatus.FAILED);
                    step.setErrorType(ErrorType.TERMINATED.code());
                    step.setErrorCode(ErrorType.TERMINATED.code());
                    step.setErrorMessage("terminated: " + reason);
                    step.setActiveToken(null);
                    addEvent(inst, "STEP_TERMINATED", step.getStepName(),
                            step.getCurrentAttempt(), reason);
                }
            }
            inst.setStatus(InstanceStatus.TERMINATED);
            inst.setErrorType(ErrorType.TERMINATED.code());
            inst.setErrorCode(ErrorType.TERMINATED.code());
            inst.setErrorMessage("terminated: " + reason);
            inst.setCompletedAtMs(nowMs());
            inst.setUpdatedAtMs(nowMs());
            addEvent(inst, "TERMINATED", null, null, reason);
            return inst;
        });
        if (committed == null) {
            throw new IllegalArgumentException("unknown instance: " + instanceId);
        }
        return committed;
    }

    private static AttemptRecord lastAttempt(StepState step) {
        return step.getAttempts().isEmpty() ? null
                : step.getAttempts().get(step.getAttempts().size() - 1);
    }

    // ------------------------------------------------------------ completion

    /** REST/Callback entry point; duplicate or stale notifications are no-ops. */
    public WorkflowInstance complete(CompletionRequest request) {
        if (request == null || request.getToken() == null) {
            throw new IllegalArgumentException("completion token is required");
        }
        String instanceId = complete(request.getToken(), request.isSuccess(), request.getResult(),
                request.getErrorCode(), request.getErrorMessage(), request.getDedupeKey());
        return instanceId == null ? null : store.load(instanceId);
    }

    /** Outcome carrier for the single-lock callback application. */
    private static final class CallbackResult {
        String instanceId; // owner of a matching token (live or settled)
        boolean applied;   // true when a live transition was committed
        boolean needsDrive;
    }

    /**
     * Apply a completion to exactly one instance, entirely under that
     * instance's lock. Token lookup and state transition are therefore
     * atomic with respect to other callbacks/drives/terminations.
     */
    private void applyCallbackToInstance(String instanceId, String token, boolean success,
                                         JsonNode result, String errorCode,
                                         String errorMessage, String dedupeKey,
                                         CallbackResult out) {
        store.mutate(instanceId, false, inst -> {
            if (inst == null) {
                return null;
            }
            StepState matching = null;
            boolean activeMatch = false;
            for (StepState st : inst.getSteps().values()) {
                if (token.equals(st.getActiveToken())) {
                    matching = st;
                    activeMatch = true;
                    break;
                }
                for (AttemptRecord a : st.getAttempts()) {
                    if (token.equals(a.getToken())) {
                        matching = st;
                        activeMatch = false;
                        break;
                    }
                }
                if (matching != null) {
                    break;
                }
            }
            if (matching == null) {
                return null; // token not on this instance: leave file untouched
            }
            out.instanceId = instanceId;
            String stepName = matching.getStepName();

            if (inst.isTerminal() || !activeMatch) {
                addEvent(inst, "CALLBACK_IGNORED", stepName, null,
                        "duplicate/late callback rejected for "
                                + (inst.isTerminal() ? "terminal" : "settled") + " attempt");
                inst.setUpdatedAtMs(nowMs());
                return inst;
            }
            if (dedupeKey != null) {
                String key = "CALLBACK:" + dedupeKey;
                if (inst.getDedupeKeys().contains(key)) {
                    addEvent(inst, "CALLBACK_DEDUPED", stepName, matching.getCurrentAttempt(),
                            "duplicate callback key=" + dedupeKey);
                    return inst;
                }
                inst.getDedupeKeys().add(key);
            }
            int attempt = matching.getCurrentAttempt();
            AttemptRecord rec = lastAttempt(matching);
            if (success) {
                if (rec != null) {
                    rec.finish("SUCCESS", ErrorType.NONE, null, null, nowMs());
                }
                matching.setStatus(StepStatus.COMPLETED);
                matching.setOutput(result);
                matching.setActiveToken(null);
                matching.setErrorType(null);
                matching.setErrorCode(null);
                matching.setErrorMessage(null);
                addEvent(inst, "STEP_COMPLETED", stepName, attempt, "async callback success");
                // Advance within this same commit: a subsequent observer can
                // never see the completed step without also seeing the
                // workflow advanced (no window for false restart recovery).
                inst.setCurrentStepIndex(inst.getCurrentStepIndex() + 1);
                maybeCompleteWorkflow(inst, defOf(inst));
            } else {
                ErrorType type = ErrorType.RETRYABLE;
                if (rec != null) {
                    rec.finish("FAILED", type,
                            errorCode == null ? "CALLBACK_FAILED" : errorCode,
                            errorMessage, nowMs());
                }
                applyFailure(inst, matching,
                        defOf(inst).steps().get(inst.getCurrentStepIndex()),
                        type, errorCode == null ? "CALLBACK_FAILED" : errorCode, errorMessage);
            }
            inst.setUpdatedAtMs(nowMs());
            out.applied = true;
            out.needsDrive = true;
            return inst;
        });
    }

    @Override
    public String complete(String token, boolean success, JsonNode result,
                           String errorCode, String errorMessage, String dedupeKey) {
        CallbackResult out = new CallbackResult();
        for (String id : store.listInstanceIds()) {
            applyCallbackToInstance(id, token, success, result, errorCode,
                    errorMessage, dedupeKey, out);
            if (out.instanceId != null) {
                break;
            }
        }
        if (out.instanceId == null) {
            log.info("token={} callback ignored: unknown token", token);
            return null;
        }
        if (out.needsDrive) {
            driveAsync(out.instanceId);
        }
        return out.instanceId;
    }

    private WorkflowDefinition defOf(WorkflowInstance inst) {
        WorkflowDefinition def = registry.get(inst.getWorkflowName());
        if (def == null) {
            throw new IllegalStateException("workflow definition not registered: "
                    + inst.getWorkflowName());
        }
        return def;
    }

    // -------------------------------------------------------------- idempotence

    @Override
    public JsonNode runOnce(String instanceId, String stepName, String effectKey,
                            StepContext.SideEffect effect) throws Exception {
        byte[] existing = store.loadEffect(instanceId, stepName, effectKey);
        if (existing != null) {
            log.info("[{}] step={} effect={} replayed (not re-executed)",
                    instanceId, stepName, effectKey);
            ObjectNode wrapper = (ObjectNode) Json.mapper().readTree(existing);
            return wrapper.get("result");
        }
        JsonNode value = effect.execute();
        ObjectNode wrapper = Json.newObject();
        wrapper.set("result", value);
        store.saveEffect(instanceId, stepName, effectKey,
                Json.mapper().writeValueAsBytes(wrapper));
        log.info("[{}] step={} effect={} executed once and persisted",
                instanceId, stepName, effectKey);
        return value;
    }

    // ------------------------------------------------------------------ drive

    private static final class StartDecision {
        boolean start;
        boolean async;
        boolean changed;
        JsonNode stepInput;
    }

    /**
     * One drive pass: restart-interrupted recovery, timeout enforcement,
     * backoff expiry, sequential step advancement. Safe to run any number of
     * times; all progress is committed inside the per-instance lock.
     */
    WorkflowInstance drive(String instanceId) {
        WorkflowInstance inst = store.load(instanceId);
        if (inst == null || inst.isTerminal()) {
            return inst;
        }
        WorkflowDefinition def = defOf(inst);

        // Loop bounded by the number of steps: only synchronous completions
        // advance within the same pass; async/backoff/terminal break out.
        int guard = def.steps().size() + 1;
        while (guard-- > 0) {
            inst = store.load(instanceId);
            if (inst == null || inst.isTerminal()) {
                return inst;
            }
            int idx = inst.getCurrentStepIndex();
            if (idx >= def.steps().size()) {
                return inst;
            }
            StepDefinition sd = def.steps().get(idx);
            StepState step = inst.stepAt(idx);

            if (step.isDone()) {
                inst = store.mutate(instanceId, false, x -> {
                    if (x == null || x.isTerminal()) {
                        return x;
                    }
                    StepState current = x.stepAt(x.getCurrentStepIndex());
                    if (current != null && current.isDone()) {
                        x.setCurrentStepIndex(x.getCurrentStepIndex() + 1);
                        maybeCompleteWorkflow(x, defOf(x));
                        x.setUpdatedAtMs(nowMs());
                    }
                    return x;
                });
                continue;
            }

            StartDecision decision = evaluateStep(instanceId, inst, sd, step);
            if (decision.changed) {
                // Timeout/retry transition just committed: re-read truth from
                // disk before deciding anything else in this pass.
                continue;
            }
            if (!decision.start) {
                return inst; // waiting for callback / backoff / deadline
            }

            if (decision.async) {
                return startAsyncStep(instanceId, inst, sd, step, decision.stepInput);
            }
            runSyncStep(instanceId, sd, decision.stepInput);
            inst = store.load(instanceId);
            if (inst == null || inst.isTerminal() || inst.getCurrentStepIndex() == idx) {
                return inst; // failed-into-backoff/terminal: next pass/tick continues
            }
        }
        return store.load(instanceId);
    }

    /** Decide whether the step is due to (re)start right now. */
    private StartDecision evaluateStep(String instanceId, WorkflowInstance inst,
                                       StepDefinition sd, StepState step) {
        StartDecision d = new StartDecision();
        long now = nowMs();
        switch (step.getStatus()) {
            case PENDING -> d.start = true;
            case RETRY_WAIT -> d.start = now >= step.getNotBeforeMs();
            case RUNNING, WAITING -> {
                long deadline = step.getAttemptDeadlineMs();
                long timeout = cappedTimeout(sd.retryPolicy());
                if (timeout > 0 && deadline > 0 && now >= deadline) {
                    boolean retryImmediately = handleTimeout(instanceId, inst, sd, step);
                    d.changed = true;
                    d.start = retryImmediately;
                } else {
                    d.start = false;
                }
            }
            case FAILED -> {
                // FAILED on a non-terminal instance only exists transiently;
                // recovery sweeps RUNNING attempts, nothing to start here.
                d.start = false;
            }
            default -> d.start = false;
        }
        if (d.start) {
            d.async = sd.async();
            d.stepInput = resolveInput(inst, sd);
        }
        return d;
    }

    /**
     * Persist the timeout transition. Returns true only when the policy
     * allows an immediate retry (zero backoff and budget left).
     */
    private boolean handleTimeout(String instanceId, WorkflowInstance snapshot,
                                  StepDefinition sd, StepState step) {
        final boolean[] retryNow = {false};
        final boolean[] changed = {false};
        store.mutate(instanceId, false, x -> {
            if (x == null || x.isTerminal()) {
                return x;
            }
            StepState live = x.getSteps().get(sd.name());
            if (live.getStatus() != StepStatus.RUNNING && live.getStatus() != StepStatus.WAITING) {
                return x;
            }
            if (cappedTimeout(sd.retryPolicy()) <= 0 || live.getAttemptDeadlineMs() == 0
                    || nowMs() < live.getAttemptDeadlineMs()) {
                return x;
            }
            boolean wasAsync = live.getStatus() == StepStatus.WAITING;
            AttemptRecord rec = lastAttempt(live);
            int timedOutAttempt = live.getCurrentAttempt();
            if (rec != null && "RUNNING".equals(rec.getOutcome())) {
                rec.finish("TIMED_OUT", ErrorType.TIMEOUT, "STEP_TIMEOUT",
                        "attempt " + timedOutAttempt + " timed out after "
                                + cappedTimeout(sd.retryPolicy()) + "ms", nowMs());
            }
            applyFailure(x, live, sd, ErrorType.TIMEOUT, "STEP_TIMEOUT",
                    (wasAsync ? "async" : "step") + " attempt timed out");
            addEvent(x, "STEP_TIMEOUT", sd.name(), timedOutAttempt, "deadline reached");
            retryNow[0] = live.getStatus() == StepStatus.RETRY_WAIT
                    && nowMs() >= live.getNotBeforeMs();
            changed[0] = true;
            x.setUpdatedAtMs(nowMs());
            return x;
        });
        // If nothing transitioned (deadline race), tell the caller not to restart.
        if (!changed[0]) {
            return false;
        }
        return retryNow[0];
    }

    /**
     * Apply a failed attempt: either schedule the next attempt with capped
     * backoff or settle the step (and instance) in a terminal FAILED state.
     */
    private void applyFailure(WorkflowInstance inst, StepState step, StepDefinition sd,
                              ErrorType type, String code, String message) {
        step.setErrorType(type.code());
        step.setErrorCode(code);
        step.setErrorMessage(message);
        step.setActiveToken(null);

        long maxAttempts = cappedMaxAttempts(sd.retryPolicy());
        boolean canRetry = type.retryable() && step.getCurrentAttempt() < maxAttempts;

        if (!canRetry) {
            step.setStatus(StepStatus.FAILED);
            ErrorType terminalType = type.retryable() ? ErrorType.RETRIES_EXHAUSTED : type;
            String terminalCode = type.retryable() ? "RETRIES_EXHAUSTED:" + code : code;
            String terminalMsg = type.retryable()
                    ? "retries exhausted after " + step.getCurrentAttempt()
                            + "/" + maxAttempts + " attempts; last=" + code + ": " + message
                    : message;
            addEvent(inst, "STEP_FAILED_TERMINAL", step.getStepName(),
                    step.getCurrentAttempt(), terminalType.code() + " " + terminalMsg);
            failInstance(inst, terminalType, terminalCode, terminalMsg);
            return;
        }

        // currentAttempt stays on the failed attempt; beginAttempt() performs
        // the only authoritative increment when the retry actually starts.
        int nextAttempt = step.getCurrentAttempt() + 1;
        long waitMs = backoffMs(sd.retryPolicy(), nextAttempt);
        step.setStatus(StepStatus.RETRY_WAIT);
        step.setNotBeforeMs(nowMs() + waitMs);
        step.setAttemptDeadlineMs(0L);
        addEvent(inst, "STEP_RETRY_SCHEDULED", step.getStepName(), nextAttempt,
                "backoffMs=" + waitMs + " reason=" + code);
    }

    private JsonNode resolveInput(WorkflowInstance inst, StepDefinition sd) {
        if (sd.inputBuilder() != null) {
            return sd.inputBuilder().apply(inst.getInput());
        }
        int idx = inst.getCurrentStepIndex();
        if (idx == 0) {
            return inst.getInput();
        }
        StepState prev = inst.stepAt(idx - 1);
        return prev == null ? null : prev.getOutput();
    }

    private AttemptRecord beginAttempt(StepState step, JsonNode stepInput, RetryPolicy policy) {
        int attempt = step.getCurrentAttempt() + 1;
        step.setCurrentAttempt(attempt);
        step.setInput(stepInput);
        AttemptRecord record = AttemptRecord.start(attempt, nowMs());
        step.getAttempts().add(record);
        long timeout = cappedTimeout(policy);
        step.setAttemptDeadlineMs(timeout > 0 ? nowMs() + timeout : 0L);
        step.setErrorType(null);
        step.setErrorCode(null);
        step.setErrorMessage(null);
        return record;
    }

    private void runSyncStep(String instanceId, StepDefinition sd, JsonNode stepInput) {
        // Attempt boundary, business execution and result settlement happen in
        // ONE lock-protected commit. No other drive/callback/recovery can
        // observe this step in RUNNING state, so an in-flight attempt can
        // never be mistaken for an interrupted one. Persistence of the
        // "boundary" is still guaranteed: the whole transition (including the
        // started attempt record) is on disk before the lock is released.
        store.mutate(instanceId, false, x -> {
            if (x == null || x.isTerminal()) {
                return x;
            }
            StepState live = x.getSteps().get(sd.name());
            if (live == null || live.isDone()
                    || (live.getStatus() == StepStatus.RETRY_WAIT
                            && nowMs() < live.getNotBeforeMs())) {
                return x;
            }
            if (live.getStatus() == StepStatus.RUNNING || live.getStatus() == StepStatus.WAITING) {
                // Another committed generation owns this attempt; do not touch.
                return x;
            }
            beginAttempt(live, stepInput, sd.retryPolicy());
            live.setStatus(StepStatus.RUNNING);
            addEvent(x, "STEP_ATTEMPT_START", sd.name(), live.getCurrentAttempt(),
                    "sync attempt " + live.getCurrentAttempt() + "/"
                            + cappedMaxAttempts(sd.retryPolicy()));

            JsonNode output = null;
            Throwable thrown = null;
            try {
                StepContext ctx = new StepContext(instanceId, sd.name(),
                        live.getCurrentAttempt(), stepInput, this);
                output = sd.function().execute(ctx);
            } catch (Throwable t) {
                thrown = t;
            }

            AttemptRecord rec = lastAttempt(live);
            if (thrown == null) {
                if (rec != null) {
                    rec.finish("SUCCESS", ErrorType.NONE, null, null, nowMs());
                }
                live.setStatus(StepStatus.COMPLETED);
                live.setOutput(output);
                live.setAttemptDeadlineMs(0L);
                addEvent(x, "STEP_COMPLETED", sd.name(), live.getCurrentAttempt(),
                        "sync success");
                x.setCurrentStepIndex(x.getCurrentStepIndex() + 1);
                maybeCompleteWorkflow(x, defOf(x));
            } else {
                StepFailureException sfe = classify(thrown);
                if (rec != null) {
                    rec.finish("FAILED", sfe.getErrorType(), sfe.getCode(),
                            String.valueOf(sfe.getMessage()), nowMs());
                }
                live.setStatus(StepStatus.RUNNING); // applyFailure decides next state
                applyFailure(x, live, sd, sfe.getErrorType(), sfe.getCode(),
                        String.valueOf(sfe.getMessage()));
            }
            x.setUpdatedAtMs(nowMs());
            return x;
        });
    }

    private WorkflowInstance startAsyncStep(String instanceId, WorkflowInstance inst,
                                            StepDefinition sd, StepState step, JsonNode stepInput) {
        // 1) persist attempt + token + WAITING before any dispatch
        store.mutate(instanceId, false, x -> {
            StepState live = x.getSteps().get(sd.name());
            beginAttempt(live, stepInput, sd.retryPolicy());
            live.setStatus(StepStatus.WAITING);
            String tok = tokenOf(instanceId, sd.name(), live.getCurrentAttempt());
            live.setActiveToken(tok);
            AttemptRecord rec0 = lastAttempt(live);
            if (rec0 != null) {
                rec0.setToken(tok);
            }
            addEvent(x, "STEP_ATTEMPT_START", sd.name(), live.getCurrentAttempt(),
                    "async attempt dispatched");
            x.setUpdatedAtMs(nowMs());
            return x;
        });

        // 2) dispatch outside the lock (best effort: failure is a retryable error)
        WorkflowInstance persisted = store.load(instanceId);
        StepState live = persisted.getSteps().get(sd.name());
        try {
            AsyncTask task = new AsyncTask(live.getActiveToken(), instanceId, sd.name(),
                    live.getCurrentAttempt(), live.getInput(), this);
            sd.dispatcher().dispatch(task);
        } catch (Throwable t) {
            StepFailureException sfe = classify(t);
            store.mutate(instanceId, false, x -> {
                StepState s = x.getSteps().get(sd.name());
                if (s.getStatus() != StepStatus.WAITING) {
                    return x;
                }
                AttemptRecord rec = lastAttempt(s);
                if (rec != null) {
                    rec.finish("FAILED", sfe.getErrorType(), "DISPATCH_FAILED:" + sfe.getCode(),
                            String.valueOf(sfe.getMessage()), nowMs());
                }
                applyFailure(x, s, sd, sfe.getErrorType(),
                        "DISPATCH_FAILED:" + sfe.getCode(), String.valueOf(sfe.getMessage()));
                x.setUpdatedAtMs(nowMs());
                return x;
            });
        }
        return store.load(instanceId);
    }

    private void maybeCompleteWorkflow(WorkflowInstance inst, WorkflowDefinition def) {
        if (inst.getStatus().isTerminal()) {
            return;
        }
        if (inst.getCurrentStepIndex() < def.steps().size()) {
            return;
        }
        StepState last = inst.stepAt(def.steps().size() - 1);
        if (def.outputBuilder() != null) {
            ObjectNode outputs = Json.newObject();
            for (StepState s : inst.getSteps().values()) {
                outputs.set(s.getStepName(), s.getOutput());
            }
            inst.setOutput(def.outputBuilder().apply(outputs));
        } else {
            inst.setOutput(last == null ? null : last.getOutput());
        }
        inst.setStatus(InstanceStatus.COMPLETED);
        inst.setCompletedAtMs(nowMs());
        addEvent(inst, "WORKFLOW_COMPLETED", null, null, null);
    }

    // ------------------------------------------------------------ recovery loop

    public synchronized void startBackground() {
        if (running) {
            return;
        }
        running = true;
        stepWorkers = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "durable-step-worker");
            t.setDaemon(true);
            return t;
        });
        recoverAll();
        ticker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "durable-ticker");
            t.setDaemon(true);
            return t;
        });
        long interval = Math.max(20L, properties.getTickIntervalMs());
        ticker.scheduleAtFixedRate(this::tickAll, interval, interval, TimeUnit.MILLISECONDS);
    }

    public synchronized void stopBackground() {
        running = false;
        if (ticker != null) {
            ticker.shutdownNow();
            ticker = null;
        }
        if (stepWorkers != null) {
            stepWorkers.shutdownNow();
            stepWorkers = null;
        }
    }

    private void recoverAll() {
        for (String id : store.listInstanceIds()) {
            recoverInterrupted(id);
            driveAsync(id);
        }
    }

    private void tickAll() {
        try {
            for (String id : store.listInstanceIds()) {
                driveAsync(id);
            }
        } catch (RuntimeException e) {
            log.warn("tick sweep failed: {}", e.toString());
        }
    }

    public void wakeup(String instanceId) {
        driveAsync(instanceId);
    }

    private void driveAsync(String instanceId) {
        // Initiating callers always perform one deterministic inline pass;
        // repeated passes are safe because every transition is serialized by
        // the per-instance lock and only due attempts move state.
        drive(instanceId);
        ExecutorService pool = stepWorkers;
        if (pool != null && running) {
            pool.submit(() -> {
                try {
                    drive(instanceId);
                } catch (RuntimeException e) {
                    log.warn("[{}] drive failed: {}", instanceId, e.toString());
                }
            });
        }
    }

    /**
     * Crash recovery: any attempt left RUNNING by a dead JVM is INTERRUPTED
     * and retried (up to the same budget). WAITING async attempts simply keep
     * waiting for their callback or deadline.
     */
    public WorkflowInstance recoverInterrupted(String instanceId) {
        final boolean[] immediateRetry = {false};
        WorkflowInstance committed = store.mutate(instanceId, false, inst -> {
            if (inst == null || inst.isTerminal()) {
                return inst;
            }
            WorkflowDefinition def = defOf(inst);
            StepState step = inst.getCurrentStepIndex() < def.steps().size()
                    ? inst.stepAt(inst.getCurrentStepIndex()) : null;
            if (step != null && step.getStatus() == StepStatus.RUNNING) {
                AttemptRecord rec = lastAttempt(step);
                if (rec != null && "RUNNING".equals(rec.getOutcome())) {
                    rec.finish("INTERRUPTED", ErrorType.INTERRUPTED, "INTERRUPTED",
                            "attempt interrupted by engine restart", nowMs());
                }
                StepDefinition sd = def.steps().get(inst.getCurrentStepIndex());
                applyFailure(inst, step, sd, ErrorType.INTERRUPTED, "INTERRUPTED",
                        "attempt interrupted by engine restart");
                immediateRetry[0] = step.getStatus() == StepStatus.RETRY_WAIT
                        && nowMs() >= step.getNotBeforeMs();
                addEvent(inst, "RECOVERED_AFTER_RESTART", step.getStepName(), null,
                        "in-flight attempt marked interrupted");
            }
            inst.setUpdatedAtMs(nowMs());
            return inst;
        });
        if (immediateRetry[0]) {
            committed = drive(instanceId);
        }
        return committed;
    }

    /** Deterministic recovery + drive pass for tests. */
    public WorkflowInstance recover(String instanceId) {
        WorkflowInstance afterRecovery = recoverInterrupted(instanceId);
        if (afterRecovery != null && afterRecovery.getStatus() == InstanceStatus.RUNNING) {
            return drive(instanceId);
        }
        return afterRecovery;
    }
}
