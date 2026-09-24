package com.github.lisama569.durablefunctionruntimejava.engine.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.durablefunctionruntimejava.engine.DurableWorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.engine.function.AsyncTask;
import com.github.lisama569.durablefunctionruntimejava.engine.model.CompletionRequest;
import com.github.lisama569.durablefunctionruntimejava.engine.model.InstanceStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StartRequest;
import com.github.lisama569.durablefunctionruntimejava.engine.model.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.engine.tests.support.ManualAsync;
import com.github.lisama569.durablefunctionruntimejava.engine.tests.support.SideEffectCounters;
import com.github.lisama569.durablefunctionruntimejava.engine.tests.support.TestHarness;

/**
 * Multiple instances of the same workflow run concurrently: state never
 * bleeds between instances, completion counters stay consistent under
 * duplicate notifications, and per-instance idempotent side effects fire
 * exactly once.
 */
class ConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger("DURABLE-TEST");

    @TempDir
    Path dir;

    @BeforeEach
    void reset() {
        SideEffectCounters.reset();
        TestHarness.FAIL_REMAINING.clear();
    }

    @Test
    void manySyncInstancesRunConcurrentlyWithoutStateBleed() throws Exception {
        TestHarness harness = new TestHarness();
        DurableWorkflowEngine engine = harness.engine(dir);

        int n = 24;
        // each instance's flaky step fails a distinct number of times
        for (int i = 0; i < n; i++) {
            String id = "wf-conc-" + i;
            TestHarness.FAIL_REMAINING.put("flaky-workflow:" + id, i % 3);
        }

        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                try {
                    gate.await();
                    StartRequest req = new StartRequest();
                    req.setWorkflowName("flaky-workflow");
                    req.setInstanceId("wf-conc-" + idx);
                    req.setInput(new ObjectMapper().readTree(
                            "{\"orderId\":\"order-" + idx + "\"}"));
                    return engine.start(req);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        gate.countDown();

        List<WorkflowInstance> results = new ArrayList<>();
        for (Future<?> f : futures) {
            results.add((WorkflowInstance) f.get(60, TimeUnit.SECONDS));
        }
        pool.shutdownNow();

        // Drive retries concurrently from many threads once backoffs elapse.
        ExecutorService drivers = Executors.newFixedThreadPool(8);
        for (int round = 0; round < 60; round++) {
            harness.advance(5);
            CountDownLatch dg = new CountDownLatch(n);
            for (int i = 0; i < n; i++) {
                final String id = "wf-conc-" + i;
                drivers.submit(() -> {
                    try {
                        engine.recover(id);
                    } finally {
                        dg.countDown();
                    }
                });
            }
            dg.await(30, TimeUnit.SECONDS);
            if (engine.listInstances().stream()
                    .map(engine::get)
                    .allMatch(w -> w.getStatus() != InstanceStatus.RUNNING)) {
                break;
            }
        }
        drivers.shutdownNow();

        for (int i = 0; i < n; i++) {
            WorkflowInstance inst = engine.get("wf-conc-" + i);
            assertThat(inst.getStatus()).isEqualTo(InstanceStatus.COMPLETED);
            int expectedAttempts = (i % 3) + 1;
            assertThat(inst.getSteps().get("flaky").getCurrentAttempt())
                    .as("instance " + i).isEqualTo(expectedAttempts);
            assertThat(inst.getOutput().path("charge").path("orderId").asText())
                    .as("no cross-instance input bleed for " + i)
                    .isEqualTo("order-" + i);
            assertThat(inst.getSteps().get("validate").getOutput().path("orderId").asText())
                    .isEqualTo("order-" + i);
        }

        // Exactly one side effect execution per instance.
        assertThat(SideEffectCounters.executions("charge")).isEqualTo(n);
        long events = results.stream().mapToLong(r -> r.getEvents().size()).sum();
        log.info("concurrent sync run: instances={} totalEvents={} chargeExecutions={}",
                n, events, SideEffectCounters.executions("charge"));
    }

    @Test
    void duplicateConcurrentAsyncCallbacksCountOncePerInstance() throws Exception {
        Path asyncDir = java.nio.file.Files.createTempDirectory("durable-aconc");
        TestHarness harness = new TestHarness();
        DurableWorkflowEngine engine = harness.engine(asyncDir);
        ManualAsync dispatcher =
                (ManualAsync) harness.registry().get("async-workflow").steps().get(0).dispatcher();

        int n = 12;
        for (int i = 0; i < n; i++) {
            StartRequest req = new StartRequest();
            req.setWorkflowName("async-workflow");
            req.setInstanceId("wf-aconc-" + i);
            req.setInput(new ObjectMapper().readTree("{\"orderId\":\"a-" + i + "\"}"));
            engine.start(req);
        }
        assertThat(dispatcher.dispatchCount).isEqualTo(n);

        Map<String, String> tokens = new ConcurrentHashMap<>();
        for (AsyncTask task : List.copyOf(dispatcher.dispatched)) {
            tokens.put(task.instanceId(), task.token());
        }

        int duplicatesPerCallback = 5;
        ExecutorService pool = Executors.newFixedThreadPool(10);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            for (int d = 0; d < duplicatesPerCallback; d++) {
                futures.add(pool.submit(() -> {
                    gate.await();
                    CompletionRequest c = new CompletionRequest();
                    c.setToken(tokens.get("wf-aconc-" + idx));
                    c.setSuccess(true);
                    c.setResult(new ObjectMapper().readTree(
                            "{\"approved\":true,\"orderId\":\"a-" + idx + "\"}"));
                    c.setDedupeKey("approval-" + idx);
                    engine.complete(c);
                    return null;
                }));
            }
        }
        gate.countDown();
        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        pool.shutdownNow();

        // Ensure deterministic advancement after the callback storm.
        for (int i = 0; i < n; i++) {
            engine.recover("wf-aconc-" + i);
        }

        for (int i = 0; i < n; i++) {
            WorkflowInstance inst = engine.get("wf-aconc-" + i);
            assertThat(inst.getStatus()).as("instance %d", i).isEqualTo(InstanceStatus.COMPLETED);
            // single attempt, single recorded completion despite 5 callbacks
            assertThat(inst.getSteps().get("awaitApproval").getCurrentAttempt()).isEqualTo(1);
            assertThat(inst.getSteps().get("awaitApproval").getAttempts()).hasSize(1);
            assertThat(inst.getOutput().path("orderId").asText()).isEqualTo("a-" + i);
        }
        // every instance but the first callback duplicates must be accounted in the trail
        WorkflowInstance sample = engine.get("wf-aconc-0");
        long ignored = sample.getEvents().stream()
                .filter(e -> e.getType().contains("DEDUP") || e.getType().contains("IGNORED"))
                .count();
        log.info("sample instance duplicate callback events: {}", ignored);
        assertThat(ignored).isGreaterThanOrEqualTo(1);
    }
}
