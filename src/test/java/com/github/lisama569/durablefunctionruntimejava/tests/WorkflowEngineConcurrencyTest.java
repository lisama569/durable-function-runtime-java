package com.github.lisama569.durablefunctionruntimejava.tests;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowStatus;
import com.github.lisama569.durablefunctionruntimejava.engine.FileWorkflowStore;
import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.workflow.StepDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.WorkflowDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 重复触发与并发执行：副作用幂等、实例间状态不串扰、计数不错乱。 */
class WorkflowEngineConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngineConcurrencyTest.class);
    private static final RetryPolicy FAST = new RetryPolicy(3, 20, 100, 5_000);

    @TempDir
    Path dataDir;

    private WorkflowEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) engine.shutdown();
    }

    @Test
    void duplicateStartWithSameIdempotencyKeyProducesSingleInstanceAndSingleSideEffect() throws Exception {
        AtomicInteger sideEffects = new AtomicInteger();
        engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        engine.register(new WorkflowDefinition("pay", List.of(
                new StepDefinition("charge", (ctx, input) -> {
                    int n = sideEffects.incrementAndGet();
                    log.info("[instance={}] step=charge idemKey={} sideEffect#{}", ctx.instanceId(), ctx.idempotencyKey(), n);
                    return "charged";
                }, FAST))));

        int submitters = 16;
        ExecutorService pool = Executors.newFixedThreadPool(submitters);
        CountDownLatch ready = new CountDownLatch(submitters);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<WorkflowInstance>> futures = new ArrayList<>();
        for (int i = 0; i < submitters; i++) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                return engine.start("pay", "same-key", "in");
            }));
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        go.countDown();

        Set<String> instanceIds = ConcurrentHashMap.newKeySet();
        for (Future<WorkflowInstance> f : futures) {
            instanceIds.add(f.get(10, TimeUnit.SECONDS).getId());
        }
        pool.shutdown();

        assertEquals(1, instanceIds.size(), "同幂等键并发提交必须收敛到同一实例");
        String id = instanceIds.iterator().next();
        WorkflowInstance terminal = engine.awaitTerminal(id, 10_000);
        assertNotNull(terminal);
        assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
        assertEquals(1, sideEffects.get(), "重复触发不得产生重复副作用");

        // 重复提交已完结实例：仍然返回同一实例，不重新执行。
        WorkflowInstance again = engine.start("pay", "same-key", "in");
        assertEquals(id, again.getId());
        assertEquals(1, sideEffects.get());
    }

    @Test
    void manyConcurrentInstancesRunIsolatedWithCorrectCounts() throws Exception {
        int instanceCount = 20;
        Map<String, AtomicInteger> perInstanceStep1 = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> perInstanceStep2 = new ConcurrentHashMap<>();
        AtomicInteger totalStep1 = new AtomicInteger();
        AtomicInteger totalStep2 = new AtomicInteger();

        engine = new WorkflowEngine(new FileWorkflowStore(dataDir));
        engine.register(new WorkflowDefinition("isolated", List.of(
                new StepDefinition("step1", (ctx, input) -> {
                    perInstanceStep1.computeIfAbsent(ctx.instanceId(), k -> new AtomicInteger()).incrementAndGet();
                    totalStep1.incrementAndGet();
                    return input + "-s1";
                }, FAST),
                new StepDefinition("step2", (ctx, input) -> {
                    perInstanceStep2.computeIfAbsent(ctx.instanceId(), k -> new AtomicInteger()).incrementAndGet();
                    totalStep2.incrementAndGet();
                    log.info("[instance={}] step=step2 input={}", ctx.instanceId(), input);
                    return input + "-s2";
                }, FAST))));

        List<WorkflowInstance> instances = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++) {
            instances.add(engine.start("isolated", "key-" + i, "input-" + i));
        }

        for (WorkflowInstance instance : instances) {
            WorkflowInstance terminal = engine.awaitTerminal(instance.getId(), 15_000);
            assertNotNull(terminal, "instance " + instance.getId() + " 应进入终态");
            assertEquals(WorkflowStatus.COMPLETED, terminal.getStatus());
            // 步骤间数据传递不串扰：step2 的输入必须来自本实例 step1 的输出。
            assertEquals("input-" + instances.indexOf(instance) + "-s1",
                    terminal.getSteps().get(1).getInput());
        }

        assertEquals(instanceCount, totalStep1.get(), "每个实例的 step1 恰好执行一次");
        assertEquals(instanceCount, totalStep2.get(), "每个实例的 step2 恰好执行一次");
        for (WorkflowInstance instance : instances) {
            assertEquals(1, perInstanceStep1.get(instance.getId()).get());
            assertEquals(1, perInstanceStep2.get(instance.getId()).get());
        }
    }
}
