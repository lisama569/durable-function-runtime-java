package com.github.lisama569.durablefunctionruntimejava.web;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.engine.FileWorkflowStore;
import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.workflow.StepDefinition;
import com.github.lisama569.durablefunctionruntimejava.workflow.WorkflowDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.List;

@Configuration
public class EngineConfig {

    private static final Logger log = LoggerFactory.getLogger(EngineConfig.class);

    @Bean(destroyMethod = "shutdown")
    public WorkflowEngine workflowEngine(@Value("${workflow.data-dir:./workflow-data}") String dataDir) {
        FileWorkflowStore store = new FileWorkflowStore(Path.of(dataDir));
        WorkflowEngine engine = new WorkflowEngine(store);

        // 示例工作流：校验 -> 扣款 -> 发货，演示顺序编排与重试。
        engine.register(new WorkflowDefinition("orderWorkflow", List.of(
                new StepDefinition("validate", (ctx, input) -> {
                    log.info("[instance={}] step=validate idemKey={} input={}", ctx.instanceId(), ctx.idempotencyKey(), input);
                    return "validated:" + input;
                }, RetryPolicy.defaults()),
                new StepDefinition("charge", (ctx, input) -> {
                    log.info("[instance={}] step=charge idemKey={} input={}", ctx.instanceId(), ctx.idempotencyKey(), input);
                    return "charged:" + input;
                }, new RetryPolicy(3, 200, 2_000, 10_000)),
                new StepDefinition("ship", (ctx, input) -> {
                    log.info("[instance={}] step=ship idemKey={} input={}", ctx.instanceId(), ctx.idempotencyKey(), input);
                    return "shipped:" + input;
                }, RetryPolicy.defaults())
        )));

        int recovered = engine.recover();
        log.info("engine started, recovered {} running instance(s) from {}", recovered, dataDir);
        return engine;
    }
}
