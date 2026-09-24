package com.github.lisama569.durablefunctionruntimejava.engine;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.lisama569.durablefunctionruntimejava.engine.persistence.FileStateStore;
import com.github.lisama569.durablefunctionruntimejava.engine.persistence.StateStore;

import jakarta.annotation.PreDestroy;

/**
 * Wires the local, single-node durable engine:
 * file-backed store + engine singleton + optional background recovery/ticker.
 */
@Configuration
@EnableConfigurationProperties(EngineProperties.class)
public class EngineConfig {

    @Bean
    public StateStore stateStore(EngineProperties properties) {
        return new FileStateStore(properties);
    }

    @Bean
    public DurableWorkflowEngine durableWorkflowEngine(StateStore store, WorkflowRegistry registry,
                                                      EngineProperties properties) {
        DurableWorkflowEngine engine = new DurableWorkflowEngine(store, registry, properties);
        if (properties.isAutoStart()) {
            engine.startBackground();
        }
        return engine;
    }

    @Bean
    public EngineLifecycle engineLifecycle(DurableWorkflowEngine engine) {
        return new EngineLifecycle(engine);
    }

    static class EngineLifecycle {
        private final DurableWorkflowEngine engine;

        EngineLifecycle(DurableWorkflowEngine engine) {
            this.engine = engine;
        }

        @PreDestroy
        void stop() {
            engine.stopBackground();
        }
    }
}
