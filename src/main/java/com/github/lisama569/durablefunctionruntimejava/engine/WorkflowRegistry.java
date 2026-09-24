package com.github.lisama569.durablefunctionruntimejava.engine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import com.github.lisama569.durablefunctionruntimejava.engine.function.WorkflowDefinition;

/**
 * In-process registry of workflow definitions.
 *
 * <p>Definitions are code, not state: after a restart the application must
 * re-register the same workflow names before instances can be driven. The
 * registry itself is therefore intentionally not persisted.
 */
@Component
public class WorkflowRegistry {

    private final ConcurrentHashMap<String, WorkflowDefinition> definitions = new ConcurrentHashMap<>();

    /** Register (or replace, e.g. on hot reload) a workflow definition. */
    public void register(WorkflowDefinition definition) {
        if (definition == null || definition.name() == null) {
            throw new IllegalArgumentException("workflow definition and its name are required");
        }
        definitions.put(definition.name(), definition);
    }

    public WorkflowDefinition get(String name) {
        return definitions.get(name);
    }

    public Map<String, WorkflowDefinition> all() {
        return Map.copyOf(definitions);
    }
}
