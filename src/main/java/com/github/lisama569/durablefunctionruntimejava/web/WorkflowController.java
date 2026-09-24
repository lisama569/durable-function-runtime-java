package com.github.lisama569.durablefunctionruntimejava.web;

import com.github.lisama569.durablefunctionruntimejava.domain.WorkflowInstance;
import com.github.lisama569.durablefunctionruntimejava.engine.WorkflowEngine;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/workflows")
public class WorkflowController {

    private final WorkflowEngine engine;

    public WorkflowController(WorkflowEngine engine) {
        this.engine = engine;
    }

    public record StartRequest(String idempotencyKey, String input) {
    }

    /** 幂等启动：重复提交同 idempotencyKey 返回同一实例。 */
    @PostMapping("/{workflowName}/start")
    public ResponseEntity<WorkflowInstance> start(@PathVariable String workflowName,
                                                  @RequestBody StartRequest request) {
        if (request.idempotencyKey() == null || request.idempotencyKey().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        try {
            return ResponseEntity.ok(engine.start(workflowName, request.idempotencyKey(), request.input()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/instances/{instanceId}")
    public ResponseEntity<WorkflowInstance> get(@PathVariable String instanceId) {
        return engine.get(instanceId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/instances/{instanceId}/terminate")
    public ResponseEntity<Map<String, Boolean>> terminate(@PathVariable String instanceId) {
        boolean accepted = engine.terminate(instanceId);
        return accepted ? ResponseEntity.ok(Map.of("terminated", true))
                : ResponseEntity.notFound().build();
    }
}
