package com.github.lisama569.durablefunctionruntimejava.engine.web;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.lisama569.durablefunctionruntimejava.engine.DurableWorkflowEngine;
import com.github.lisama569.durablefunctionruntimejava.demo.DemoWorkflows;
import com.github.lisama569.durablefunctionruntimejava.engine.model.CompletionRequest;
import com.github.lisama569.durablefunctionruntimejava.engine.model.StartRequest;
import com.github.lisama569.durablefunctionruntimejava.engine.model.WorkflowInstance;

/**
 * Local REST surface of the durable engine (no external services required):
 *
 * <ul>
 *   <li>POST /workflows/start          - start (or idempotently re-submit) an instance</li>
 *   <li>GET  /workflows/{instanceId}   - persisted status, steps, attempts, event trail</li>
 *   <li>GET  /workflows                - list instance ids</li>
 *   <li>POST /workflows/{id}/complete  - async step completion callback</li>
 *   <li>POST /workflows/{id}/terminate - request termination</li>
 * </ul>
 */
@RestController
@RequestMapping("/workflows")
public class WorkflowController {

    private final DurableWorkflowEngine engine;

    public WorkflowController(DurableWorkflowEngine engine) {
        this.engine = engine;
    }

    /** Demo helper: arrange N transient failures for a demo-order instance. */
    @PostMapping("/demo/simulate-failures/{instanceId}")
    public Map<String, Object> simulateFailures(@PathVariable String instanceId,
                                                @RequestParam(defaultValue = "2") int count) {
        DemoWorkflows.simulateFailures(instanceId, count);
        return Map.of("instanceId", instanceId, "simulatedFailures", count);
    }

    @PostMapping("/start")
    public WorkflowInstance start(@RequestBody StartRequest request) {
        return engine.start(request);
    }

    @GetMapping
    public Map<String, Object> list() {
        return Map.of("instanceIds", engine.listInstances());
    }

    @GetMapping("/{instanceId}")
    public WorkflowInstance get(@PathVariable String instanceId) {
        return engine.get(instanceId);
    }

    @PostMapping("/{instanceId}/complete")
    public WorkflowInstance complete(@PathVariable String instanceId,
                                     @RequestBody CompletionRequest request) {
        return engine.complete(request);
    }

    @PostMapping("/{instanceId}/terminate")
    public WorkflowInstance terminate(@PathVariable String instanceId,
                                      @RequestParam(defaultValue = "user requested") String reason) {
        return engine.terminate(instanceId, reason);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", e.getMessage() == null ? "bad request" : e.getMessage()));
    }
}
