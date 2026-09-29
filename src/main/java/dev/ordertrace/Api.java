package dev.ordertrace;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;

@RestController
@RequestMapping("/api")
public class Api {
    private final ReadModel model;
    private final ObjectProvider<Pipeline> pipeline;
    public Api(ReadModel model, ObjectProvider<Pipeline> pipeline) { this.model = model; this.pipeline = pipeline; }
    private void page(int limit, int offset) {
        if (limit < 1 || limit > 100 || offset < 0 || offset > 10000) throw new IllegalArgumentException("limit: 1..100; offset: 0..10000");
    }
    @GetMapping("/parents") public Map<String,Object> parents(@RequestParam(required=false) String rootOrderId,
            @RequestParam(required=false) String status, @RequestParam(defaultValue="20") int limit, @RequestParam(defaultValue="0") int offset) {
        page(limit, offset);
        if (rootOrderId != null) OrderEvent.id(rootOrderId, 80);
        if (status != null && !Set.of("ACTIVE", "UNKNOWN", "FILLED", "CANCELED", "REJECTED", "COMPLETED").contains(status))
            throw new IllegalArgumentException("Invalid parent status");
        return Map.of("items", model.parents(rootOrderId, status, limit, offset), "limit", limit, "offset", offset);
    }
    @GetMapping("/parents/{root}") public Map<String,Object> parent(@PathVariable String root) {
        OrderEvent.id(root, 80);
        return model.parent(root).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Parent not found"));
    }
    @GetMapping("/parents/{root}/children") public Map<String,Object> children(@PathVariable String root,
            @RequestParam(defaultValue="100") int limit, @RequestParam(defaultValue="0") int offset) {
        page(limit, offset); parent(root);
        return Map.of("items", model.children(root, limit, offset), "limit", limit, "offset", offset);
    }
    @GetMapping("/parents/{root}/events") public Map<String,Object> events(@PathVariable String root,
            @RequestParam(required=false) String childOrderId, @RequestParam(defaultValue="50") int limit, @RequestParam(defaultValue="0") int offset) {
        page(limit, offset); parent(root);
        if (childOrderId != null) OrderEvent.id(childOrderId, 80);
        return Map.of("items", model.events(root, childOrderId, limit, offset), "limit", limit, "offset", offset);
    }
    @GetMapping("/health") public Map<String,Object> health() {
        model.parents(null, null, 1, 0);
        Pipeline active = pipeline.getIfAvailable();
        Map<String,String> state = active == null ? Map.of("streams", "DISABLED", "writer", "DISABLED") : active.status();
        if (active != null && (!"RUNNING".equals(state.get("streams")) || !"RUNNING".equals(state.get("writer"))))
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Pipeline not ready: " + state);
        return Map.of("database", "UP", "pipeline", state);
    }
}

@RestControllerAdvice
class ApiErrors {
    @ExceptionHandler({IllegalArgumentException.class, MethodArgumentTypeMismatchException.class})
    ResponseEntity<ProblemDetail> badRequest(Exception exception) {
        return ResponseEntity.badRequest().body(ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                exception instanceof MethodArgumentTypeMismatchException ? "Invalid parameter type" : exception.getMessage()));
    }
    @ExceptionHandler(ResponseStatusException.class) ResponseEntity<ProblemDetail> status(ResponseStatusException exception) {
        return ResponseEntity.status(exception.getStatusCode()).body(ProblemDetail.forStatusAndDetail(exception.getStatusCode(), exception.getReason()));
    }
    @ExceptionHandler(org.springframework.dao.DataAccessException.class) ResponseEntity<ProblemDetail> unavailable() {
        return ResponseEntity.status(503).body(ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, "Database temporarily unavailable"));
    }
}
