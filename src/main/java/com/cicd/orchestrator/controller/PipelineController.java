package com.cicd.orchestrator.controller;

import com.cicd.orchestrator.model.PipelineState;
import com.cicd.orchestrator.service.PipelineExecutionService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/pipelines")
public class PipelineController {

    private final PipelineExecutionService executionService;

    public PipelineController(PipelineExecutionService executionService) {
        this.executionService = executionService;
    }

    @PostMapping("/trigger")
    public ResponseEntity<Map<String, String>> triggerPipeline(@RequestBody Map<String, String> request) {
        String repo = request.getOrDefault("repo", "unknown/repo");
        String branch = request.getOrDefault("branch", "main");
        String commitSha = request.getOrDefault("commitSha", "unknown-sha");
        
        String pipelineId = UUID.randomUUID().toString();
        
        PipelineState state = new PipelineState(pipelineId, repo, branch, commitSha);
        
        // Start asynchronously
        executionService.executePipeline(state);
        
        return ResponseEntity.accepted().body(Map.of(
                "status", "QUEUED",
                "pipelineId", pipelineId,
                "message", "Pipeline triggered successfully."
        ));
    }

    @GetMapping
    public ResponseEntity<List<PipelineState>> getAllPipelines() {
        return ResponseEntity.ok(executionService.getAllPipelines());
    }

    @GetMapping("/{pipelineId}")
    public ResponseEntity<PipelineState> getStatus(@PathVariable String pipelineId) {
        PipelineState state = executionService.getPipelineStatus(pipelineId);
        if (state == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(state);
    }

    @GetMapping("/{pipelineId}/report")
    public ResponseEntity<Map<String, Object>> getReport(@PathVariable String pipelineId) {
        Map<String, Object> report = executionService.getPipelineReport(pipelineId);
        if (report == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(report);
    }

    @GetMapping(value = "/{pipelineId}/pr-comment", produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> getPrComment(@PathVariable String pipelineId) {
        PipelineState state = executionService.getPipelineStatus(pipelineId);
        if (state == null) {
            return ResponseEntity.notFound().build();
        }
        String prComment = state.getPrCommentBody();
        if (prComment == null || prComment.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(prComment);
    }

    @DeleteMapping("/{pipelineId}")
    public ResponseEntity<Map<String, String>> cancelPipeline(@PathVariable String pipelineId) {
        boolean cancelled = executionService.cancelPipeline(pipelineId);
        if (!cancelled) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(Map.of(
                "status", "CANCELLED",
                "pipelineId", pipelineId,
                "message", "Pipeline cancelled successfully."
        ));
    }
}
