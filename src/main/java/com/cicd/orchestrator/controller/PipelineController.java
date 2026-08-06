package com.cicd.orchestrator.controller;

import com.cicd.orchestrator.model.PipelineState;
import com.cicd.orchestrator.service.PipelineExecutionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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

    @GetMapping("/{pipelineId}")
    public ResponseEntity<PipelineState> getStatus(@PathVariable String pipelineId) {
        PipelineState state = executionService.getPipelineStatus(pipelineId);
        if (state == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(state);
    }
}
