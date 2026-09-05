package com.cicd.orchestrator.service;

import com.cicd.orchestrator.engine.DagExecutor;
import com.cicd.orchestrator.engine.TaskNode;
import com.cicd.orchestrator.model.PipelineState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PipelineExecutionService {
    private static final Logger log = LoggerFactory.getLogger(PipelineExecutionService.class);

    private final DagExecutor dagExecutor;
    private final List<TaskNode> pipelineDag;
    private final KafkaTemplate<String, String> kafkaTemplate;
    
    // In-memory store for running pipelines (use Redis in production)
    private final Map<String, PipelineState> activePipelines = new ConcurrentHashMap<>();

    public PipelineExecutionService(DagExecutor dagExecutor, List<TaskNode> pipelineDag, KafkaTemplate<String, String> kafkaTemplate) {
        this.dagExecutor = dagExecutor;
        this.pipelineDag = pipelineDag;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Async
    public void executePipeline(PipelineState initialState) {
        String id = initialState.getPipelineId();
        activePipelines.put(id, initialState);
        log.info("🚀 Starting Distributed DAG pipeline execution for ID: {}", id);

        try {
            Map<String, Object> executionContext = new HashMap<>();
            executionContext.put("repo", initialState.getRepoName());
            executionContext.put("commit", initialState.getCommitSha());
            
            // Execute the graph concurrently using Virtual Threads
            boolean success = dagExecutor.execute(pipelineDag, executionContext);
            
            if (success) {
                initialState.status(PipelineState.PipelineStatus.COMPLETED);
                initialState.setCompletedAt(java.time.Instant.now());
                activePipelines.put(id, initialState);
                log.info("✅ Pipeline completed successfully: {}", id);
                kafkaTemplate.send("pipeline-results", id, "COMPLETED");
            } else {
                log.error("❌ Pipeline execution failed for ID: {}", id);
                initialState.status(PipelineState.PipelineStatus.FAILED);
                initialState.setCompletedAt(java.time.Instant.now());
                activePipelines.put(id, initialState);
                kafkaTemplate.send("pipeline-results", id, "FAILED");
            }
            
        } catch (Exception e) {
            log.error("❌ Pipeline execution crashed for ID: {}", id, e);
            initialState.status(PipelineState.PipelineStatus.FAILED);
            initialState.addError(e.getMessage());
            initialState.setCompletedAt(java.time.Instant.now());
            activePipelines.put(id, initialState);
            
            kafkaTemplate.send("pipeline-results", id, "FAILED");
        }
    }

    public PipelineState getPipelineStatus(String pipelineId) {
        return activePipelines.get(pipelineId);
    }

    public boolean cancelPipeline(String pipelineId) {
        PipelineState state = activePipelines.get(pipelineId);
        if (state != null) {
            state.status(PipelineState.PipelineStatus.FAILED);
            state.addError("Pipeline cancelled by user");
            state.setCompletedAt(java.time.Instant.now());
            kafkaTemplate.send("pipeline-results", pipelineId, "CANCELLED");
            return true;
        }
        return false;
    }
}
