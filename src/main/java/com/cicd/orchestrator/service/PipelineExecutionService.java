package com.cicd.orchestrator.service;

import com.cicd.orchestrator.model.PipelineState;
import org.bsc.langgraph4j.StateGraph;
import org.bsc.langgraph4j.CompiledGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PipelineExecutionService {
    private static final Logger log = LoggerFactory.getLogger(PipelineExecutionService.class);

    private final StateGraph<PipelineState> pipelineGraph;
    private final KafkaTemplate<String, String> kafkaTemplate;
    
    // In-memory store for running pipelines (use Redis/DB in production)
    private final Map<String, PipelineState> activePipelines = new ConcurrentHashMap<>();

    public PipelineExecutionService(StateGraph<PipelineState> pipelineGraph, KafkaTemplate<String, String> kafkaTemplate) {
        this.pipelineGraph = pipelineGraph;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Async
    public void executePipeline(PipelineState initialState) {
        String id = initialState.getPipelineId();
        activePipelines.put(id, initialState);
        log.info("🚀 Starting pipeline execution for ID: {}", id);

        try {
            CompiledGraph<PipelineState> compiled = pipelineGraph.compile();
            
            // Execute the graph
            var result = compiled.invoke(initialState);
            
            // The result contains the final state
            if (result.isPresent()) {
                PipelineState finalState = result.get();
                finalState.status(PipelineState.PipelineStatus.COMPLETED);
                finalState.setCompletedAt(java.time.Instant.now());
                activePipelines.put(id, finalState);
                log.info("✅ Pipeline completed successfully: {}", id);
                
                // Publish completion event
                // In a real app, serialize finalState to JSON
                kafkaTemplate.send("pipeline-results", id, "COMPLETED");
            } else {
                log.warn("⚠️ Pipeline finished without a final state: {}", id);
            }
            
        } catch (Exception e) {
            log.error("❌ Pipeline execution failed for ID: {}", id, e);
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
}
