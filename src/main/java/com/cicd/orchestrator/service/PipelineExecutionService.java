package com.cicd.orchestrator.service;

import com.cicd.orchestrator.engine.DagExecutor;
import com.cicd.orchestrator.engine.TaskNode;
import com.cicd.orchestrator.model.PipelineState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
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
    private final PipelineMetricsService metricsService;
    private final PipelineFeedbackProducer feedbackProducer;
    
    // In-memory store for running pipelines (use Redis in production)
    private final Map<String, PipelineState> activePipelines = new ConcurrentHashMap<>();

    public PipelineExecutionService(
            DagExecutor dagExecutor,
            List<TaskNode> pipelineDag,
            KafkaTemplate<String, String> kafkaTemplate,
            PipelineMetricsService metricsService,
            PipelineFeedbackProducer feedbackProducer
    ) {
        this.dagExecutor = dagExecutor;
        this.pipelineDag = pipelineDag;
        this.kafkaTemplate = kafkaTemplate;
        this.metricsService = metricsService;
        this.feedbackProducer = feedbackProducer;
    }

    @Async
    public void executePipeline(PipelineState initialState) {
        String id = initialState.getPipelineId();
        activePipelines.put(id, initialState);
        log.info("🚀 Starting Distributed DAG pipeline execution for ID: {}", id);

        feedbackProducer.sendPipelineStarted(id, initialState.getRepo(), initialState.getCommitSha(), initialState.getBranch());

        long startNanos = System.currentTimeMillis();

        try {
            Map<String, Object> executionContext = new HashMap<>();
            executionContext.put("repo", initialState.getRepo());
            executionContext.put("commit", initialState.getCommitSha());
            
            // Execute the graph concurrently using Virtual Threads
            boolean success = dagExecutor.execute(pipelineDag, executionContext);
            long durationMs = System.currentTimeMillis() - startNanos;

            // Ingest results produced by the tasks into the persistent pipeline state
            @SuppressWarnings("unchecked")
            Map<String, Object> commitAnalysis = (Map<String, Object>) executionContext.get("commitAnalysis");
            if (commitAnalysis != null) {
                initialState.setCommitAnalysis(commitAnalysis);
                String risk = (String) commitAnalysis.getOrDefault("riskScore", "UNKNOWN");
                metricsService.recordAgentAnalysis(initialState.getRepo(), risk, durationMs);

                int findingsCount = commitAnalysis.containsKey("findings") ? ((List<?>) commitAnalysis.get("findings")).size() : 0;
                feedbackProducer.sendSecurityAuditEvent(id, initialState.getRepo(), risk, findingsCount);
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> dependencyReport = (Map<String, Object>) executionContext.get("dependencyReport");
            if (dependencyReport != null) {
                initialState.setDependencyReport(dependencyReport);
                int vulns = (Integer) dependencyReport.getOrDefault("vulnerabilitiesFound", 0);
                boolean valid = (Boolean) dependencyReport.getOrDefault("valid", true);
                metricsService.recordVulnerabilities(initialState.getRepo(), vulns);
                feedbackProducer.sendDependencyValidationEvent(id, initialState.getRepo(), valid, vulns);
            }

            String prComment = (String) executionContext.get("prCommentBody");
            if (prComment != null) {
                initialState.setPrCommentBody(prComment);
            }

            metricsService.recordPipelineCompleted(
                    initialState.getRepo(),
                    initialState.getEnvironment(),
                    success,
                    durationMs
            );

            feedbackProducer.sendPipelineFinished(id, initialState.getRepo(), success, durationMs);
            
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
            long durationMs = System.currentTimeMillis() - startNanos;
            metricsService.recordPipelineCompleted(
                    initialState.getRepo(),
                    initialState.getEnvironment(),
                    false,
                    durationMs
            );

            feedbackProducer.sendPipelineFinished(id, initialState.getRepo(), false, durationMs);

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

    public List<PipelineState> getAllPipelines() {
        return new ArrayList<>(activePipelines.values());
    }

    public Map<String, Object> getPipelineReport(String pipelineId) {
        PipelineState state = activePipelines.get(pipelineId);
        if (state == null) {
            return null;
        }
        Map<String, Object> report = new HashMap<>();
        report.put("pipelineId", state.getPipelineId());
        report.put("repo", state.getRepo());
        report.put("branch", state.getBranch());
        report.put("commitSha", state.getCommitSha());
        report.put("status", state.getStatus());
        report.put("environment", state.getEnvironment());
        report.put("startedAt", state.getStartedAt());
        report.put("completedAt", state.getCompletedAt());
        report.put("commitAnalysis", state.getCommitAnalysis());
        report.put("dependencyReport", state.getDependencyReport());
        report.put("errors", state.getErrors());
        report.put("hasPrComment", state.getPrCommentBody() != null && !state.getPrCommentBody().isEmpty());
        return report;
    }

    public boolean cancelPipeline(String pipelineId) {
        PipelineState state = activePipelines.get(pipelineId);
        if (state != null) {
            state.status(PipelineState.PipelineStatus.FAILED);
            state.addError("Pipeline cancelled by user");
            state.setCompletedAt(java.time.Instant.now());
            kafkaTemplate.send("pipeline-results", pipelineId, "CANCELLED");
            feedbackProducer.sendPipelineFinished(pipelineId, state.getRepo(), false, 0);
            return true;
        }
        return false;
    }
}
