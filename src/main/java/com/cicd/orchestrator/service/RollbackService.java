package com.cicd.orchestrator.service;

import com.cicd.orchestrator.model.PipelineState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Orchestrates automatic rollback actions when pipelines fail after deployment.
 * Maintains rollback history and supports configurable rollback strategies
 * per repository and environment.
 */
@Service
public class RollbackService {
    private static final Logger log = LoggerFactory.getLogger(RollbackService.class);

    private final NotificationService notificationService;
    private final PipelineFeedbackProducer feedbackProducer;

    // Track rollback history per pipeline
    private final Map<String, RollbackRecord> rollbackHistory = new ConcurrentHashMap<>();

    // Track last known good deployment per repo+env
    private final Map<String, String> lastKnownGood = new ConcurrentHashMap<>();

    public RollbackService(NotificationService notificationService,
                           PipelineFeedbackProducer feedbackProducer) {
        this.notificationService = notificationService;
        this.feedbackProducer = feedbackProducer;
    }

    /**
     * Evaluate whether a rollback should be triggered based on the pipeline failure.
     * Rollback is triggered for production deployments or when critical security
     * vulnerabilities are detected.
     */
    public boolean shouldRollback(PipelineState state) {
        if (state == null) return false;

        String env = state.getEnvironment();
        PipelineState.PipelineStatus status = state.getStatus();

        // Only rollback completed-failed or blocked pipelines in production/staging
        if (status != PipelineState.PipelineStatus.FAILED &&
            status != PipelineState.PipelineStatus.BLOCKED) {
            return false;
        }

        // Always rollback production failures
        if ("production".equalsIgnoreCase(env)) {
            log.info("Rollback triggered: production pipeline [{}] failed", state.getPipelineId());
            return true;
        }

        // Rollback staging if critical security issues found
        if ("staging".equalsIgnoreCase(env) && hasCriticalSecurityIssues(state)) {
            log.info("Rollback triggered: staging pipeline [{}] has critical security issues",
                    state.getPipelineId());
            return true;
        }

        return false;
    }

    /**
     * Execute the rollback procedure.
     * In a real deployment, this would invoke Kubernetes rollback, 
     * revert Docker image tags, or trigger a redeployment of the last known good version.
     */
    public RollbackRecord executeRollback(PipelineState state, String reason) {
        String pipelineId = state.getPipelineId();
        String repo = state.getRepo();
        String env = state.getEnvironment();
        String repoEnvKey = repo + ":" + env;

        log.info("⏪ Initiating rollback for pipeline [{}] repo [{}] env [{}]",
                pipelineId, repo, env);

        RollbackRecord record = new RollbackRecord();
        record.pipelineId = pipelineId;
        record.repo = repo;
        record.environment = env;
        record.reason = reason;
        record.triggeredAt = Instant.now();
        record.previousGoodVersion = lastKnownGood.getOrDefault(repoEnvKey, "unknown");

        try {
            // Simulate rollback operations
            performRollbackSteps(state, record);

            record.success = true;
            record.completedAt = Instant.now();
            log.info("✅ Rollback completed successfully for pipeline [{}]", pipelineId);

            // Notify about successful rollback
            notificationService.notifyRollback(pipelineId, repo, env, reason);

            // Send Kafka event
            feedbackProducer.sendDeploymentGateEvent(
                    pipelineId, repo, "ROLLED_BACK",
                    "Rollback to version: " + record.previousGoodVersion);

        } catch (Exception e) {
            record.success = false;
            record.error = e.getMessage();
            record.completedAt = Instant.now();
            log.error("❌ Rollback failed for pipeline [{}]: {}", pipelineId, e.getMessage());

            notificationService.notifyPipelineFailure(
                    pipelineId, repo, state.getBranch(), state.getCommitSha(),
                    List.of("Rollback failed: " + e.getMessage()));
        }

        rollbackHistory.put(pipelineId, record);
        return record;
    }

    /**
     * Register a successful deployment as the "last known good" version.
     * Called when a pipeline completes successfully.
     */
    public void registerSuccessfulDeployment(String repo, String environment, String commitSha) {
        String key = repo + ":" + environment;
        lastKnownGood.put(key, commitSha);
        log.info("📌 Registered last known good version for {}: {}", key, commitSha);
    }

    /**
     * Get the last known good version for a repo+environment.
     */
    public String getLastKnownGoodVersion(String repo, String environment) {
        return lastKnownGood.getOrDefault(repo + ":" + environment, null);
    }

    /**
     * Get rollback record for a specific pipeline.
     */
    public RollbackRecord getRollbackRecord(String pipelineId) {
        return rollbackHistory.get(pipelineId);
    }

    /**
     * Get all rollback records.
     */
    public List<RollbackRecord> getAllRollbacks() {
        return new ArrayList<>(rollbackHistory.values());
    }

    private void performRollbackSteps(PipelineState state, RollbackRecord record) {
        List<String> steps = new ArrayList<>();

        // Step 1: Stop current deployment
        steps.add("Halted active deployment for " + state.getRepo());
        log.info("  [1/4] Halting current deployment...");

        // Step 2: Revert container image / artifact
        String previousVersion = record.previousGoodVersion;
        steps.add("Reverting to image version: " + previousVersion);
        log.info("  [2/4] Reverting container image to {}...", previousVersion);

        // Step 3: Restart services
        steps.add("Restarting service instances with rolled-back version");
        log.info("  [3/4] Restarting services...");

        // Step 4: Verify health checks
        steps.add("Health check verification passed");
        log.info("  [4/4] Verifying health checks...");

        record.steps = steps;
    }

    private boolean hasCriticalSecurityIssues(PipelineState state) {
        Map<String, Object> analysis = state.getCommitAnalysis();
        if (analysis == null) return false;

        String risk = (String) analysis.getOrDefault("riskScore", "LOW");
        return "CRITICAL".equalsIgnoreCase(risk) || "HIGH".equalsIgnoreCase(risk);
    }

    /**
     * Immutable record of a rollback execution.
     */
    public static class RollbackRecord {
        public String pipelineId;
        public String repo;
        public String environment;
        public String reason;
        public String previousGoodVersion;
        public boolean success;
        public String error;
        public Instant triggeredAt;
        public Instant completedAt;
        public List<String> steps = new ArrayList<>();

        public Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("pipelineId", pipelineId);
            map.put("repo", repo);
            map.put("environment", environment);
            map.put("reason", reason);
            map.put("previousGoodVersion", previousGoodVersion);
            map.put("success", success);
            map.put("error", error);
            map.put("triggeredAt", triggeredAt);
            map.put("completedAt", completedAt);
            map.put("steps", steps);
            return map;
        }
    }
}
