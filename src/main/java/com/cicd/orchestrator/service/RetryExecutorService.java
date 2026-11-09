package com.cicd.orchestrator.service;

import com.cicd.orchestrator.engine.Task;
import com.cicd.orchestrator.model.RetryAttempt;
import com.cicd.orchestrator.model.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service that executes tasks with configurable retry policies.
 * Implements exponential backoff with jitter and maintains
 * a per-pipeline retry history for observability.
 */
@Service
public class RetryExecutorService {
    private static final Logger log = LoggerFactory.getLogger(RetryExecutorService.class);

    private final NotificationService notificationService;

    // Per-pipeline retry history
    private final Map<String, List<RetryAttempt>> retryHistory = new ConcurrentHashMap<>();

    public RetryExecutorService(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    /**
     * Execute a task with the given retry policy.
     * Returns true if the task eventually succeeds, false if all retries are exhausted.
     */
    public boolean executeWithRetry(
            String pipelineId, String repo, String taskId,
            Task task, Map<String, Object> context, RetryPolicy policy) {

        List<RetryAttempt> attempts = retryHistory.computeIfAbsent(pipelineId, k -> new ArrayList<>());

        for (int attempt = 0; attempt <= policy.getMaxRetries(); attempt++) {
            RetryAttempt record = new RetryAttempt(attempt, taskId, pipelineId);

            if (attempt > 0) {
                long delay = policy.calculateDelay(attempt - 1);
                record.setDelayMs(delay);
                log.info("⏱️ Retry {}/{} for task [{}] in pipeline [{}] after {}ms delay",
                        attempt, policy.getMaxRetries(), taskId, pipelineId, delay);

                notificationService.notifyRetryAttempt(
                        pipelineId, repo, taskId, attempt, policy.getMaxRetries(),
                        getLastError(attempts));

                sleep(delay);
            }

            try {
                boolean success = task.execute(context);
                record.complete(success, success ? null : "Task returned false");
                attempts.add(record);

                if (success) {
                    if (attempt > 0) {
                        log.info("✅ Task [{}] succeeded on retry attempt {} for pipeline [{}]",
                                taskId, attempt, pipelineId);
                    }
                    return true;
                }

                log.warn("❌ Task [{}] failed (attempt {}/{}) for pipeline [{}]",
                        taskId, attempt, policy.getMaxRetries(), pipelineId);

            } catch (Exception e) {
                record.complete(false, e.getMessage());
                attempts.add(record);

                if (!policy.isRetryable(e)) {
                    log.error("🚫 Non-retryable exception for task [{}]: {}", taskId, e.getMessage());
                    return false;
                }

                log.warn("⚠️ Task [{}] threw exception (attempt {}/{}): {}",
                        taskId, attempt, policy.getMaxRetries(), e.getMessage());
            }
        }

        log.error("💀 All {} retries exhausted for task [{}] in pipeline [{}]",
                policy.getMaxRetries(), taskId, pipelineId);
        return false;
    }

    /**
     * Get retry history for a specific pipeline.
     */
    public List<RetryAttempt> getRetryHistory(String pipelineId) {
        return retryHistory.getOrDefault(pipelineId, List.of());
    }

    /**
     * Get retry history for a specific task within a pipeline.
     */
    public List<RetryAttempt> getTaskRetryHistory(String pipelineId, String taskId) {
        return retryHistory.getOrDefault(pipelineId, List.of()).stream()
                .filter(a -> taskId.equals(a.getTaskId()))
                .toList();
    }

    /**
     * Clear retry history for a pipeline (e.g., after pipeline completes).
     */
    public void clearHistory(String pipelineId) {
        retryHistory.remove(pipelineId);
    }

    private String getLastError(List<RetryAttempt> attempts) {
        if (attempts.isEmpty()) return "Unknown";
        RetryAttempt last = attempts.get(attempts.size() - 1);
        return last.getErrorMessage() != null ? last.getErrorMessage() : "Unknown";
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
