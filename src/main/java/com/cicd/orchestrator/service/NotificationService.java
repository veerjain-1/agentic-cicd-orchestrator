package com.cicd.orchestrator.service;

import com.cicd.orchestrator.model.NotificationEvent;
import com.cicd.orchestrator.model.NotificationEvent.Channel;
import com.cicd.orchestrator.model.NotificationEvent.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Multi-channel notification service for CI/CD pipeline events.
 * Supports Slack webhook integration and generic HTTP webhook dispatch
 * for pipeline failures, approvals, deployments, and security alerts.
 */
@Service
public class NotificationService {
    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final WebClient webClient;
    private final String slackWebhookUrl;
    private final String genericWebhookUrl;
    private final boolean notificationsEnabled;

    // Audit trail of recent notifications (bounded)
    private final Queue<NotificationEvent> recentNotifications = new ConcurrentLinkedQueue<>();
    private static final int MAX_HISTORY = 100;

    public NotificationService(
            WebClient.Builder webClientBuilder,
            @Value("${notifications.slack.webhook-url:}") String slackWebhookUrl,
            @Value("${notifications.webhook.url:}") String genericWebhookUrl,
            @Value("${notifications.enabled:true}") boolean notificationsEnabled
    ) {
        this.webClient = webClientBuilder.build();
        this.slackWebhookUrl = slackWebhookUrl;
        this.genericWebhookUrl = genericWebhookUrl;
        this.notificationsEnabled = notificationsEnabled;
    }

    /**
     * Send a pipeline failure notification.
     */
    public NotificationEvent notifyPipelineFailure(String pipelineId, String repo, String branch,
                                                     String commitSha, List<String> errors) {
        String errorSummary = errors != null && !errors.isEmpty()
                ? String.join("; ", errors.subList(0, Math.min(errors.size(), 3)))
                : "No error details available";

        NotificationEvent event = NotificationEvent.create(UUID.randomUUID().toString())
                .pipelineId(pipelineId)
                .repo(repo)
                .severity(Severity.CRITICAL)
                .channel(Channel.ALL)
                .title("🚨 Pipeline Failed: " + repo)
                .message(String.format(
                        "Pipeline `%s` failed on branch `%s` (commit: `%s`).%nErrors: %s",
                        pipelineId, branch, truncateSha(commitSha), errorSummary
                ))
                .addMeta("branch", branch)
                .addMeta("commitSha", commitSha);

        return dispatch(event);
    }

    /**
     * Send a pipeline success notification.
     */
    public NotificationEvent notifyPipelineSuccess(String pipelineId, String repo,
                                                     String branch, long durationMs) {
        NotificationEvent event = NotificationEvent.create(UUID.randomUUID().toString())
                .pipelineId(pipelineId)
                .repo(repo)
                .severity(Severity.INFO)
                .channel(Channel.ALL)
                .title("✅ Pipeline Completed: " + repo)
                .message(String.format(
                        "Pipeline `%s` completed successfully on branch `%s` in %dms.",
                        pipelineId, branch, durationMs
                ))
                .addMeta("branch", branch)
                .addMeta("durationMs", String.valueOf(durationMs));

        return dispatch(event);
    }

    /**
     * Send a deployment gate notification (pipeline awaiting human approval).
     */
    public NotificationEvent notifyDeploymentGating(String pipelineId, String repo,
                                                      String environment) {
        NotificationEvent event = NotificationEvent.create(UUID.randomUUID().toString())
                .pipelineId(pipelineId)
                .repo(repo)
                .severity(Severity.WARNING)
                .channel(Channel.SLACK)
                .title("⏳ Approval Required: " + repo)
                .message(String.format(
                        "Pipeline `%s` for `%s` environment requires human approval.%n" +
                        "Approve: POST /api/v1/pipelines/%s/approve",
                        pipelineId, environment, pipelineId
                ))
                .addMeta("environment", environment);

        return dispatch(event);
    }

    /**
     * Send a security vulnerability notification.
     */
    public NotificationEvent notifySecurityAlert(String pipelineId, String repo,
                                                   String riskScore, int vulnCount) {
        Severity sev = "HIGH".equalsIgnoreCase(riskScore) || "CRITICAL".equalsIgnoreCase(riskScore)
                ? Severity.CRITICAL : Severity.WARNING;

        NotificationEvent event = NotificationEvent.create(UUID.randomUUID().toString())
                .pipelineId(pipelineId)
                .repo(repo)
                .severity(sev)
                .channel(Channel.ALL)
                .title("🔒 Security Alert: " + repo)
                .message(String.format(
                        "Dependency scan found %d vulnerabilities (risk: %s) in pipeline `%s`.",
                        vulnCount, riskScore, pipelineId
                ))
                .addMeta("riskScore", riskScore)
                .addMeta("vulnerabilities", String.valueOf(vulnCount));

        return dispatch(event);
    }

    /**
     * Send a retry notification.
     */
    public NotificationEvent notifyRetryAttempt(String pipelineId, String repo, String taskId,
                                                  int attempt, int maxRetries, String error) {
        NotificationEvent event = NotificationEvent.create(UUID.randomUUID().toString())
                .pipelineId(pipelineId)
                .repo(repo)
                .severity(Severity.WARNING)
                .channel(Channel.SLACK)
                .title("🔄 Retry Attempt: " + taskId)
                .message(String.format(
                        "Task `%s` in pipeline `%s` failed (attempt %d/%d).%nError: %s%nRetrying...",
                        taskId, pipelineId, attempt, maxRetries, error
                ))
                .addMeta("taskId", taskId)
                .addMeta("attempt", String.valueOf(attempt))
                .addMeta("maxRetries", String.valueOf(maxRetries));

        return dispatch(event);
    }

    /**
     * Send a rollback notification.
     */
    public NotificationEvent notifyRollback(String pipelineId, String repo,
                                              String environment, String reason) {
        NotificationEvent event = NotificationEvent.create(UUID.randomUUID().toString())
                .pipelineId(pipelineId)
                .repo(repo)
                .severity(Severity.CRITICAL)
                .channel(Channel.ALL)
                .title("⏪ Rollback Triggered: " + repo)
                .message(String.format(
                        "Automatic rollback initiated for pipeline `%s` in `%s` environment.%nReason: %s",
                        pipelineId, environment, reason
                ))
                .addMeta("environment", environment)
                .addMeta("reason", reason);

        return dispatch(event);
    }

    /**
     * Retrieve recent notification history.
     */
    public List<NotificationEvent> getRecentNotifications() {
        return new ArrayList<>(recentNotifications);
    }

    /**
     * Core dispatch logic — routes to Slack and/or generic webhook based on channel.
     */
    private NotificationEvent dispatch(NotificationEvent event) {
        if (!notificationsEnabled) {
            log.info("[DRY-RUN] Notification: {} — {}", event.getTitle(), event.getMessage());
            event.markDelivered();
            addToHistory(event);
            return event;
        }

        boolean slackSuccess = true;
        boolean webhookSuccess = true;

        if (event.getChannel() == Channel.SLACK || event.getChannel() == Channel.ALL) {
            slackSuccess = sendSlack(event);
        }

        if (event.getChannel() == Channel.WEBHOOK || event.getChannel() == Channel.ALL) {
            webhookSuccess = sendWebhook(event);
        }

        if (slackSuccess && webhookSuccess) {
            event.markDelivered();
        } else {
            String errors = (!slackSuccess ? "Slack delivery failed. " : "") +
                           (!webhookSuccess ? "Webhook delivery failed." : "");
            event.markFailed(errors.trim());
        }

        addToHistory(event);
        return event;
    }

    /**
     * Send notification to Slack via incoming webhook.
     */
    private boolean sendSlack(NotificationEvent event) {
        if (slackWebhookUrl == null || slackWebhookUrl.isBlank()) {
            log.debug("Slack webhook URL not configured. Skipping Slack notification.");
            return true; // Not a failure, just unconfigured
        }

        try {
            String emoji = getEmojiForSeverity(event.getSeverity());
            String color = getColorForSeverity(event.getSeverity());

            Map<String, Object> payload = buildSlackPayload(event, emoji, color);

            webClient.post()
                    .uri(slackWebhookUrl)
                    .bodyValue(payload)
                    .retrieve()
                    .toBodilessEntity()
                    .block();

            log.info("✅ Slack notification sent: {}", event.getTitle());
            return true;
        } catch (Exception e) {
            log.error("❌ Slack notification failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Send notification to a generic webhook endpoint.
     */
    private boolean sendWebhook(NotificationEvent event) {
        if (genericWebhookUrl == null || genericWebhookUrl.isBlank()) {
            log.debug("Generic webhook URL not configured. Skipping webhook notification.");
            return true;
        }

        try {
            webClient.post()
                    .uri(genericWebhookUrl)
                    .bodyValue(event)
                    .retrieve()
                    .toBodilessEntity()
                    .block();

            log.info("✅ Webhook notification sent: {}", event.getTitle());
            return true;
        } catch (Exception e) {
            log.error("❌ Webhook notification failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Build a rich Slack message payload with attachments.
     */
    private Map<String, Object> buildSlackPayload(NotificationEvent event, String emoji, String color) {
        Map<String, Object> attachment = new LinkedHashMap<>();
        attachment.put("color", color);
        attachment.put("title", event.getTitle());
        attachment.put("text", event.getMessage());
        attachment.put("ts", event.getTimestamp().getEpochSecond());

        List<Map<String, String>> fields = new ArrayList<>();
        if (event.getPipelineId() != null) {
            fields.add(Map.of("title", "Pipeline ID", "value", event.getPipelineId(), "short", "true"));
        }
        if (event.getRepo() != null) {
            fields.add(Map.of("title", "Repository", "value", event.getRepo(), "short", "true"));
        }
        for (Map.Entry<String, String> meta : event.getMetadata().entrySet()) {
            fields.add(Map.of("title", meta.getKey(), "value", meta.getValue(), "short", "true"));
        }
        attachment.put("fields", fields);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("text", emoji + " " + event.getTitle());
        payload.put("attachments", List.of(attachment));
        return payload;
    }

    private void addToHistory(NotificationEvent event) {
        recentNotifications.add(event);
        while (recentNotifications.size() > MAX_HISTORY) {
            recentNotifications.poll();
        }
    }

    private String getEmojiForSeverity(Severity severity) {
        return switch (severity) {
            case INFO -> "ℹ️";
            case WARNING -> "⚠️";
            case CRITICAL -> "🚨";
        };
    }

    private String getColorForSeverity(Severity severity) {
        return switch (severity) {
            case INFO -> "#36a64f";     // green
            case WARNING -> "#ffa500";  // orange
            case CRITICAL -> "#ff0000"; // red
        };
    }

    private String truncateSha(String sha) {
        if (sha == null) return "unknown";
        return sha.length() > 7 ? sha.substring(0, 7) : sha;
    }
}
