package com.cicd.orchestrator.service;

import com.cicd.orchestrator.model.CommitEvent;
import com.cicd.orchestrator.model.PipelineState;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.UUID;

/**
 * Service to verify and process GitHub Webhook payloads.
 * Implements HMAC-SHA256 signature verification and event extraction.
 */
@Service
public class GitHubWebhookService {
    private static final Logger log = LoggerFactory.getLogger(GitHubWebhookService.class);
    private static final String HMAC_SHA256 = "HmacSHA256";

    private final String webhookSecret;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final PipelineExecutionService executionService;
    private final ObjectMapper objectMapper;

    public GitHubWebhookService(
            @Value("${agent.github.webhook-secret:}") String webhookSecret,
            KafkaTemplate<String, String> kafkaTemplate,
            PipelineExecutionService executionService,
            ObjectMapper objectMapper
    ) {
        this.webhookSecret = webhookSecret != null ? webhookSecret.trim() : "";
        this.kafkaTemplate = kafkaTemplate;
        this.executionService = executionService;
        this.objectMapper = objectMapper;
    }

    /**
     * Verifies the HMAC-SHA256 signature of the incoming webhook payload.
     * Uses constant-time equality check to prevent timing attacks.
     */
    public boolean verifySignature(String payload, String signatureHeader) {
        if (webhookSecret.isEmpty()) {
            // Secret not configured; permit for local sandbox testing
            log.warn("GitHub webhook secret is not configured. Skipping HMAC verification.");
            return true;
        }

        if (signatureHeader == null || !signatureHeader.startsWith("sha256=")) {
            log.warn("Missing or malformed X-Hub-Signature-256 header: {}", signatureHeader);
            return false;
        }

        String expectedSignature = signatureHeader.substring("sha256=".length());

        try {
            Mac mac = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec secretKey = new SecretKeySpec(webhookSecret.getBytes(StandardCharsets.UTF_8), HMAC_SHA256);
            mac.init(secretKey);
            byte[] computedHash = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            String computedHex = HexFormat.of().formatHex(computedHash);

            return MessageDigest.isEqual(
                    expectedSignature.getBytes(StandardCharsets.UTF_8),
                    computedHex.getBytes(StandardCharsets.UTF_8)
            );
        } catch (Exception e) {
            log.error("Failed to calculate HMAC signature", e);
            return false;
        }
    }

    /**
     * Processes verified GitHub events (push, pull_request, ping).
     */
    public CommitEvent processWebhook(String eventType, String signatureHeader, String rawPayload) {
        if (!verifySignature(rawPayload, signatureHeader)) {
            throw new SecurityException("Invalid HMAC-SHA256 webhook signature.");
        }

        try {
            JsonNode root = objectMapper.readTree(rawPayload);

            if ("ping".equalsIgnoreCase(eventType)) {
                log.info("Received GitHub ping event from repository: {}",
                        root.path("repository").path("full_name").asText("unknown"));
                return null;
            }

            CommitEvent event = null;
            if ("push".equalsIgnoreCase(eventType)) {
                event = parsePushEvent(root);
            } else if ("pull_request".equalsIgnoreCase(eventType)) {
                event = parsePullRequestEvent(root);
            } else {
                log.info("Ignored unsupported GitHub event type: {}", eventType);
                return null;
            }

            if (event != null) {
                // Publish to Kafka topic for event-driven decoupled ingestion
                String eventJson = objectMapper.writeValueAsString(event);
                kafkaTemplate.send("commit-events", event.getRepo(), eventJson);
                log.info("Published commit event to Kafka topic 'commit-events' for repo {}", event.getRepo());

                // Trigger pipeline directly in orchestrator
                String pipelineId = UUID.randomUUID().toString();
                PipelineState state = new PipelineState(pipelineId, event.getRepo(), event.getBranch(), event.getCommitSha())
                        .author(event.getAuthor())
                        .commitMessage(event.getCommitMessage())
                        .prNumber(event.getPrNumber());

                executionService.executePipeline(state);
            }

            return event;
        } catch (SecurityException se) {
            throw se;
        } catch (Exception e) {
            log.error("Error parsing GitHub webhook payload", e);
            throw new IllegalArgumentException("Invalid webhook JSON payload: " + e.getMessage(), e);
        }
    }

    private CommitEvent parsePushEvent(JsonNode root) {
        CommitEvent event = new CommitEvent();
        String repo = root.path("repository").path("full_name").asText("unknown/repo");
        String ref = root.path("ref").asText("");
        String branch = ref.startsWith("refs/heads/") ? ref.substring("refs/heads/".length()) : ref;
        
        JsonNode headCommit = root.path("head_commit");
        String commitSha = headCommit.path("id").asText(root.path("after").asText("unknown-sha"));
        String message = headCommit.path("message").asText("Push commit");
        String author = headCommit.path("author").path("username").asText(
                headCommit.path("author").path("name").asText("unknown-author")
        );

        int added = headCommit.path("added").size();
        int modified = headCommit.path("modified").size();
        int removed = headCommit.path("removed").size();

        event.setRepo(repo);
        event.setBranch(branch);
        event.setCommitSha(commitSha);
        event.setCommitMessage(message);
        event.setAuthor(author);
        event.setChangedFiles(added + modified + removed);

        return event;
    }

    private CommitEvent parsePullRequestEvent(JsonNode root) {
        String action = root.path("action").asText("");
        // We trigger on pull requests opened, synchronize (new push to PR), or reopened
        if (!("opened".equals(action) || "synchronize".equals(action) || "reopened".equals(action))) {
            log.info("Ignoring pull_request action: {}", action);
            return null;
        }

        JsonNode prNode = root.path("pull_request");
        int prNumber = prNode.path("number").asInt();
        String prUrl = prNode.path("html_url").asText("");
        String repo = root.path("repository").path("full_name").asText("unknown/repo");
        String branch = prNode.path("head").path("ref").asText("unknown-branch");
        String commitSha = prNode.path("head").path("sha").asText("unknown-sha");
        String title = prNode.path("title").asText("PR #" + prNumber);
        String author = prNode.path("user").path("login").asText("unknown-author");

        CommitEvent event = new CommitEvent();
        event.setRepo(repo);
        event.setBranch(branch);
        event.setCommitSha(commitSha);
        event.setCommitMessage("PR #" + prNumber + ": " + title);
        event.setAuthor(author);
        event.setPrNumber(prNumber);
        event.setPrUrl(prUrl);
        event.setAdditions(prNode.path("additions").asInt(0));
        event.setDeletions(prNode.path("deletions").asInt(0));
        event.setChangedFiles(prNode.path("changed_files").asInt(0));

        return event;
    }
}
