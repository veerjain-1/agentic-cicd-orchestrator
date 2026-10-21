package com.cicd.orchestrator.controller;

import com.cicd.orchestrator.model.CommitEvent;
import com.cicd.orchestrator.service.GitHubWebhookService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Controller to receive and process Webhook events from VCS providers (e.g. GitHub).
 */
@RestController
@RequestMapping("/api/v1/webhooks")
public class WebhookController {
    private static final Logger log = LoggerFactory.getLogger(WebhookController.class);

    private final GitHubWebhookService gitHubWebhookService;

    public WebhookController(GitHubWebhookService gitHubWebhookService) {
        this.gitHubWebhookService = gitHubWebhookService;
    }

    @PostMapping(value = "/github", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> handleGitHubWebhook(
            @RequestHeader(value = "X-GitHub-Event", defaultValue = "push") String eventType,
            @RequestHeader(value = "X-Hub-Signature-256", required = false) String signatureHeader,
            @RequestBody String rawPayload
    ) {
        log.info("Received GitHub webhook event: {}", eventType);

        try {
            CommitEvent event = gitHubWebhookService.processWebhook(eventType, signatureHeader, rawPayload);

            if (event == null) {
                return ResponseEntity.ok(Map.of(
                        "status", "IGNORED",
                        "event", eventType,
                        "message", "Webhook event acknowledged but no pipeline triggered."
                ));
            }

            return ResponseEntity.accepted().body(Map.of(
                    "status", "TRIGGERED",
                    "repo", event.getRepo(),
                    "branch", event.getBranch(),
                    "commitSha", event.getCommitSha(),
                    "author", event.getAuthor() != null ? event.getAuthor() : "unknown"
            ));

        } catch (SecurityException se) {
            log.warn("HMAC verification failed for webhook: {}", se.getMessage());
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of(
                    "status", "UNAUTHORIZED",
                    "error", se.getMessage()
            ));
        } catch (IllegalArgumentException iae) {
            log.warn("Malformed webhook payload: {}", iae.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "BAD_REQUEST",
                    "error", iae.getMessage()
            ));
        } catch (Exception e) {
            log.error("Internal error processing webhook", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "ERROR",
                    "error", e.getMessage()
            ));
        }
    }
}
