package com.cicd.orchestrator.service;

import com.cicd.orchestrator.model.CommitEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GitHubWebhookServiceTest {

    private static final String SECRET = "super-secret-key-12345";

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private PipelineExecutionService executionService;

    private ObjectMapper objectMapper;
    private GitHubWebhookService webhookService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        webhookService = new GitHubWebhookService(SECRET, kafkaTemplate, executionService, objectMapper);
    }

    private String computeHmacSha256(String payload, String secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKey = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            mac.init(secretKey);
            byte[] hash = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            return "sha256=" + HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testVerifySignatureSuccess() {
        String payload = "{\"test\": \"payload\"}";
        String signature = computeHmacSha256(payload, SECRET);

        assertTrue(webhookService.verifySignature(payload, signature));
    }

    @Test
    void testVerifySignatureTamperedPayloadFails() {
        String payload = "{\"test\": \"payload\"}";
        String signature = computeHmacSha256(payload, SECRET);

        assertFalse(webhookService.verifySignature("{\"test\": \"tampered\"}", signature));
    }

    @Test
    void testVerifySignatureWrongSecretFails() {
        String payload = "{\"test\": \"payload\"}";
        String signature = computeHmacSha256(payload, "wrong-secret");

        assertFalse(webhookService.verifySignature(payload, signature));
    }

    @Test
    void testVerifySignatureMissingHeaderFails() {
        String payload = "{\"test\": \"payload\"}";

        assertFalse(webhookService.verifySignature(payload, null));
        assertFalse(webhookService.verifySignature(payload, "malformed-header"));
    }

    @Test
    void testProcessPushWebhook() {
        String pushPayload = """
                {
                  "ref": "refs/heads/feature/login",
                  "after": "deadbeef12345678",
                  "repository": {
                    "full_name": "veerjain-1/test-repo"
                  },
                  "head_commit": {
                    "id": "deadbeef12345678",
                    "message": "Add user authentication flow",
                    "author": {
                      "username": "veerjain"
                    },
                    "added": ["Auth.java"],
                    "modified": ["User.java"],
                    "removed": []
                  }
                }
                """;

        String signature = computeHmacSha256(pushPayload, SECRET);

        CommitEvent event = webhookService.processWebhook("push", signature, pushPayload);

        assertNotNull(event);
        assertEquals("veerjain-1/test-repo", event.getRepo());
        assertEquals("feature/login", event.getBranch());
        assertEquals("deadbeef12345678", event.getCommitSha());
        assertEquals("Add user authentication flow", event.getCommitMessage());
        assertEquals("veerjain", event.getAuthor());
        assertEquals(2, event.getChangedFiles());

        verify(kafkaTemplate, times(1)).send(eq("commit-events"), eq("veerjain-1/test-repo"), any());
        verify(executionService, times(1)).executePipeline(any());
    }

    @Test
    void testProcessPullRequestWebhook() {
        String prPayload = """
                {
                  "action": "opened",
                  "repository": {
                    "full_name": "veerjain-1/test-repo"
                  },
                  "pull_request": {
                    "number": 42,
                    "html_url": "https://github.com/veerjain-1/test-repo/pull/42",
                    "title": "Upgrade Spring Boot dependencies",
                    "user": {
                      "login": "octocat"
                    },
                    "head": {
                      "ref": "dependabot/spring-boot-3",
                      "sha": "abc123fed456"
                    },
                    "additions": 150,
                    "deletions": 40,
                    "changed_files": 4
                  }
                }
                """;

        String signature = computeHmacSha256(prPayload, SECRET);

        CommitEvent event = webhookService.processWebhook("pull_request", signature, prPayload);

        assertNotNull(event);
        assertEquals("veerjain-1/test-repo", event.getRepo());
        assertEquals("dependabot/spring-boot-3", event.getBranch());
        assertEquals("abc123fed456", event.getCommitSha());
        assertEquals(42, event.getPrNumber());
        assertEquals("https://github.com/veerjain-1/test-repo/pull/42", event.getPrUrl());
        assertEquals(150, event.getAdditions());
        assertEquals(40, event.getDeletions());

        verify(kafkaTemplate, times(1)).send(eq("commit-events"), eq("veerjain-1/test-repo"), any());
        verify(executionService, times(1)).executePipeline(any());
    }

    @Test
    void testProcessPingWebhookReturnsNull() {
        String pingPayload = """
                {
                  "zen": "Keep it logically awesome.",
                  "repository": {
                    "full_name": "veerjain-1/test-repo"
                  }
                }
                """;
        String signature = computeHmacSha256(pingPayload, SECRET);

        CommitEvent event = webhookService.processWebhook("ping", signature, pingPayload);
        assertNull(event);

        verify(kafkaTemplate, never()).send(any(), any(), any());
        verify(executionService, never()).executePipeline(any());
    }

    @Test
    void testInvalidSignatureThrowsSecurityException() {
        String payload = "{\"test\": \"payload\"}";
        assertThrows(SecurityException.class, () -> {
            webhookService.processWebhook("push", "sha256=invalidhash", payload);
        });
    }
}
