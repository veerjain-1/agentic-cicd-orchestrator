package com.cicd.orchestrator.service;

import com.cicd.orchestrator.model.NotificationEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class NotificationServiceTest {

    private NotificationService notificationService;

    @BeforeEach
    void setUp() {
        // Use dry-run mode (notifications.enabled=false) so we don't need live webhook URLs
        notificationService = new NotificationService(
                WebClient.builder(), "", "", false
        );
    }

    @Test
    void testPipelineFailureNotification() {
        NotificationEvent event = notificationService.notifyPipelineFailure(
                "pipe-1", "veerjain-1/repo", "main", "abc1234def",
                List.of("Build failed", "Test timeout")
        );

        assertNotNull(event);
        assertEquals(NotificationEvent.Severity.CRITICAL, event.getSeverity());
        assertEquals(NotificationEvent.Channel.ALL, event.getChannel());
        assertTrue(event.getTitle().contains("Pipeline Failed"));
        assertTrue(event.getMessage().contains("pipe-1"));
        assertTrue(event.getMessage().contains("abc1234")); // Truncated SHA
        assertTrue(event.isDelivered()); // Dry-run always succeeds
    }

    @Test
    void testPipelineSuccessNotification() {
        NotificationEvent event = notificationService.notifyPipelineSuccess(
                "pipe-2", "veerjain-1/repo", "main", 12500
        );

        assertNotNull(event);
        assertEquals(NotificationEvent.Severity.INFO, event.getSeverity());
        assertTrue(event.getTitle().contains("Pipeline Completed"));
        assertTrue(event.getMessage().contains("12500"));
        assertEquals("12500", event.getMetadata().get("durationMs"));
    }

    @Test
    void testDeploymentGatingNotification() {
        NotificationEvent event = notificationService.notifyDeploymentGating(
                "pipe-3", "veerjain-1/repo", "production"
        );

        assertNotNull(event);
        assertEquals(NotificationEvent.Severity.WARNING, event.getSeverity());
        assertEquals(NotificationEvent.Channel.SLACK, event.getChannel());
        assertTrue(event.getTitle().contains("Approval Required"));
        assertTrue(event.getMessage().contains("production"));
        assertTrue(event.getMessage().contains("/approve"));
    }

    @Test
    void testSecurityAlertCriticalSeverity() {
        NotificationEvent event = notificationService.notifySecurityAlert(
                "pipe-4", "veerjain-1/repo", "CRITICAL", 5
        );

        assertNotNull(event);
        assertEquals(NotificationEvent.Severity.CRITICAL, event.getSeverity());
        assertTrue(event.getTitle().contains("Security Alert"));
        assertTrue(event.getMessage().contains("5 vulnerabilities"));
    }

    @Test
    void testSecurityAlertWarningSeverity() {
        NotificationEvent event = notificationService.notifySecurityAlert(
                "pipe-5", "veerjain-1/repo", "MEDIUM", 2
        );

        assertEquals(NotificationEvent.Severity.WARNING, event.getSeverity());
    }

    @Test
    void testRetryAttemptNotification() {
        NotificationEvent event = notificationService.notifyRetryAttempt(
                "pipe-6", "veerjain-1/repo", "build", 2, 3, "Connection timeout"
        );

        assertNotNull(event);
        assertEquals(NotificationEvent.Severity.WARNING, event.getSeverity());
        assertTrue(event.getTitle().contains("Retry Attempt"));
        assertTrue(event.getMessage().contains("2/3"));
        assertTrue(event.getMessage().contains("Connection timeout"));
    }

    @Test
    void testRollbackNotification() {
        NotificationEvent event = notificationService.notifyRollback(
                "pipe-7", "veerjain-1/repo", "production", "Critical failure"
        );

        assertNotNull(event);
        assertEquals(NotificationEvent.Severity.CRITICAL, event.getSeverity());
        assertEquals(NotificationEvent.Channel.ALL, event.getChannel());
        assertTrue(event.getTitle().contains("Rollback Triggered"));
        assertTrue(event.getMessage().contains("production"));
    }

    @Test
    void testNotificationHistoryBounded() {
        // Send 110 notifications, history should be capped at 100
        for (int i = 0; i < 110; i++) {
            notificationService.notifyPipelineSuccess("pipe-" + i, "repo", "main", 100);
        }

        List<NotificationEvent> history = notificationService.getRecentNotifications();
        assertEquals(100, history.size());
    }

    @Test
    void testNotificationMetadata() {
        NotificationEvent event = notificationService.notifyPipelineFailure(
                "pipe-8", "veerjain-1/repo", "feature-x", "def456",
                List.of("OOM killed")
        );

        assertEquals("feature-x", event.getMetadata().get("branch"));
        assertEquals("def456", event.getMetadata().get("commitSha"));
    }

    @Test
    void testDryRunMode() {
        // Already in dry-run mode (enabled=false)
        NotificationEvent event = notificationService.notifyPipelineFailure(
                "pipe-9", "veerjain-1/repo", "main", "abc123", List.of()
        );

        assertTrue(event.isDelivered());
        assertNull(event.getDeliveryError());
    }
}
