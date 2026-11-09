package com.cicd.orchestrator.service;

import com.cicd.orchestrator.model.NotificationEvent;
import com.cicd.orchestrator.model.PipelineState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RollbackServiceTest {

    @Mock
    private NotificationService notificationService;

    @Mock
    private PipelineFeedbackProducer feedbackProducer;

    private RollbackService rollbackService;

    @BeforeEach
    void setUp() {
        rollbackService = new RollbackService(notificationService, feedbackProducer);
    }

    @Test
    void testShouldRollbackProductionFailure() {
        PipelineState state = new PipelineState("pipe-1", "veerjain-1/repo", "main", "sha123");
        state.environment("production");
        state.status(PipelineState.PipelineStatus.FAILED);

        assertTrue(rollbackService.shouldRollback(state));
    }

    @Test
    void testShouldNotRollbackDevelopmentFailure() {
        PipelineState state = new PipelineState("pipe-2", "veerjain-1/repo", "main", "sha123");
        state.environment("development");
        state.status(PipelineState.PipelineStatus.FAILED);

        assertFalse(rollbackService.shouldRollback(state));
    }

    @Test
    void testShouldRollbackStagingWithCriticalSecurity() {
        PipelineState state = new PipelineState("pipe-3", "veerjain-1/repo", "main", "sha123");
        state.environment("staging");
        state.status(PipelineState.PipelineStatus.FAILED);
        state.setCommitAnalysis(Map.of("riskScore", "CRITICAL"));

        assertTrue(rollbackService.shouldRollback(state));
    }

    @Test
    void testShouldNotRollbackStagingWithLowRisk() {
        PipelineState state = new PipelineState("pipe-4", "veerjain-1/repo", "main", "sha123");
        state.environment("staging");
        state.status(PipelineState.PipelineStatus.FAILED);
        state.setCommitAnalysis(Map.of("riskScore", "LOW"));

        assertFalse(rollbackService.shouldRollback(state));
    }

    @Test
    void testShouldNotRollbackCompletedPipeline() {
        PipelineState state = new PipelineState("pipe-5", "veerjain-1/repo", "main", "sha123");
        state.environment("production");
        state.status(PipelineState.PipelineStatus.COMPLETED);

        assertFalse(rollbackService.shouldRollback(state));
    }

    @Test
    void testShouldRollbackBlockedPipeline() {
        PipelineState state = new PipelineState("pipe-6", "veerjain-1/repo", "main", "sha123");
        state.environment("production");
        state.status(PipelineState.PipelineStatus.BLOCKED);

        assertTrue(rollbackService.shouldRollback(state));
    }

    @Test
    void testShouldNotRollbackNullState() {
        assertFalse(rollbackService.shouldRollback(null));
    }

    @Test
    void testExecuteRollback() {
        // Mock notification return
        when(notificationService.notifyRollback(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(new NotificationEvent());

        PipelineState state = new PipelineState("pipe-7", "veerjain-1/repo", "main", "sha123");
        state.environment("production");
        state.status(PipelineState.PipelineStatus.FAILED);

        RollbackService.RollbackRecord record = rollbackService.executeRollback(state, "Critical failure");

        assertNotNull(record);
        assertTrue(record.success);
        assertEquals("pipe-7", record.pipelineId);
        assertEquals("veerjain-1/repo", record.repo);
        assertEquals("production", record.environment);
        assertEquals("Critical failure", record.reason);
        assertNotNull(record.triggeredAt);
        assertNotNull(record.completedAt);
        assertFalse(record.steps.isEmpty());

        verify(notificationService).notifyRollback("pipe-7", "veerjain-1/repo", "production", "Critical failure");
        verify(feedbackProducer).sendDeploymentGateEvent(eq("pipe-7"), eq("veerjain-1/repo"), eq("ROLLED_BACK"), anyString());
    }

    @Test
    void testRegisterAndRetrieveLastKnownGood() {
        rollbackService.registerSuccessfulDeployment("veerjain-1/repo", "production", "sha-abc123");

        assertEquals("sha-abc123", rollbackService.getLastKnownGoodVersion("veerjain-1/repo", "production"));
        assertNull(rollbackService.getLastKnownGoodVersion("veerjain-1/repo", "staging"));
    }

    @Test
    void testRollbackRecordPersistence() {
        when(notificationService.notifyRollback(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(new NotificationEvent());

        PipelineState state = new PipelineState("pipe-8", "veerjain-1/repo", "main", "sha123");
        state.environment("production");

        rollbackService.executeRollback(state, "Test reason");

        RollbackService.RollbackRecord record = rollbackService.getRollbackRecord("pipe-8");
        assertNotNull(record);
        assertEquals("pipe-8", record.pipelineId);
    }

    @Test
    void testGetAllRollbacks() {
        when(notificationService.notifyRollback(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(new NotificationEvent());

        PipelineState state1 = new PipelineState("pipe-9", "veerjain-1/repo1", "main", "sha1");
        state1.environment("production");
        PipelineState state2 = new PipelineState("pipe-10", "veerjain-1/repo2", "main", "sha2");
        state2.environment("production");

        rollbackService.executeRollback(state1, "Failure 1");
        rollbackService.executeRollback(state2, "Failure 2");

        List<RollbackService.RollbackRecord> all = rollbackService.getAllRollbacks();
        assertEquals(2, all.size());
    }

    @Test
    void testRollbackRecordToMap() {
        when(notificationService.notifyRollback(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(new NotificationEvent());

        PipelineState state = new PipelineState("pipe-11", "veerjain-1/repo", "main", "sha123");
        state.environment("production");

        RollbackService.RollbackRecord record = rollbackService.executeRollback(state, "Test");
        Map<String, Object> map = record.toMap();

        assertEquals("pipe-11", map.get("pipelineId"));
        assertEquals("veerjain-1/repo", map.get("repo"));
        assertEquals("production", map.get("environment"));
        assertTrue((Boolean) map.get("success"));
    }

    @Test
    void testRollbackUsesLastKnownGoodVersion() {
        when(notificationService.notifyRollback(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(new NotificationEvent());

        // Register a known good version
        rollbackService.registerSuccessfulDeployment("veerjain-1/repo", "production", "sha-good");

        PipelineState state = new PipelineState("pipe-12", "veerjain-1/repo", "main", "sha-bad");
        state.environment("production");

        RollbackService.RollbackRecord record = rollbackService.executeRollback(state, "Bad deploy");

        assertEquals("sha-good", record.previousGoodVersion);
    }
}
