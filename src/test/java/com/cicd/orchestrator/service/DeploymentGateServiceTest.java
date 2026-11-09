package com.cicd.orchestrator.service;

import com.cicd.orchestrator.model.PipelineState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DeploymentGateServiceTest {

    @Mock
    private PipelineFeedbackProducer feedbackProducer;

    @Mock
    private GitHubCommentDispatcherService commentDispatcher;

    private DeploymentGateService gateService;

    @BeforeEach
    void setUp() {
        gateService = new DeploymentGateService(feedbackProducer, commentDispatcher);
    }

    @Test
    void testPolicy1CriticalRiskBlocksDeployment() {
        PipelineState state = new PipelineState("pipe-1", "veerjain-1/agentguard", "main", "sha123");
        state.setCommitAnalysis(Map.of("riskScore", "CRITICAL", "securityFindings", java.util.List.of("Hardcoded AWS Secret")));

        boolean passed = gateService.evaluateGate(state);

        assertFalse(passed);
        assertEquals(PipelineState.PipelineStatus.BLOCKED, state.getStatus());
        assertEquals("BLOCKED_BY_CRITICAL_SECURITY_RISK", state.getDeploymentDecision());
        verify(feedbackProducer).sendDeploymentGateEvent(eq("pipe-1"), eq("veerjain-1/agentguard"), eq("BLOCKED"), anyString());
    }

    @Test
    void testPolicy2VulnerableDependenciesBlocksDeployment() {
        PipelineState state = new PipelineState("pipe-2", "veerjain-1/agentguard", "main", "sha123");
        state.setDependencyReport(Map.of("vulnerabilitiesFound", 2));

        boolean passed = gateService.evaluateGate(state);

        assertFalse(passed);
        assertEquals(PipelineState.PipelineStatus.BLOCKED, state.getStatus());
        assertEquals("BLOCKED_BY_DEPENDENCY_VULNERABILITIES", state.getDeploymentDecision());
        verify(feedbackProducer).sendDeploymentGateEvent(eq("pipe-2"), eq("veerjain-1/agentguard"), eq("BLOCKED"), anyString());
    }

    @Test
    void testPolicy3ProductionEnvironmentRequiresApproval() {
        PipelineState state = new PipelineState("pipe-3", "veerjain-1/agentguard", "main", "sha123");
        state.environment("production");

        boolean passed = gateService.evaluateGate(state);

        assertTrue(passed, "Gate hold returns true to preserve non-fatal evaluation");
        assertEquals(PipelineState.PipelineStatus.GATING, state.getStatus());
        assertEquals("AWAITING_HUMAN_APPROVAL", state.getDeploymentDecision());
        verify(feedbackProducer).sendDeploymentGateEvent(eq("pipe-3"), eq("veerjain-1/agentguard"), eq("GATING"), eq("AWAITING_HUMAN_APPROVAL"));
    }

    @Test
    void testPolicy4CleanDevelopmentBuildAutoApproved() {
        PipelineState state = new PipelineState("pipe-4", "veerjain-1/agentguard", "main", "sha123");
        state.environment("development");

        boolean passed = gateService.evaluateGate(state);

        assertTrue(passed);
        assertEquals(PipelineState.PipelineStatus.APPROVED, state.getStatus());
        assertEquals("AUTO_APPROVED", state.getDeploymentDecision());
        verify(feedbackProducer).sendDeploymentGateEvent(eq("pipe-4"), eq("veerjain-1/agentguard"), eq("APPROVED"), eq("AUTO_APPROVED"));
    }

    @Test
    void testApproveHeldPipeline() {
        PipelineState state = new PipelineState("pipe-5", "veerjain-1/agentguard", "main", "sha123");
        state.status(PipelineState.PipelineStatus.GATING);

        PipelineState approved = gateService.approve(state, "veerjain", "Verified production readiness");

        assertEquals(PipelineState.PipelineStatus.APPROVED, approved.getStatus());
        assertEquals("veerjain", approved.getApprover());
        assertEquals("Verified production readiness", approved.getApprovalNotes());
        assertNotNull(approved.getApprovedAt());
        assertEquals("APPROVED_BY_HUMAN", approved.getDeploymentDecision());

        verify(feedbackProducer).sendDeploymentGateEvent(eq("pipe-5"), eq("veerjain-1/agentguard"), eq("APPROVED"), eq("APPROVED_BY_HUMAN"));
        verify(commentDispatcher).dispatchGateStatusUpdate(eq("veerjain-1/agentguard"), any(), eq("APPROVED"), eq("veerjain"), anyString());
    }

    @Test
    void testRejectHeldPipeline() {
        PipelineState state = new PipelineState("pipe-6", "veerjain-1/agentguard", "main", "sha123");
        state.status(PipelineState.PipelineStatus.GATING);

        PipelineState rejected = gateService.reject(state, "sec-lead", "Security compliance check failed");

        assertEquals(PipelineState.PipelineStatus.BLOCKED, rejected.getStatus());
        assertEquals("sec-lead", rejected.getApprover());
        assertEquals("Security compliance check failed", rejected.getApprovalNotes());
        assertEquals("REJECTED_BY_HUMAN", rejected.getDeploymentDecision());

        verify(feedbackProducer).sendDeploymentGateEvent(eq("pipe-6"), eq("veerjain-1/agentguard"), eq("BLOCKED"), eq("REJECTED_BY_HUMAN"));
        verify(commentDispatcher).dispatchGateStatusUpdate(eq("veerjain-1/agentguard"), any(), eq("REJECTED"), eq("sec-lead"), anyString());
    }

    @Test
    void testApproveNonGatingPipelineThrowsException() {
        PipelineState state = new PipelineState("pipe-7", "veerjain-1/agentguard", "main", "sha123");
        state.status(PipelineState.PipelineStatus.COMPLETED);

        assertThrows(IllegalStateException.class, () -> {
            gateService.approve(state, "veerjain", "notes");
        });
    }
}
