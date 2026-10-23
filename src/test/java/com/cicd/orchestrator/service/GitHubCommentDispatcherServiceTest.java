package com.cicd.orchestrator.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.reactive.function.client.WebClient;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class GitHubCommentDispatcherServiceTest {

    private GitHubCommentDispatcherService dispatcherService;

    @BeforeEach
    void setUp() {
        // Construct with empty token to test dry-run simulation mode
        dispatcherService = new GitHubCommentDispatcherService("", WebClient.builder().build());
    }

    @Test
    void testDispatchPrCommentDryRunSucceeds() {
        boolean result = dispatcherService.dispatchPrComment(
                "veerjain-1/agentguard",
                42,
                "### 🤖 Autonomous Code Review\nLooks great!"
        );

        assertTrue(result, "Dry-run dispatch should succeed cleanly when token is unconfigured");
    }

    @Test
    void testDispatchPrCommentNullPrReturnsFalse() {
        boolean result = dispatcherService.dispatchPrComment(
                "veerjain-1/agentguard",
                null,
                "Some comment"
        );

        assertFalse(result, "Dispatch with null PR number should be skipped and return false");
    }

    @Test
    void testDispatchPrCommentZeroPrReturnsFalse() {
        boolean result = dispatcherService.dispatchPrComment(
                "veerjain-1/agentguard",
                0,
                "Some comment"
        );

        assertFalse(result, "Dispatch with 0 PR number should be skipped and return false");
    }

    @Test
    void testDispatchPrCommentBlankBodyReturnsFalse() {
        boolean result = dispatcherService.dispatchPrComment(
                "veerjain-1/agentguard",
                12,
                "   "
        );

        assertFalse(result, "Dispatch with blank body should return false");
    }

    @Test
    void testDispatchGateStatusUpdateApproved() {
        boolean result = dispatcherService.dispatchGateStatusUpdate(
                "veerjain-1/agentguard",
                42,
                "APPROVED",
                "veerjain",
                "Deployment verified"
        );

        assertTrue(result);
    }

    @Test
    void testDispatchGateStatusUpdateRejected() {
        boolean result = dispatcherService.dispatchGateStatusUpdate(
                "veerjain-1/agentguard",
                42,
                "REJECTED",
                "sec-lead",
                "Critical vulnerability detected"
        );

        assertTrue(result);
    }
}
