package com.cicd.orchestrator.service;

import com.cicd.orchestrator.model.PipelineState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;

/**
 * Human-in-the-Loop (HITL) Deployment Gate Service.
 * Evaluates risk assessments, dependency scans, and target environments to
 * enforce strict production controls, automatic security blocks, and engineer approval workflows.
 */
@Service
public class DeploymentGateService {
    private static final Logger log = LoggerFactory.getLogger(DeploymentGateService.class);

    private final PipelineFeedbackProducer feedbackProducer;
    private final GitHubCommentDispatcherService commentDispatcher;

    public DeploymentGateService(
            PipelineFeedbackProducer feedbackProducer,
            GitHubCommentDispatcherService commentDispatcher
    ) {
        this.feedbackProducer = feedbackProducer;
        this.commentDispatcher = commentDispatcher;
    }

    /**
     * Evaluates deployment policies for a given pipeline run.
     *
     * @param state Current pipeline state
     * @return true if approved or held in GATING state for approval, false if hard-blocked
     */
    public boolean evaluateGate(PipelineState state) {
        log.info("Evaluating Deployment Gate policies for pipeline {} (env: {})",
                state.getPipelineId(), state.getEnvironment());

        Map<String, Object> analysis = state.getCommitAnalysis();
        String riskScore = "LOW";
        if (analysis != null) {
            riskScore = (String) analysis.getOrDefault("riskScore", "LOW");
        }

        Map<String, Object> depReport = state.getDependencyReport();
        int vulnerabilities = 0;
        if (depReport != null) {
            vulnerabilities = (Integer) depReport.getOrDefault("vulnerabilitiesFound", 0);
        }

        // Policy 1: Hard security block on Critical vulnerabilities
        if ("CRITICAL".equalsIgnoreCase(riskScore)) {
            log.warn("🚨 Deployment Gate BLOCKED pipeline {}: Critical security risk detected.", state.getPipelineId());
            state.status(PipelineState.PipelineStatus.BLOCKED);
            state.setDeploymentDecision("BLOCKED_BY_CRITICAL_SECURITY_RISK");
            state.addError("Critical security risk detected by AI code analysis agent.");
            feedbackProducer.sendDeploymentGateEvent(state.getPipelineId(), state.getRepo(), "BLOCKED", "BLOCKED_BY_CRITICAL_SECURITY_RISK");
            commentDispatcher.dispatchGateStatusUpdate(state.getRepo(), state.getPrNumber(), "BLOCKED", "security-agent", "Critical security risk detected.");
            return false;
        }

        // Policy 2: Hard block on unresolved dependency CVEs
        if (vulnerabilities > 0) {
            log.warn("🚨 Deployment Gate BLOCKED pipeline {}: {} vulnerable dependencies detected.",
                    state.getPipelineId(), vulnerabilities);
            state.status(PipelineState.PipelineStatus.BLOCKED);
            state.setDeploymentDecision("BLOCKED_BY_DEPENDENCY_VULNERABILITIES");
            state.addError(vulnerabilities + " known vulnerable dependencies detected.");
            feedbackProducer.sendDeploymentGateEvent(state.getPipelineId(), state.getRepo(), "BLOCKED", "BLOCKED_BY_DEPENDENCY_VULNERABILITIES");
            commentDispatcher.dispatchGateStatusUpdate(state.getRepo(), state.getPrNumber(), "BLOCKED", "dependency-validator", vulnerabilities + " known CVEs found.");
            return false;
        }

        // Policy 3: Production release or explicit flag requires Human Approval
        boolean isProd = "production".equalsIgnoreCase(state.getEnvironment()) || "prod".equalsIgnoreCase(state.getEnvironment());
        if (isProd || state.isRequiresApproval()) {
            log.info("⏳ Deployment Gate HOLD for pipeline {}: Production release requires Human-in-the-Loop approval.",
                    state.getPipelineId());
            state.status(PipelineState.PipelineStatus.GATING);
            state.setDeploymentDecision("AWAITING_HUMAN_APPROVAL");
            feedbackProducer.sendDeploymentGateEvent(state.getPipelineId(), state.getRepo(), "GATING", "AWAITING_HUMAN_APPROVAL");
            commentDispatcher.dispatchGateStatusUpdate(
                    state.getRepo(),
                    state.getPrNumber(),
                    "AWAITING_APPROVAL",
                    "deployment-gate-agent",
                    "Production release requires manual approval via REST API: POST /api/v1/pipelines/" + state.getPipelineId() + "/approve"
            );
            return true;
        }

        // Policy 4: Clean non-production runs are automatically approved
        log.info("✅ Deployment Gate AUTO-APPROVED pipeline {} for environment: {}",
                state.getPipelineId(), state.getEnvironment());
        state.status(PipelineState.PipelineStatus.APPROVED);
        state.setDeploymentDecision("AUTO_APPROVED");
        feedbackProducer.sendDeploymentGateEvent(state.getPipelineId(), state.getRepo(), "APPROVED", "AUTO_APPROVED");
        return true;
    }

    /**
     * Approves a held deployment gate.
     */
    public PipelineState approve(PipelineState state, String approver, String notes) {
        if (state.getStatus() != PipelineState.PipelineStatus.GATING) {
            throw new IllegalStateException("Pipeline " + state.getPipelineId() + " is not awaiting approval. Current status: " + state.getStatus());
        }

        log.info("👤 Human Approval received for pipeline {} by approver '{}'", state.getPipelineId(), approver);
        state.status(PipelineState.PipelineStatus.APPROVED);
        state.approver(approver != null && !approver.isBlank() ? approver : "anonymous-engineer");
        state.approvalNotes(notes != null ? notes : "Approved by engineer");
        state.approvedAt(Instant.now());
        state.setDeploymentDecision("APPROVED_BY_HUMAN");

        feedbackProducer.sendDeploymentGateEvent(state.getPipelineId(), state.getRepo(), "APPROVED", "APPROVED_BY_HUMAN");
        commentDispatcher.dispatchGateStatusUpdate(state.getRepo(), state.getPrNumber(), "APPROVED", state.getApprover(), state.getApprovalNotes());

        return state;
    }

    /**
     * Rejects a held deployment gate.
     */
    public PipelineState reject(PipelineState state, String approver, String reason) {
        if (state.getStatus() != PipelineState.PipelineStatus.GATING) {
            throw new IllegalStateException("Pipeline " + state.getPipelineId() + " is not awaiting approval. Current status: " + state.getStatus());
        }

        log.warn("🚫 Human Rejection received for pipeline {} by approver '{}': {}", state.getPipelineId(), approver, reason);
        state.status(PipelineState.PipelineStatus.BLOCKED);
        state.approver(approver != null && !approver.isBlank() ? approver : "anonymous-engineer");
        state.approvalNotes(reason != null ? reason : "Deployment rejected by engineer");
        state.setDeploymentDecision("REJECTED_BY_HUMAN");
        state.addError("Deployment rejected by " + state.getApprover() + ": " + state.getApprovalNotes());

        feedbackProducer.sendDeploymentGateEvent(state.getPipelineId(), state.getRepo(), "BLOCKED", "REJECTED_BY_HUMAN");
        commentDispatcher.dispatchGateStatusUpdate(state.getRepo(), state.getPrNumber(), "REJECTED", state.getApprover(), state.getApprovalNotes());

        return state;
    }
}
