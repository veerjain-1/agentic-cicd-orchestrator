package com.cicd.orchestrator.tasks;

import com.cicd.orchestrator.engine.Task;
import com.cicd.orchestrator.model.PipelineState;
import com.cicd.orchestrator.service.DeploymentGateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Task executing the Deployment Gate evaluation.
 * Decides whether artifacts can proceed to deployment, must wait for Human-in-the-Loop approval, or are blocked.
 */
public class DeploymentGateTask implements Task {
    private static final Logger log = LoggerFactory.getLogger(DeploymentGateTask.class);

    private final DeploymentGateService deploymentGateService;

    public DeploymentGateTask(DeploymentGateService deploymentGateService) {
        this.deploymentGateService = deploymentGateService;
    }

    @Override
    public boolean execute(Map<String, Object> context) {
        String repo = (String) context.getOrDefault("repo", "unknown-repo");
        String commit = (String) context.getOrDefault("commit", "HEAD");
        String env = (String) context.getOrDefault("environment", "development");

        PipelineState state = new PipelineState("task-eval", repo, "main", commit);
        state.environment(env);

        if (context.containsKey("requiresApproval")) {
            state.requiresApproval(Boolean.TRUE.equals(context.get("requiresApproval")));
        }

        if (context.containsKey("prNumber")) {
            Object prObj = context.get("prNumber");
            if (prObj instanceof Integer) {
                state.prNumber((Integer) prObj);
            }
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> commitAnalysis = (Map<String, Object>) context.get("commitAnalysis");
        state.setCommitAnalysis(commitAnalysis);

        @SuppressWarnings("unchecked")
        Map<String, Object> dependencyReport = (Map<String, Object>) context.get("dependencyReport");
        state.setDependencyReport(dependencyReport);

        log.info("🚪 Evaluating Deployment Gate for {} at {} (environment: {})", repo, commit, env);

        boolean passed = deploymentGateService.evaluateGate(state);

        context.put("deploymentDecision", state.getDeploymentDecision());
        context.put("deploymentStatus", state.getStatus().name());

        return passed;
    }
}
