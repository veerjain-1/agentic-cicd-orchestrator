package com.cicd.orchestrator.nodes;

import com.cicd.orchestrator.model.PipelineState;
import org.bsc.langgraph4j.action.NodeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DeploymentGateNode implements NodeAction<PipelineState> {
    private static final Logger log = LoggerFactory.getLogger(DeploymentGateNode.class);

    @Override
    public PipelineState apply(PipelineState state) throws Exception {
        log.info("Node: DeploymentGate | Processing pipeline: {}", state.getPipelineId());
        long start = System.currentTimeMillis();
        state.currentNode("DeploymentGate");
        state.status(PipelineState.PipelineStatus.GATING);

        // Simple mock gating logic
        boolean shouldDeploy = state.getTestResults() != null &&
                               (int)state.getTestResults().get("failed") == 0;

        if (shouldDeploy) {
            state.setDeploymentDecision("APPROVED");
            state.status(PipelineState.PipelineStatus.APPROVED);
        } else {
            state.setDeploymentDecision("BLOCKED");
            state.status(PipelineState.PipelineStatus.BLOCKED);
        }

        state.recordNodeTiming("DeploymentGate", System.currentTimeMillis() - start);
        return state;
    }
}
