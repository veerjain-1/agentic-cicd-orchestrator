package com.cicd.orchestrator.nodes;

import com.cicd.orchestrator.model.BuildResult;
import com.cicd.orchestrator.model.PipelineState;
import org.bsc.langgraph4j.action.NodeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class BuildOrchestratorNode implements NodeAction<PipelineState> {
    private static final Logger log = LoggerFactory.getLogger(BuildOrchestratorNode.class);

    @Override
    public PipelineState apply(PipelineState state) throws Exception {
        log.info("Node: BuildOrchestrator | Processing pipeline: {}", state.getPipelineId());
        long start = System.currentTimeMillis();
        state.currentNode("BuildOrchestrator");
        state.status(PipelineState.PipelineStatus.BUILDING);

        // Simulated build orchestration
        List<BuildResult> results = List.of(
            new BuildResult("compile", "SUCCESS", 1200, "Compilation successful"),
            new BuildResult("package", "SUCCESS", 800, "Packaging successful")
        );

        state.setBuildResults(results);
        state.recordNodeTiming("BuildOrchestrator", System.currentTimeMillis() - start);
        return state;
    }
}
