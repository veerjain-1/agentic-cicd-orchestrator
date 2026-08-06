package com.cicd.orchestrator.nodes;

import com.cicd.orchestrator.model.PipelineState;
import org.bsc.langgraph4j.action.NodeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class DependencyValidatorNode implements NodeAction<PipelineState> {
    private static final Logger log = LoggerFactory.getLogger(DependencyValidatorNode.class);

    @Override
    public PipelineState apply(PipelineState state) throws Exception {
        log.info("Node: DependencyValidator | Processing pipeline: {}", state.getPipelineId());
        long start = System.currentTimeMillis();
        state.currentNode("DependencyValidator");
        state.status(PipelineState.PipelineStatus.VALIDATING_DEPS);

        // Simulated dependency validation
        Map<String, Object> deps = Map.of(
            "vulnerabilitiesFound", 0,
            "outdatedPackages", 2,
            "status", "PASS"
        );

        state.setDependencyReport(deps);
        state.recordNodeTiming("DependencyValidator", System.currentTimeMillis() - start);
        return state;
    }
}
