package com.cicd.orchestrator.nodes;

import com.cicd.orchestrator.model.PipelineState;
import org.bsc.langgraph4j.action.NodeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class TestRunnerNode implements NodeAction<PipelineState> {
    private static final Logger log = LoggerFactory.getLogger(TestRunnerNode.class);

    @Override
    public PipelineState apply(PipelineState state) throws Exception {
        log.info("Node: TestRunner | Processing pipeline: {}", state.getPipelineId());
        long start = System.currentTimeMillis();
        state.currentNode("TestRunner");
        state.status(PipelineState.PipelineStatus.TESTING);

        // Simulated test execution
        Map<String, Object> results = Map.of(
            "totalTests", 150,
            "passed", 150,
            "failed", 0,
            "coverage", 85.5
        );

        state.setTestResults(results);
        state.recordNodeTiming("TestRunner", System.currentTimeMillis() - start);
        return state;
    }
}
