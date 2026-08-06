package com.cicd.orchestrator.nodes;

import com.cicd.orchestrator.model.PipelineState;
import org.bsc.langgraph4j.action.NodeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class CommitAnalyzerNode implements NodeAction<PipelineState> {
    private static final Logger log = LoggerFactory.getLogger(CommitAnalyzerNode.class);

    @Override
    public PipelineState apply(PipelineState state) throws Exception {
        log.info("Node: CommitAnalyzer | Processing pipeline: {}", state.getPipelineId());
        long start = System.currentTimeMillis();
        state.currentNode("CommitAnalyzer");
        state.status(PipelineState.PipelineStatus.ANALYZING);

        // Simulated AI analysis of the commit
        // In a real scenario, we'd use LangChain4j here to call the LLM
        Map<String, Object> analysis = Map.of(
            "complexity", "medium",
            "riskLevel", "low",
            "filesAffected", 5,
            "requiresDatabaseMigration", false
        );

        state.setCommitAnalysis(analysis);
        state.recordNodeTiming("CommitAnalyzer", System.currentTimeMillis() - start);
        log.info("Commit Analysis complete for {}", state.getPipelineId());
        return state;
    }
}
