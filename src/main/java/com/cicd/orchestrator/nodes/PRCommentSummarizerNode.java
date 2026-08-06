package com.cicd.orchestrator.nodes;

import com.cicd.orchestrator.model.PipelineState;
import org.bsc.langgraph4j.action.NodeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class PRCommentSummarizerNode implements NodeAction<PipelineState> {
    private static final Logger log = LoggerFactory.getLogger(PRCommentSummarizerNode.class);

    @Override
    public PipelineState apply(PipelineState state) throws Exception {
        log.info("Node: PRCommentSummarizer | Processing pipeline: {}", state.getPipelineId());
        long start = System.currentTimeMillis();
        state.currentNode("PRCommentSummarizer");
        state.status(PipelineState.PipelineStatus.COMMENTING);

        // Simulated AI PR Comment generation
        String comment = "### 🤖 AI Pipeline Summary\n" +
                         "✅ **Dependencies**: Validated.\n" +
                         "✅ **Build**: Successful in " + (state.getBuildResults().isEmpty() ? 0 : state.getBuildResults().get(0).getDurationMs()) + "ms.\n" +
                         "✅ **Tests**: Passed (" + (state.getTestResults() != null ? state.getTestResults().get("passed") : 0) + " total).\n" +
                         "🔍 **Notes**: Ready for deployment gate review.";

        state.setPrCommentBody(comment);
        state.recordNodeTiming("PRCommentSummarizer", System.currentTimeMillis() - start);
        return state;
    }
}
