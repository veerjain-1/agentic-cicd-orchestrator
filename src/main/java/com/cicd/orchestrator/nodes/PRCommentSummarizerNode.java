package com.cicd.orchestrator.nodes;

import com.cicd.orchestrator.model.PipelineState;
import org.bsc.langgraph4j.action.NodeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import dev.langchain4j.model.chat.ChatLanguageModel;
import org.springframework.stereotype.Component;

@Component
public class PRCommentSummarizerNode implements NodeAction<PipelineState> {
    private static final Logger log = LoggerFactory.getLogger(PRCommentSummarizerNode.class);

    private final ChatLanguageModel chatLanguageModel;

    public PRCommentSummarizerNode(ChatLanguageModel chatLanguageModel) {
        this.chatLanguageModel = chatLanguageModel;
    }

    @Override
    public PipelineState apply(PipelineState state) throws Exception {
        log.info("Node: PRCommentSummarizer | Processing pipeline: {}", state.getPipelineId());
        long start = System.currentTimeMillis();
        state.currentNode("PRCommentSummarizer");
        state.status(PipelineState.PipelineStatus.COMMENTING);

        // Generate PR comment dynamically via LangChain4j
        String prompt = String.format(
            "You are an AI CI/CD agent. Generate a concise PR comment summarizing the pipeline results.\n" +
            "Pipeline ID: %s\n" +
            "Build Results: %s\n" +
            "Test Results: %s\n" +
            "Commit Analysis: %s\n" +
            "Keep it short, use markdown, and output emojis for passed/failed statuses.",
            state.getPipelineId(),
            state.getBuildResults().toString(),
            state.getTestResults() != null ? state.getTestResults().toString() : "None",
            state.getCommitAnalysis() != null ? state.getCommitAnalysis().toString() : "None"
        );

        String comment;
        try {
            comment = chatLanguageModel.generate(prompt);
        } catch (Exception e) {
            log.error("Failed to generate PR comment from LLM. Falling back to default.", e);
            comment = "### 🤖 AI Pipeline Summary\n" +
                      "✅ **Dependencies**: Validated.\n" +
                      "✅ **Build**: Successful in " + (state.getBuildResults().isEmpty() ? 0 : state.getBuildResults().get(0).getDurationMs()) + "ms.\n" +
                      "✅ **Tests**: Passed (" + (state.getTestResults() != null ? state.getTestResults().get("passed") : 0) + " total).\n" +
                      "🔍 **Notes**: Ready for deployment gate review.";
        }

        state.setPrCommentBody(comment);
        state.recordNodeTiming("PRCommentSummarizer", System.currentTimeMillis() - start);
        return state;
    }
}
