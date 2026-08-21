package com.cicd.orchestrator.nodes;

import com.cicd.orchestrator.model.PipelineState;
import org.bsc.langgraph4j.action.NodeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import dev.langchain4j.model.chat.ChatLanguageModel;

import java.util.Map;

@Component
public class CommitAnalyzerNode implements NodeAction<PipelineState> {
    private static final Logger log = LoggerFactory.getLogger(CommitAnalyzerNode.class);

    private final ChatLanguageModel chatLanguageModel;
    private final ObjectMapper objectMapper;

    public CommitAnalyzerNode(ChatLanguageModel chatLanguageModel, ObjectMapper objectMapper) {
        this.chatLanguageModel = chatLanguageModel;
        this.objectMapper = objectMapper;
    }

    @Override
    public PipelineState apply(PipelineState state) throws Exception {
        log.info("Node: CommitAnalyzer | Processing pipeline: {}", state.getPipelineId());
        long start = System.currentTimeMillis();
        state.currentNode("CommitAnalyzer");
        state.status(PipelineState.PipelineStatus.ANALYZING);

        // Use LangChain4j to analyze the commit
        String prompt = String.format(
            "Analyze the following git commit and output a JSON object containing 'complexity' (low/medium/high), " +
            "'riskLevel' (low/medium/high), 'filesAffected' (integer), and 'requiresDatabaseMigration' (boolean).\n" +
            "Commit Message: %s\n" +
            "Only return the raw JSON object, no markdown or markdown code blocks.",
            state.getCommitMessage() != null ? state.getCommitMessage() : "No commit message provided"
        );

        Map<String, Object> analysis = Map.of(
            "complexity", "medium",
            "riskLevel", "low",
            "filesAffected", 5,
            "requiresDatabaseMigration", false
        );
        
        try {
            String response = chatLanguageModel.generate(prompt);
            analysis = objectMapper.readValue(response, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            log.error("Failed to parse LLM response or communicate with LLM. Falling back to default analysis.", e);
        }

        state.setCommitAnalysis(analysis);
        state.recordNodeTiming("CommitAnalyzer", System.currentTimeMillis() - start);
        log.info("Commit Analysis complete for {}", state.getPipelineId());
        return state;
    }
}
