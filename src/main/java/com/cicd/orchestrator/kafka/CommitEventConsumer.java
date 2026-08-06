package com.cicd.orchestrator.kafka;

import com.cicd.orchestrator.model.CommitEvent;
import com.cicd.orchestrator.model.PipelineState;
import com.cicd.orchestrator.service.PipelineExecutionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class CommitEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(CommitEventConsumer.class);

    private final PipelineExecutionService executionService;
    private final ObjectMapper objectMapper;

    public CommitEventConsumer(PipelineExecutionService executionService, ObjectMapper objectMapper) {
        this.executionService = executionService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "commit-events", groupId = "cicd-orchestrator")
    public void onCommitEvent(String message) {
        try {
            CommitEvent event = objectMapper.readValue(message, CommitEvent.class);
            log.info("📥 Received commit event for repo: {}, sha: {}", event.getRepo(), event.getCommitSha());

            String pipelineId = UUID.randomUUID().toString();
            PipelineState state = new PipelineState(pipelineId, event.getRepo(), event.getBranch(), event.getCommitSha())
                    .author(event.getAuthor())
                    .commitMessage(event.getCommitMessage())
                    .prNumber(event.getPrNumber());

            // Start pipeline execution
            executionService.executePipeline(state);
            
        } catch (Exception e) {
            log.error("❌ Failed to process commit event", e);
        }
    }
}
