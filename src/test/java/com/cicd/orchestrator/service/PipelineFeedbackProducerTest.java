package com.cicd.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PipelineFeedbackProducerTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper objectMapper;
    private PipelineFeedbackProducer feedbackProducer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        feedbackProducer = new PipelineFeedbackProducer(kafkaTemplate, objectMapper);
    }

    @Test
    void testSendPipelineStarted() throws Exception {
        feedbackProducer.sendPipelineStarted("pipe-101", "org/repo", "sha-abc", "main");

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("pipeline-events"), eq("pipe-101"), captor.capture());

        String json = captor.getValue();
        Map<?, ?> map = objectMapper.readValue(json, Map.class);
        assertEquals("PIPELINE_STARTED", map.get("eventType"));
        assertEquals("pipe-101", map.get("pipelineId"));
        assertEquals("org/repo", map.get("repo"));
        assertEquals("sha-abc", map.get("commitSha"));
        assertEquals("main", map.get("branch"));
    }

    @Test
    void testSendDependencyValidationFailedAlertsFeedbackLoop() throws Exception {
        feedbackProducer.sendDependencyValidationEvent("pipe-102", "org/repo", false, 3);

        verify(kafkaTemplate).send(eq("pipeline-events"), eq("pipe-102"), anyString());
        // Since it failed, feedback-loop should also receive the event
        verify(kafkaTemplate).send(eq("feedback-loop"), eq("pipe-102"), anyString());
    }

    @Test
    void testSendSecurityAuditCriticalAlertsFeedbackLoop() throws Exception {
        feedbackProducer.sendSecurityAuditEvent("pipe-103", "org/repo", "CRITICAL", 2);

        verify(kafkaTemplate).send(eq("pipeline-events"), eq("pipe-103"), anyString());
        verify(kafkaTemplate).send(eq("feedback-loop"), eq("pipe-103"), anyString());
    }

    @Test
    void testSendPipelineFinished() throws Exception {
        feedbackProducer.sendPipelineFinished("pipe-104", "org/repo", true, 1250);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("pipeline-events"), eq("pipe-104"), captor.capture());

        Map<?, ?> map = objectMapper.readValue(captor.getValue(), Map.class);
        assertEquals("PIPELINE_COMPLETED", map.get("eventType"));
        assertEquals("SUCCESS", map.get("status"));
        assertEquals(1250, map.get("durationMs"));
    }
}
