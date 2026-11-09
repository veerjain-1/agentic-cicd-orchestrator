package com.cicd.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Publishes structured pipeline execution events to Kafka topics ('pipeline-events' and 'feedback-loop').
 * Enables decoupled observability, real-time Slack/Discord bot alerts, and downstream workflow integration.
 */
@Service
public class PipelineFeedbackProducer {
    private static final Logger log = LoggerFactory.getLogger(PipelineFeedbackProducer.class);

    public static final String TOPIC_PIPELINE_EVENTS = "pipeline-events";
    public static final String TOPIC_FEEDBACK_LOOP = "feedback-loop";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public PipelineFeedbackProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendPipelineStarted(String pipelineId, String repo, String commitSha, String branch) {
        Map<String, Object> payload = basePayload("PIPELINE_STARTED", pipelineId, repo);
        payload.put("commitSha", commitSha);
        payload.put("branch", branch);
        send(TOPIC_PIPELINE_EVENTS, pipelineId, payload);
    }

    public void sendDependencyValidationEvent(String pipelineId, String repo, boolean passed, int vulnerabilitiesFound) {
        Map<String, Object> payload = basePayload(passed ? "DEPENDENCY_CHECK_PASSED" : "DEPENDENCY_CHECK_FAILED", pipelineId, repo);
        payload.put("vulnerabilitiesFound", vulnerabilitiesFound);
        send(TOPIC_PIPELINE_EVENTS, pipelineId, payload);

        if (!passed) {
            // Also alert on feedback-loop for auto-remediation triggers
            send(TOPIC_FEEDBACK_LOOP, pipelineId, payload);
        }
    }

    public void sendSecurityAuditEvent(String pipelineId, String repo, String riskScore, int findingsCount) {
        Map<String, Object> payload = basePayload("SECURITY_AUDIT_COMPLETED", pipelineId, repo);
        payload.put("riskScore", riskScore);
        payload.put("findingsCount", findingsCount);
        send(TOPIC_PIPELINE_EVENTS, pipelineId, payload);

        if ("HIGH".equalsIgnoreCase(riskScore) || "CRITICAL".equalsIgnoreCase(riskScore)) {
            send(TOPIC_FEEDBACK_LOOP, pipelineId, payload);
        }
    }

    public void sendDeploymentGateEvent(String pipelineId, String repo, String gateStatus, String decision) {
        Map<String, Object> payload = basePayload("DEPLOYMENT_GATE_" + gateStatus.toUpperCase(), pipelineId, repo);
        payload.put("gateStatus", gateStatus);
        payload.put("decision", decision);
        send(TOPIC_PIPELINE_EVENTS, pipelineId, payload);

        if ("AWAITING_APPROVAL".equalsIgnoreCase(decision) || "BLOCKED".equalsIgnoreCase(gateStatus)) {
            send(TOPIC_FEEDBACK_LOOP, pipelineId, payload);
        }
    }

    public void sendPipelineFinished(String pipelineId, String repo, boolean success, long durationMs) {
        Map<String, Object> payload = basePayload(success ? "PIPELINE_COMPLETED" : "PIPELINE_FAILED", pipelineId, repo);
        payload.put("durationMs", durationMs);
        payload.put("status", success ? "SUCCESS" : "FAILURE");
        send(TOPIC_PIPELINE_EVENTS, pipelineId, payload);
    }

    private Map<String, Object> basePayload(String eventType, String pipelineId, String repo) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("eventType", eventType);
        payload.put("pipelineId", pipelineId);
        payload.put("repo", repo);
        payload.put("timestamp", Instant.now().toString());
        return payload;
    }

    private void send(String topic, String key, Map<String, Object> payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            kafkaTemplate.send(topic, key, json);
            log.debug("Sent event [{}] to Kafka topic [{}]", payload.get("eventType"), topic);
        } catch (Exception e) {
            log.error("Failed to serialize or send Kafka event for pipeline {}", key, e);
        }
    }
}
