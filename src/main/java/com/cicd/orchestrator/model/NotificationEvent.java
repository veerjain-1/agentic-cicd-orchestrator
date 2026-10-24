package com.cicd.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a notification event dispatched to external channels (Slack, webhooks, etc.).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NotificationEvent {

    public enum Severity {
        INFO, WARNING, CRITICAL
    }

    public enum Channel {
        SLACK, WEBHOOK, ALL
    }

    @JsonProperty("eventId")
    private String eventId;

    @JsonProperty("pipelineId")
    private String pipelineId;

    @JsonProperty("repo")
    private String repo;

    @JsonProperty("title")
    private String title;

    @JsonProperty("message")
    private String message;

    @JsonProperty("severity")
    private Severity severity;

    @JsonProperty("channel")
    private Channel channel;

    @JsonProperty("timestamp")
    private Instant timestamp;

    @JsonProperty("metadata")
    private Map<String, String> metadata;

    @JsonProperty("delivered")
    private boolean delivered;

    @JsonProperty("deliveryError")
    private String deliveryError;

    public NotificationEvent() {
        this.timestamp = Instant.now();
        this.metadata = new HashMap<>();
        this.delivered = false;
    }

    // Builder pattern
    public static NotificationEvent create(String eventId) {
        NotificationEvent event = new NotificationEvent();
        event.eventId = eventId;
        return event;
    }

    public NotificationEvent pipelineId(String pid) { this.pipelineId = pid; return this; }
    public NotificationEvent repo(String repo) { this.repo = repo; return this; }
    public NotificationEvent title(String title) { this.title = title; return this; }
    public NotificationEvent message(String msg) { this.message = msg; return this; }
    public NotificationEvent severity(Severity sev) { this.severity = sev; return this; }
    public NotificationEvent channel(Channel ch) { this.channel = ch; return this; }
    public NotificationEvent addMeta(String key, String value) { this.metadata.put(key, value); return this; }

    public NotificationEvent markDelivered() {
        this.delivered = true;
        return this;
    }

    public NotificationEvent markFailed(String error) {
        this.delivered = false;
        this.deliveryError = error;
        return this;
    }

    // Getters
    public String getEventId() { return eventId; }
    public String getPipelineId() { return pipelineId; }
    public String getRepo() { return repo; }
    public String getTitle() { return title; }
    public String getMessage() { return message; }
    public Severity getSeverity() { return severity; }
    public Channel getChannel() { return channel; }
    public Instant getTimestamp() { return timestamp; }
    public Map<String, String> getMetadata() { return metadata; }
    public boolean isDelivered() { return delivered; }
    public String getDeliveryError() { return deliveryError; }

    // Setters for Jackson
    public void setEventId(String eventId) { this.eventId = eventId; }
    public void setPipelineId(String pipelineId) { this.pipelineId = pipelineId; }
    public void setRepo(String repo) { this.repo = repo; }
    public void setTitle(String title) { this.title = title; }
    public void setMessage(String message) { this.message = message; }
    public void setSeverity(Severity severity) { this.severity = severity; }
    public void setChannel(Channel channel) { this.channel = channel; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
    public void setMetadata(Map<String, String> metadata) { this.metadata = metadata; }
    public void setDelivered(boolean delivered) { this.delivered = delivered; }
    public void setDeliveryError(String deliveryError) { this.deliveryError = deliveryError; }
}
