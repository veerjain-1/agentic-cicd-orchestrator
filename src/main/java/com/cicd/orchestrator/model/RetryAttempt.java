package com.cicd.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a single retry attempt record for audit and observability.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RetryAttempt {

    @JsonProperty("attemptNumber")
    private int attemptNumber;

    @JsonProperty("taskId")
    private String taskId;

    @JsonProperty("pipelineId")
    private String pipelineId;

    @JsonProperty("startedAt")
    private Instant startedAt;

    @JsonProperty("completedAt")
    private Instant completedAt;

    @JsonProperty("success")
    private boolean success;

    @JsonProperty("errorMessage")
    private String errorMessage;

    @JsonProperty("delayMs")
    private long delayMs;

    public RetryAttempt() {}

    public RetryAttempt(int attemptNumber, String taskId, String pipelineId) {
        this.attemptNumber = attemptNumber;
        this.taskId = taskId;
        this.pipelineId = pipelineId;
        this.startedAt = Instant.now();
    }

    public RetryAttempt complete(boolean success, String errorMessage) {
        this.success = success;
        this.errorMessage = errorMessage;
        this.completedAt = Instant.now();
        return this;
    }

    // Getters and Setters
    public int getAttemptNumber() { return attemptNumber; }
    public void setAttemptNumber(int attemptNumber) { this.attemptNumber = attemptNumber; }
    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public String getPipelineId() { return pipelineId; }
    public void setPipelineId(String pipelineId) { this.pipelineId = pipelineId; }
    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public Instant getCompletedAt() { return completedAt; }
    public void setCompletedAt(Instant completedAt) { this.completedAt = completedAt; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public long getDelayMs() { return delayMs; }
    public void setDelayMs(long delayMs) { this.delayMs = delayMs; }

    /**
     * Utility: build a history list from a series of retry attempts.
     */
    public static List<RetryAttempt> emptyHistory() {
        return new ArrayList<>();
    }
}
