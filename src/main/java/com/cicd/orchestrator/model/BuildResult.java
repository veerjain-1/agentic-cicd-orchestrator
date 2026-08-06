package com.cicd.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BuildResult {
    @JsonProperty("stage")
    private String stage;

    @JsonProperty("status")
    private String status; // SUCCESS, FAILED, RUNNING

    @JsonProperty("durationMs")
    private long durationMs;

    @JsonProperty("logs")
    private String logs;

    @JsonProperty("timestamp")
    private Instant timestamp = Instant.now();

    public BuildResult() {}

    public BuildResult(String stage, String status, long durationMs, String logs) {
        this.stage = stage;
        this.status = status;
        this.durationMs = durationMs;
        this.logs = logs;
    }

    // Getters and Setters
    public String getStage() { return stage; }
    public void setStage(String stage) { this.stage = stage; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public long getDurationMs() { return durationMs; }
    public void setDurationMs(long durationMs) { this.durationMs = durationMs; }
    public String getLogs() { return logs; }
    public void setLogs(String logs) { this.logs = logs; }
    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
}
