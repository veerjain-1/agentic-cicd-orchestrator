package com.cicd.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.*;

/**
 * Represents the full state of a CI/CD pipeline as it flows through the LangGraph.
 * This is the AgentState that persists across all graph nodes.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PipelineState {

    @JsonProperty("pipelineId")
    private String pipelineId;

    @JsonProperty("repo")
    private String repo;

    @JsonProperty("branch")
    private String branch;

    @JsonProperty("commitSha")
    private String commitSha;

    @JsonProperty("commitMessage")
    private String commitMessage;

    @JsonProperty("author")
    private String author;

    @JsonProperty("prNumber")
    private Integer prNumber;

    @JsonProperty("status")
    private PipelineStatus status = PipelineStatus.PENDING;

    @JsonProperty("currentNode")
    private String currentNode;

    @JsonProperty("environment")
    private String environment = "development";

    // ─── Node Results ───
    @JsonProperty("commitAnalysis")
    private Map<String, Object> commitAnalysis;

    @JsonProperty("dependencyReport")
    private Map<String, Object> dependencyReport;

    @JsonProperty("buildResults")
    private List<BuildResult> buildResults = new ArrayList<>();

    @JsonProperty("testResults")
    private Map<String, Object> testResults;

    @JsonProperty("prCommentBody")
    private String prCommentBody;

    @JsonProperty("deploymentDecision")
    private String deploymentDecision;

    // ─── Control Flow ───
    @JsonProperty("retryCount")
    private int retryCount = 0;

    @JsonProperty("maxRetries")
    private int maxRetries = 3;

    @JsonProperty("errors")
    private List<String> errors = new ArrayList<>();

    @JsonProperty("warnings")
    private List<String> warnings = new ArrayList<>();

    // ─── Timestamps ───
    @JsonProperty("createdAt")
    private Instant createdAt = Instant.now();

    @JsonProperty("updatedAt")
    private Instant updatedAt = Instant.now();

    @JsonProperty("completedAt")
    private Instant completedAt;

    // ─── Metadata for observability ───
    @JsonProperty("nodeTimings")
    private Map<String, Long> nodeTimings = new LinkedHashMap<>();

    public enum PipelineStatus {
        PENDING, ANALYZING, VALIDATING_DEPS, BUILDING, TESTING,
        COMMENTING, GATING, APPROVED, BLOCKED, FAILED, COMPLETED
    }

    // Constructors
    public PipelineState() {}

    public PipelineState(String pipelineId, String repo, String branch, String commitSha) {
        this.pipelineId = pipelineId;
        this.repo = repo;
        this.branch = branch;
        this.commitSha = commitSha;
    }

    // Fluent setters
    public PipelineState pipelineId(String id) { this.pipelineId = id; return this; }
    public PipelineState repo(String repo) { this.repo = repo; return this; }
    public PipelineState branch(String branch) { this.branch = branch; return this; }
    public PipelineState commitSha(String sha) { this.commitSha = sha; return this; }
    public PipelineState commitMessage(String msg) { this.commitMessage = msg; return this; }
    public PipelineState author(String author) { this.author = author; return this; }
    public PipelineState prNumber(Integer pr) { this.prNumber = pr; return this; }
    public PipelineState status(PipelineStatus s) { this.status = s; this.updatedAt = Instant.now(); return this; }
    public PipelineState currentNode(String node) { this.currentNode = node; this.updatedAt = Instant.now(); return this; }
    public PipelineState environment(String env) { this.environment = env; return this; }

    public void addError(String error) { this.errors.add(error); }
    public void addWarning(String warning) { this.warnings.add(warning); }
    public void recordNodeTiming(String node, long durationMs) { this.nodeTimings.put(node, durationMs); }
    public void incrementRetry() { this.retryCount++; }
    public boolean canRetry() { return this.retryCount < this.maxRetries; }

    // Getters
    public String getPipelineId() { return pipelineId; }
    public String getRepo() { return repo; }
    public String getBranch() { return branch; }
    public String getCommitSha() { return commitSha; }
    public String getCommitMessage() { return commitMessage; }
    public String getAuthor() { return author; }
    public Integer getPrNumber() { return prNumber; }
    public PipelineStatus getStatus() { return status; }
    public String getCurrentNode() { return currentNode; }
    public String getEnvironment() { return environment; }
    public Map<String, Object> getCommitAnalysis() { return commitAnalysis; }
    public Map<String, Object> getDependencyReport() { return dependencyReport; }
    public List<BuildResult> getBuildResults() { return buildResults; }
    public Map<String, Object> getTestResults() { return testResults; }
    public String getPrCommentBody() { return prCommentBody; }
    public String getDeploymentDecision() { return deploymentDecision; }
    public int getRetryCount() { return retryCount; }
    public List<String> getErrors() { return errors; }
    public List<String> getWarnings() { return warnings; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public Instant getCompletedAt() { return completedAt; }
    public Map<String, Long> getNodeTimings() { return nodeTimings; }

    // Setters for Jackson
    public void setCommitAnalysis(Map<String, Object> ca) { this.commitAnalysis = ca; }
    public void setDependencyReport(Map<String, Object> dr) { this.dependencyReport = dr; }
    public void setBuildResults(List<BuildResult> br) { this.buildResults = br; }
    public void setTestResults(Map<String, Object> tr) { this.testResults = tr; }
    public void setPrCommentBody(String body) { this.prCommentBody = body; }
    public void setDeploymentDecision(String d) { this.deploymentDecision = d; }
    public void setCompletedAt(Instant t) { this.completedAt = t; }
}
