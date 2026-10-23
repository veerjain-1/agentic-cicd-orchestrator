package com.cicd.orchestrator.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;

/**
 * Service to dispatch automated AI reviews and deployment gate notifications
 * directly to GitHub Pull Requests via GitHub's REST API.
 */
@Service
public class GitHubCommentDispatcherService {
    private static final Logger log = LoggerFactory.getLogger(GitHubCommentDispatcherService.class);

    private final String githubToken;
    private final WebClient webClient;

    @org.springframework.beans.factory.annotation.Autowired
    public GitHubCommentDispatcherService(
            @Value("${agent.github.token:}") String githubToken,
            org.springframework.beans.factory.ObjectProvider<WebClient.Builder> webClientBuilderProvider
    ) {
        this.githubToken = githubToken != null ? githubToken.trim() : "";
        WebClient.Builder builder = webClientBuilderProvider != null && webClientBuilderProvider.getIfAvailable() != null
                ? webClientBuilderProvider.getIfAvailable()
                : WebClient.builder();
        this.webClient = builder
                .baseUrl("https://api.github.com")
                .defaultHeader(HttpHeaders.ACCEPT, "application/vnd.github+json")
                .defaultHeader(HttpHeaders.USER_AGENT, "Agentic-CICD-Orchestrator")
                .build();
    }

    // Secondary constructor for testing with custom WebClient
    public GitHubCommentDispatcherService(String githubToken, WebClient webClient) {
        this.githubToken = githubToken != null ? githubToken.trim() : "";
        this.webClient = webClient;
    }

    /**
     * Posts a markdown review comment to a GitHub Pull Request.
     * Fallbacks to graceful dry-run simulation when no GitHub token is present.
     *
     * @param repo Repository in "owner/repo" format
     * @param prNumber PR number
     * @param commentBody Formatted markdown content
     * @return true if dispatched or simulated successfully, false on error
     */
    public boolean dispatchPrComment(String repo, Integer prNumber, String commentBody) {
        if (prNumber == null || prNumber <= 0) {
            log.info("No PR number provided for repo {}. Skipping PR comment dispatch.", repo);
            return false;
        }

        if (commentBody == null || commentBody.isBlank()) {
            log.warn("PR comment body is empty for repo {} #{}. Skipping dispatch.", repo, prNumber);
            return false;
        }

        if (githubToken.isEmpty()) {
            log.info("ℹ️ GitHub token not configured. Simulating dry-run PR comment dispatch for {} #{}", repo, prNumber);
            log.debug("Dry-run PR comment content:\n{}", commentBody);
            return true;
        }

        try {
            log.info("🚀 Dispatching automated PR comment to GitHub: {} #{}", repo, prNumber);

            webClient.post()
                    .uri("/repos/{repo}/issues/{prNumber}/comments", repo, prNumber)
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + githubToken)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(Map.of("body", commentBody))
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

            log.info("✅ Successfully posted PR comment to {} #{}", repo, prNumber);
            return true;
        } catch (Exception e) {
            log.error("❌ Failed to dispatch comment to GitHub PR {} #{}: {}", repo, prNumber, e.getMessage());
            return false;
        }
    }

    /**
     * Dispatches an update regarding Human-in-the-Loop deployment approval or rejection.
     */
    public boolean dispatchGateStatusUpdate(String repo, Integer prNumber, String status, String actor, String notes) {
        if (prNumber == null || prNumber <= 0) {
            return false;
        }

        String emoji = "APPROVED".equalsIgnoreCase(status) ? "🟢" : "🔴";
        StringBuilder sb = new StringBuilder();
        sb.append("### ").append(emoji).append(" Deployment Gate: **").append(status.toUpperCase()).append("**\n\n");
        sb.append("- **Actor:** `").append(actor != null ? actor : "system").append("`\n");
        if (notes != null && !notes.isBlank()) {
            sb.append("- **Notes/Reason:** ").append(notes).append("\n");
        }
        sb.append("\n*Automated notification from Agentic CI/CD Orchestrator Deployment Gate.*");

        return dispatchPrComment(repo, prNumber, sb.toString());
    }
}
