package com.cicd.orchestrator.tasks;

import com.cicd.orchestrator.engine.Task;
import com.cicd.orchestrator.service.GitHubCommentDispatcherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Task to dispatch the generated AI PR review summary to GitHub.
 */
public class PRCommentDispatcherTask implements Task {
    private static final Logger log = LoggerFactory.getLogger(PRCommentDispatcherTask.class);

    private final GitHubCommentDispatcherService commentDispatcherService;

    public PRCommentDispatcherTask(GitHubCommentDispatcherService commentDispatcherService) {
        this.commentDispatcherService = commentDispatcherService;
    }

    @Override
    public boolean execute(Map<String, Object> context) {
        String repo = (String) context.getOrDefault("repo", "unknown-repo");
        String prCommentBody = (String) context.get("prCommentBody");
        Object prObj = context.get("prNumber");

        Integer prNumber = null;
        if (prObj instanceof Integer) {
            prNumber = (Integer) prObj;
        } else if (prObj instanceof String str && !str.isBlank()) {
            try {
                prNumber = Integer.parseInt(str);
            } catch (NumberFormatException ignored) {}
        }

        if (prCommentBody == null || prCommentBody.isBlank()) {
            log.info("No PR review comment found in context to dispatch for {}", repo);
            return true;
        }

        if (prNumber == null) {
            log.info("Pipeline run for {} is not tied to a PR. Skipping comment dispatch.", repo);
            return true;
        }

        log.info("📤 Dispatching review comment to GitHub PR {} #{}", repo, prNumber);
        boolean dispatched = commentDispatcherService.dispatchPrComment(repo, prNumber, prCommentBody);
        context.put("prCommentDispatched", dispatched);
        return true;
    }
}
