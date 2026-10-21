package com.cicd.orchestrator.tasks;

import com.cicd.orchestrator.engine.Task;
import com.cicd.orchestrator.service.LLMAnalysisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Task executing recursive LLM code analysis agent over commit diffs.
 * Generates automated PR reviews, security vulnerability flags, and summary comments.
 */
public class CodeAnalysisAgentTask implements Task {
    private static final Logger log = LoggerFactory.getLogger(CodeAnalysisAgentTask.class);

    private final LLMAnalysisService analysisService;

    public CodeAnalysisAgentTask(LLMAnalysisService analysisService) {
        this.analysisService = analysisService;
    }

    @Override
    public boolean execute(Map<String, Object> context) {
        String repo = (String) context.getOrDefault("repo", "unknown-repo");
        String commit = (String) context.getOrDefault("commit", "HEAD");
        String diff = (String) context.getOrDefault("diff", "");

        log.info("🧠 Executing Recursive LLM Code Analysis Agent for {} at {}", repo, commit);

        try {
            Map<String, Object> analysis = analysisService.analyzeCode(repo, commit, diff);
            context.put("commitAnalysis", analysis);

            @SuppressWarnings("unchecked")
            Map<String, Object> dependencyReport = (Map<String, Object>) context.get("dependencyReport");
            String prComment = analysisService.generatePrSummary(analysis, dependencyReport);
            context.put("prCommentBody", prComment);

            boolean passed = (Boolean) analysis.getOrDefault("passed", true);
            log.info("Agent analysis completed. Risk: {}, Passed: {}", analysis.get("riskScore"), passed);
            return passed;
        } catch (Exception e) {
            log.error("Failed to execute LLM code analysis agent: {}", e.getMessage(), e);
            return false;
        }
    }
}
