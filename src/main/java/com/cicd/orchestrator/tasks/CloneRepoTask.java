package com.cicd.orchestrator.tasks;

import com.cicd.orchestrator.engine.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CloneRepoTask implements Task {
    private static final Logger log = LoggerFactory.getLogger(CloneRepoTask.class);

    @Override
    public boolean execute(Map<String, Object> context) {
        String repo = (String) context.getOrDefault("repo", "unknown-repo");
        log.info("Cloning repository: {}...", repo);
        try {
            // Simulate network latency
            Thread.sleep(1000);
            context.put("workspacePath", "/tmp/workspace/" + repo);
            log.info("Successfully cloned {}", repo);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
