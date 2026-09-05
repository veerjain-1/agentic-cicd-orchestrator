package com.cicd.orchestrator.tasks;

import com.cicd.orchestrator.engine.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MavenBuildTask implements Task {
    private static final Logger log = LoggerFactory.getLogger(MavenBuildTask.class);

    @Override
    public boolean execute(Map<String, Object> context) {
        String workspace = (String) context.get("workspacePath");
        if (workspace == null) {
            log.error("No workspace path found in context. Did the clone step fail?");
            return false;
        }
        
        log.info("Running `mvn clean compile` in {}...", workspace);
        try {
            // Simulate build time
            Thread.sleep(1500);
            log.info("Build completed successfully.");
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
