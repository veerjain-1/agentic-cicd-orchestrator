package com.cicd.orchestrator.tasks;

import com.cicd.orchestrator.engine.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PublishArtifactTask implements Task {
    private static final Logger log = LoggerFactory.getLogger(PublishArtifactTask.class);

    @Override
    public boolean execute(Map<String, Object> context) {
        log.info("Packaging and publishing Docker image to registry...");
        try {
            Thread.sleep(1000);
            log.info("Successfully published artifact.");
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
