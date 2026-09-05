package com.cicd.orchestrator.tasks;

import com.cicd.orchestrator.engine.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RunTestsTask implements Task {
    private static final Logger log = LoggerFactory.getLogger(RunTestsTask.class);

    @Override
    public boolean execute(Map<String, Object> context) {
        log.info("Running unit and integration tests...");
        try {
            // Simulate test execution
            Thread.sleep(2000);
            context.put("testCoverage", 85.5);
            log.info("All tests passed! Coverage: 85.5%");
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
