package com.cicd.orchestrator.engine;

import java.util.Map;

/**
 * Represents a discrete unit of execution within the CI/CD pipeline DAG.
 */
@FunctionalInterface
public interface Task {
    /**
     * Executes the task.
     * @param context the execution context containing state from previous steps
     * @return true if successful, false otherwise
     */
    boolean execute(Map<String, Object> context);
}
