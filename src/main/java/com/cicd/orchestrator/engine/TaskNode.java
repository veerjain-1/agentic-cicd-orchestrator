package com.cicd.orchestrator.engine;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A node in the execution DAG wrapping a specific task.
 */
public class TaskNode {
    private final String id;
    private final Task task;
    private final Set<String> dependencies;

    public TaskNode(String id, Task task) {
        this.id = id;
        this.task = task;
        this.dependencies = new HashSet<>();
    }

    public TaskNode dependsOn(String predecessorId) {
        this.dependencies.add(predecessorId);
        return this;
    }

    public String getId() {
        return id;
    }

    public Task getTask() {
        return task;
    }

    public Set<String> getDependencies() {
        return Collections.unmodifiableSet(dependencies);
    }
}
