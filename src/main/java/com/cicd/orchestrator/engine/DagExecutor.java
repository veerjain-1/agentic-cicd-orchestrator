package com.cicd.orchestrator.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * High-performance Directed Acyclic Graph (DAG) Execution Engine.
 * Utilizes Java 21 Virtual Threads to execute non-dependent nodes concurrently.
 */
public class DagExecutor {
    private static final Logger log = LoggerFactory.getLogger(DagExecutor.class);
    
    // Use Virtual Threads for lightweight, massive concurrency
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /**
     * Executes a DAG of tasks.
     * @param nodes List of TaskNodes that make up the graph
     * @param context Shared execution state
     * @return true if all tasks succeeded, false if any failed
     */
    public boolean execute(List<TaskNode> nodes, Map<String, Object> context) {
        Map<String, TaskNode> nodeMap = new HashMap<>();
        for (TaskNode node : nodes) {
            nodeMap.put(node.getId(), node);
        }

        // Validate DAG (ensure no missing dependencies and no cycles)
        validateDag(nodes, nodeMap);

        // Map to hold the Future for each task
        Map<String, CompletableFuture<Boolean>> futures = new HashMap<>();

        long startTime = System.currentTimeMillis();
        log.info("Starting DAG execution with {} nodes using Virtual Threads...", nodes.size());

        // Initialize futures for all nodes
        for (TaskNode node : nodes) {
            futures.put(node.getId(), new CompletableFuture<>());
        }

        // Trigger execution
        for (TaskNode node : nodes) {
            scheduleNode(node, nodeMap, futures, context);
        }

        // Wait for all futures to complete
        boolean overallSuccess = true;
        for (CompletableFuture<Boolean> future : futures.values()) {
            try {
                if (!future.get()) { // Wait for completion
                    overallSuccess = false;
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error during DAG execution", e);
                overallSuccess = false;
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        log.info("DAG execution finished in {} ms. Success: {}", duration, overallSuccess);
        
        return overallSuccess;
    }

    private void scheduleNode(TaskNode node, Map<String, TaskNode> nodeMap, 
                              Map<String, CompletableFuture<Boolean>> futures, 
                              Map<String, Object> context) {
        
        Set<String> deps = node.getDependencies();
        
        if (deps.isEmpty()) {
            // No dependencies, run immediately
            runTask(node, futures, context);
        } else {
            // Wait for all dependencies to complete successfully
            List<CompletableFuture<Boolean>> depFutures = new ArrayList<>();
            for (String depId : deps) {
                depFutures.add(futures.get(depId));
            }

            CompletableFuture<Void> allDeps = CompletableFuture.allOf(depFutures.toArray(new CompletableFuture[0]));
            
            allDeps.thenRunAsync(() -> {
                // Check if any dependency failed
                boolean depsSucceeded = true;
                for (CompletableFuture<Boolean> f : depFutures) {
                    try {
                        if (!f.get()) depsSucceeded = false;
                    } catch (Exception e) {
                        depsSucceeded = false;
                    }
                }

                if (depsSucceeded) {
                    runTask(node, futures, context);
                } else {
                    log.warn("Skipping Task [{}] due to dependency failure.", node.getId());
                    futures.get(node.getId()).complete(false);
                }
            }, executor);
        }
    }

    private void runTask(TaskNode node, Map<String, CompletableFuture<Boolean>> futures, Map<String, Object> context) {
        executor.submit(() -> {
            log.info("▶️ Executing Task [{}]", node.getId());
            boolean success = false;
            try {
                success = node.getTask().execute(context);
                if (success) {
                    log.info("✅ Task [{}] completed successfully.", node.getId());
                } else {
                    log.error("❌ Task [{}] failed.", node.getId());
                }
            } catch (Exception e) {
                log.error("❌ Task [{}] threw an exception: {}", node.getId(), e.getMessage());
            } finally {
                futures.get(node.getId()).complete(success);
            }
        });
    }

    private void validateDag(List<TaskNode> nodes, Map<String, TaskNode> nodeMap) {
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> dependents = new HashMap<>();

        for (TaskNode node : nodes) {
            inDegree.putIfAbsent(node.getId(), 0);
            dependents.putIfAbsent(node.getId(), new ArrayList<>());
        }

        for (TaskNode node : nodes) {
            for (String dep : node.getDependencies()) {
                if (!nodeMap.containsKey(dep)) {
                    throw new IllegalArgumentException("Missing dependency: " + dep + " for node: " + node.getId());
                }
                // dep must complete before node can run: dep -> node
                dependents.get(dep).add(node.getId());
                inDegree.put(node.getId(), inDegree.get(node.getId()) + 1);
            }
        }

        Queue<String> readyQueue = new LinkedList<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                readyQueue.add(entry.getKey());
            }
        }

        int visitedCount = 0;
        while (!readyQueue.isEmpty()) {
            String current = readyQueue.poll();
            visitedCount++;

            for (String dependentId : dependents.getOrDefault(current, Collections.emptyList())) {
                int newInDegree = inDegree.get(dependentId) - 1;
                inDegree.put(dependentId, newInDegree);
                if (newInDegree == 0) {
                    readyQueue.add(dependentId);
                }
            }
        }

        if (visitedCount != nodes.size()) {
            throw new IllegalArgumentException("Cycle detected in DAG topology. Visited " + visitedCount + " of " + nodes.size() + " nodes.");
        }
    }
}
