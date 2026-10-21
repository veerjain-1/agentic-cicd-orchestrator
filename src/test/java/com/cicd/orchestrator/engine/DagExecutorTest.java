package com.cicd.orchestrator.engine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class DagExecutorTest {

    private DagExecutor executor;
    private Map<String, Object> context;

    @BeforeEach
    public void setup() {
        executor = new DagExecutor();
        context = new ConcurrentHashMap<>();
    }

    @Test
    public void testSequentialExecution() {
        List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());

        TaskNode nodeA = new TaskNode("nodeA", ctx -> {
            executionOrder.add("nodeA");
            return true;
        });

        TaskNode nodeB = new TaskNode("nodeB", ctx -> {
            executionOrder.add("nodeB");
            return true;
        }).dependsOn("nodeA");

        boolean result = executor.execute(List.of(nodeA, nodeB), context);

        assertTrue(result);
        assertEquals(List.of("nodeA", "nodeB"), executionOrder);
    }

    @Test
    public void testConcurrentExecutionWithVirtualThreads() {
        AtomicInteger runningConcurrent = new AtomicInteger(0);
        AtomicInteger maxConcurrency = new AtomicInteger(0);

        Task task = ctx -> {
            int current = runningConcurrent.incrementAndGet();
            maxConcurrency.updateAndGet(max -> Math.max(max, current));
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            runningConcurrent.decrementAndGet();
            return true;
        };

        TaskNode node1 = new TaskNode("n1", task);
        TaskNode node2 = new TaskNode("n2", task);
        TaskNode node3 = new TaskNode("n3", task);

        boolean result = executor.execute(List.of(node1, node2, node3), context);

        assertTrue(result);
        assertTrue(maxConcurrency.get() >= 2, "Tasks without dependencies should execute concurrently");
    }

    @Test
    public void testCycleDetectionThrowsException() {
        TaskNode nodeA = new TaskNode("nodeA", ctx -> true).dependsOn("nodeB");
        TaskNode nodeB = new TaskNode("nodeB", ctx -> true).dependsOn("nodeA");

        assertThrows(IllegalArgumentException.class, () -> {
            executor.execute(List.of(nodeA, nodeB), context);
        });
    }

    @Test
    public void testMissingDependencyThrowsException() {
        TaskNode nodeA = new TaskNode("nodeA", ctx -> true).dependsOn("nonExistentNode");

        assertThrows(IllegalArgumentException.class, () -> {
            executor.execute(List.of(nodeA), context);
        });
    }

    @Test
    public void testFailedNodeHaltsDependentNodes() {
        List<String> executed = Collections.synchronizedList(new ArrayList<>());

        TaskNode nodeA = new TaskNode("nodeA", ctx -> {
            executed.add("nodeA");
            return false; // Failure
        });

        TaskNode nodeB = new TaskNode("nodeB", ctx -> {
            executed.add("nodeB");
            return true;
        }).dependsOn("nodeA");

        boolean result = executor.execute(List.of(nodeA, nodeB), context);

        assertFalse(result);
        assertTrue(executed.contains("nodeA"));
        assertFalse(executed.contains("nodeB"), "Dependent node must not execute if parent fails");
    }
}
