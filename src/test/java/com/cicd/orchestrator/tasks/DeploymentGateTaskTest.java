package com.cicd.orchestrator.tasks;

import com.cicd.orchestrator.service.DeploymentGateService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DeploymentGateTaskTest {

    @Mock
    private DeploymentGateService deploymentGateService;

    private DeploymentGateTask task;

    @BeforeEach
    void setUp() {
        task = new DeploymentGateTask(deploymentGateService);
    }

    @Test
    void testExecuteGatePassed() {
        when(deploymentGateService.evaluateGate(any())).thenReturn(true);

        Map<String, Object> context = new HashMap<>();
        context.put("repo", "veerjain-1/test-repo");
        context.put("environment", "development");

        boolean result = task.execute(context);

        assertTrue(result);
        assertTrue(context.containsKey("deploymentDecision"));
    }

    @Test
    void testExecuteGateBlocked() {
        when(deploymentGateService.evaluateGate(any())).thenReturn(false);

        Map<String, Object> context = new HashMap<>();
        context.put("repo", "veerjain-1/test-repo");
        context.put("environment", "production");

        boolean result = task.execute(context);

        assertFalse(result);
    }
}
