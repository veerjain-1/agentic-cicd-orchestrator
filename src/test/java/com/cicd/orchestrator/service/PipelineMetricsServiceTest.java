package com.cicd.orchestrator.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class PipelineMetricsServiceTest {

    private MeterRegistry meterRegistry;
    private PipelineMetricsService metricsService;

    @BeforeEach
    public void setup() {
        meterRegistry = new SimpleMeterRegistry();
        metricsService = new PipelineMetricsService(meterRegistry);
    }

    @Test
    public void testRecordPipelineCompleted() {
        metricsService.recordPipelineCompleted("test-repo", "production", true, 450);

        Counter counter = meterRegistry.find("cicd_pipeline_executions_total")
                .tag("repo", "test-repo")
                .tag("status", "SUCCESS")
                .counter();
        assertNotNull(counter);
        assertEquals(1.0, counter.count());

        Timer timer = meterRegistry.find("cicd_pipeline_duration_seconds")
                .tag("repo", "test-repo")
                .tag("status", "SUCCESS")
                .timer();
        assertNotNull(timer);
        assertEquals(1, timer.count());
    }

    @Test
    public void testRecordAgentAnalysisAndVulnerabilities() {
        metricsService.recordAgentAnalysis("security-repo", "HIGH", 1200);
        metricsService.recordVulnerabilities("security-repo", 3);

        Counter agentCounter = meterRegistry.find("cicd_agent_reviews_total")
                .tag("risk_score", "HIGH")
                .counter();
        assertNotNull(agentCounter);
        assertEquals(1.0, agentCounter.count());

        Counter vulnCounter = meterRegistry.find("cicd_dependency_vulnerabilities_total")
                .counter();
        assertNotNull(vulnCounter);
        assertEquals(3.0, vulnCounter.count());
    }
}
