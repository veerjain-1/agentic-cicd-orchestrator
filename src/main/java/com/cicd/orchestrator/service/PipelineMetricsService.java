package com.cicd.orchestrator.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Service managing custom Prometheus & Micrometer metrics for CI/CD pipeline observability.
 * Exposes metrics for Prometheus scraping and Grafana dashboard visualization.
 */
@Service
public class PipelineMetricsService {

    private final MeterRegistry meterRegistry;

    public PipelineMetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Records end-to-end pipeline execution completion and latency.
     */
    public void recordPipelineCompleted(String repo, String environment, boolean success, long durationMs) {
        String status = success ? "SUCCESS" : "FAILED";

        Counter.builder("cicd_pipeline_executions_total")
                .description("Total number of CI/CD pipeline executions")
                .tag("repo", repo != null ? repo : "unknown")
                .tag("environment", environment != null ? environment : "development")
                .tag("status", status)
                .register(meterRegistry)
                .increment();

        Timer.builder("cicd_pipeline_duration_seconds")
                .description("Total duration of CI/CD pipeline executions in seconds")
                .tag("repo", repo != null ? repo : "unknown")
                .tag("status", status)
                .publishPercentiles(0.5, 0.9, 0.95, 0.99)
                .register(meterRegistry)
                .record(durationMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Records the duration and risk rating of LLM code analysis agent tasks.
     */
    public void recordAgentAnalysis(String repo, String riskScore, long durationMs) {
        Timer.builder("cicd_agent_code_analysis_duration_seconds")
                .description("Duration of LLM code analysis agent inspection")
                .tag("repo", repo != null ? repo : "unknown")
                .tag("risk_score", riskScore != null ? riskScore : "UNKNOWN")
                .register(meterRegistry)
                .record(durationMs, TimeUnit.MILLISECONDS);

        Counter.builder("cicd_agent_reviews_total")
                .description("Total number of automated LLM agent code reviews performed")
                .tag("repo", repo != null ? repo : "unknown")
                .tag("risk_score", riskScore != null ? riskScore : "UNKNOWN")
                .register(meterRegistry)
                .increment();
    }

    /**
     * Records count of detected third-party dependency vulnerabilities.
     */
    public void recordVulnerabilities(String repo, int count) {
        if (count > 0) {
            Counter.builder("cicd_dependency_vulnerabilities_total")
                    .description("Total number of dependency vulnerabilities discovered")
                    .tag("repo", repo != null ? repo : "unknown")
                    .register(meterRegistry)
                    .increment(count);
        }
    }
}
