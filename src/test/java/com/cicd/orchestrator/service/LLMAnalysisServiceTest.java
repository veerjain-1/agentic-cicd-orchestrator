package com.cicd.orchestrator.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class LLMAnalysisServiceTest {

    private LLMAnalysisService service;

    @BeforeEach
    public void setup() {
        service = new LLMAnalysisService();
        ReflectionTestUtils.setField(service, "modelName", "gpt-4o");
        ReflectionTestUtils.setField(service, "provider", "openai");
    }

    @Test
    public void testCleanDiffYieldsLowRisk() {
        String diff = "+ public int add(int a, int b) { return a + b; }";
        Map<String, Object> analysis = service.analyzeCode("test-repo", "abc1234", diff);

        assertEquals("LOW", analysis.get("riskScore"));
        assertEquals(true, analysis.get("passed"));
        @SuppressWarnings("unchecked")
        List<String> sec = (List<String>) analysis.get("securityFindings");
        assertTrue(sec.isEmpty());
    }

    @Test
    public void testSecretLeakageTriggersCriticalRisk() {
        String diff = "+ String awsKey = \"AKIAIOSFODNN7EXAMPLE\";";
        Map<String, Object> analysis = service.analyzeCode("test-repo", "deadbeef", diff);

        assertEquals("CRITICAL", analysis.get("riskScore"));
        assertEquals(false, analysis.get("passed"));
        @SuppressWarnings("unchecked")
        List<String> sec = (List<String>) analysis.get("securityFindings");
        assertFalse(sec.isEmpty());
        assertTrue(sec.get(0).contains("API key"));
    }

    @Test
    public void testSqlInjectionTriggersHighRisk() {
        String diff = "+ Statement stmt = conn.createStatement();\n+ ResultSet rs = stmt.executeQuery(\"SELECT * FROM users WHERE name = '\" + userInput + \"'\");";
        Map<String, Object> analysis = service.analyzeCode("test-repo", "feebdaed", diff);

        assertTrue(analysis.get("riskScore").equals("HIGH") || analysis.get("riskScore").equals("CRITICAL"));
        @SuppressWarnings("unchecked")
        List<String> sec = (List<String>) analysis.get("securityFindings");
        assertTrue(sec.stream().anyMatch(s -> s.contains("SQL concatenation")));
    }

    @Test
    public void testPrSummaryMarkdownGeneration() {
        Map<String, Object> analysis = service.analyzeCode("orchestrator", "7788990", "+ public void deploy() {}");
        Map<String, Object> depReport = Map.of(
                "totalDependenciesScanned", 12,
                "vulnerabilitiesFound", 0,
                "unpinnedDependencies", 0
        );

        String markdown = service.generatePrSummary(analysis, depReport);

        assertNotNull(markdown);
        assertTrue(markdown.contains("## 🤖 Autonomous Agentic CI/CD Code Review"));
        assertTrue(markdown.contains("orchestrator"));
        assertTrue(markdown.contains("Scanned Dependencies: `12`"));
    }
}
