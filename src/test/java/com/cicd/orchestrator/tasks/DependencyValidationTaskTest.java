package com.cicd.orchestrator.tasks;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class DependencyValidationTaskTest {

    @Test
    public void testCleanDependenciesPass() {
        DependencyValidationTask task = new DependencyValidationTask();
        Map<String, Object> context = new HashMap<>();
        context.put("declaredDependencies", List.of(
                "org.springframework.boot:spring-boot-starter-web:3.3.0",
                "org.springframework.kafka:spring-kafka:3.2.0"
        ));

        boolean success = task.execute(context);

        assertTrue(success);
        @SuppressWarnings("unchecked")
        Map<String, Object> report = (Map<String, Object>) context.get("dependencyReport");
        assertNotNull(report);
        assertEquals(0, report.get("vulnerabilitiesFound"));
        assertEquals(true, report.get("passed"));
    }

    @Test
    public void testKnownCveFailsPipeline() {
        DependencyValidationTask task = new DependencyValidationTask();
        Map<String, Object> context = new HashMap<>();
        context.put("declaredDependencies", List.of(
                "org.apache.logging.log4j:log4j-core:2.14.1",
                "org.springframework.boot:spring-boot-starter-web:3.3.0"
        ));

        boolean success = task.execute(context);

        assertFalse(success);
        @SuppressWarnings("unchecked")
        Map<String, Object> report = (Map<String, Object>) context.get("dependencyReport");
        assertNotNull(report);
        assertEquals(1, report.get("vulnerabilitiesFound"));
        assertEquals(false, report.get("passed"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> findings = (List<Map<String, Object>>) report.get("findings");
        assertEquals("CRITICAL", findings.get(0).get("severity"));
        assertTrue(((String) findings.get(0).get("advisory")).contains("Log4Shell"));
    }

    @Test
    public void testUnpinnedSnapshotFlagged() {
        DependencyValidationTask task = new DependencyValidationTask();
        Map<String, Object> context = new HashMap<>();
        context.put("declaredDependencies", List.of(
                "com.mycompany:internal-lib:1.0.0-SNAPSHOT"
        ));

        boolean success = task.execute(context);

        assertTrue(success, "SNAPSHOT alone does not fail build if no CVEs, but increments unpinned count");
        @SuppressWarnings("unchecked")
        Map<String, Object> report = (Map<String, Object>) context.get("dependencyReport");
        assertEquals(1, report.get("unpinnedDependencies"));
    }
}
