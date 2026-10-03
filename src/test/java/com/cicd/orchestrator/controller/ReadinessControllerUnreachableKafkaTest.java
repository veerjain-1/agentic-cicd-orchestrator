package com.cicd.orchestrator.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Separate SpringBootTest (its own application context) that points at an
 * unreachable Kafka broker, verifying the readiness endpoint fails closed
 * (503) rather than reporting healthy when Kafka can't actually be reached.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ReadinessControllerUnreachableKafkaTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @LocalServerPort
    private int port;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        // A address in the TEST-NET-1 reserved block (RFC 5737) -- guaranteed
        // non-routable, so the admin client's describeCluster() call reliably
        // times out instead of connecting.
        registry.add("spring.kafka.bootstrap-servers", () -> "192.0.2.1:9092");
    }

    @Test
    void returnsNotReadyWhenKafkaIsUnreachable() {
        ResponseEntity<Map> response = restTemplate.getForEntity(
                "http://localhost:" + port + "/health/ready", Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody()).containsEntry("status", "NOT_READY");

        Map<String, Object> dependencies = (Map<String, Object>) response.getBody().get("dependencies");
        Map<String, Object> kafka = (Map<String, Object>) dependencies.get("kafka");
        assertThat(kafka).containsEntry("status", "DOWN");
        assertThat(kafka).containsKey("error");
    }
}
