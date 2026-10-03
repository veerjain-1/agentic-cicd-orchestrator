package com.cicd.orchestrator.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {"commit-events"})
class ReadinessControllerTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @LocalServerPort
    private int port;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        // Points the orchestrator at the embedded broker spun up by @EmbeddedKafka
        // for this test, overriding the application.yml default.
        registry.add("spring.kafka.bootstrap-servers",
                () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void returnsReadyWhenKafkaIsReachable() {
        ResponseEntity<Map> response = restTemplate.getForEntity(
                "http://localhost:" + port + "/health/ready", Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).containsEntry("status", "READY");

        Map<String, Object> dependencies = (Map<String, Object>) response.getBody().get("dependencies");
        Map<String, Object> kafka = (Map<String, Object>) dependencies.get("kafka");
        assertThat(kafka).containsEntry("status", "UP");
    }
}
