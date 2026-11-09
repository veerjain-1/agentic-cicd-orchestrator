package com.cicd.orchestrator.controller;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Dependency-aware readiness endpoint. Unlike {@link HealthCheckController},
 * which only reports that the process is up, this checks whether the
 * orchestrator can actually reach the infrastructure it depends on
 * (currently: the configured Kafka broker) before declaring itself ready
 * to accept and process pipeline events.
 */
@RestController
@RequestMapping("/health")
public class ReadinessController {

    private static final Logger log = LoggerFactory.getLogger(ReadinessController.class);
    private static final long KAFKA_CHECK_TIMEOUT_SECONDS = 3;

    private final String kafkaBootstrapServers;

    public ReadinessController(
            @Value("${spring.kafka.bootstrap-servers}") String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    @GetMapping("/ready")
    public ResponseEntity<Map<String, Object>> ready() {
        Map<String, Object> dependencies = new LinkedHashMap<>();

        boolean kafkaHealthy = checkKafka(dependencies);

        boolean allHealthy = kafkaHealthy;

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", allHealthy ? "READY" : "NOT_READY");
        body.put("dependencies", dependencies);

        return allHealthy
                ? ResponseEntity.ok(body)
                : ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }

    /**
     * Verifies Kafka connectivity by asking the admin client to describe
     * the cluster. This requires an actual round-trip to a broker, so it
     * fails fast (within {@link #KAFKA_CHECK_TIMEOUT_SECONDS}) if the
     * configured bootstrap servers are unreachable.
     */
    private boolean checkKafka(Map<String, Object> dependencies) {
        Map<String, Object> kafkaStatus = new HashMap<>();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) TimeUnit.SECONDS.toMillis(KAFKA_CHECK_TIMEOUT_SECONDS));

        try (AdminClient adminClient = AdminClient.create(props)) {
            adminClient.describeCluster().nodes().get(KAFKA_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            kafkaStatus.put("status", "UP");
            kafkaStatus.put("bootstrapServers", kafkaBootstrapServers);
            dependencies.put("kafka", kafkaStatus);
            return true;
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            log.warn("⚠️ Kafka readiness check failed: {}", e.getMessage());
            kafkaStatus.put("status", "DOWN");
            kafkaStatus.put("bootstrapServers", kafkaBootstrapServers);
            kafkaStatus.put("error", e.getMessage());
            dependencies.put("kafka", kafkaStatus);

            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }
}
