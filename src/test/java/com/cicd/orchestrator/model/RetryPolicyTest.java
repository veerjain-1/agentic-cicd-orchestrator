package com.cicd.orchestrator.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RetryPolicyTest {

    @Test
    void testDefaultRetryPolicy() {
        RetryPolicy policy = new RetryPolicy();
        assertEquals(3, policy.getMaxRetries());
        assertEquals(1000, policy.getBaseDelayMs());
        assertEquals(30_000, policy.getMaxDelayMs());
        assertEquals(RetryPolicy.BackoffStrategy.EXPONENTIAL_WITH_JITTER, policy.getBackoffStrategy());
    }

    @Test
    void testFixedBackoffCalculation() {
        RetryPolicy policy = new RetryPolicy(3, 2000, RetryPolicy.BackoffStrategy.FIXED);

        assertEquals(2000, policy.calculateDelay(0));
        assertEquals(2000, policy.calculateDelay(1));
        assertEquals(2000, policy.calculateDelay(5));
    }

    @Test
    void testExponentialBackoffCalculation() {
        RetryPolicy policy = new RetryPolicy(5, 1000, RetryPolicy.BackoffStrategy.EXPONENTIAL);
        policy.setMaxDelayMs(60_000);

        assertEquals(1000, policy.calculateDelay(0));  // 1000 * 2^0
        assertEquals(2000, policy.calculateDelay(1));  // 1000 * 2^1
        assertEquals(4000, policy.calculateDelay(2));  // 1000 * 2^2
        assertEquals(8000, policy.calculateDelay(3));  // 1000 * 2^3
    }

    @Test
    void testExponentialBackoffCapsAtMaxDelay() {
        RetryPolicy policy = new RetryPolicy(10, 1000, RetryPolicy.BackoffStrategy.EXPONENTIAL);
        policy.setMaxDelayMs(5000);

        // 1000 * 2^5 = 32000, should be capped at 5000
        assertEquals(5000, policy.calculateDelay(5));
    }

    @Test
    void testExponentialWithJitterIsWithinBounds() {
        RetryPolicy policy = new RetryPolicy();
        policy.setBaseDelayMs(1000);
        policy.setJitterMs(500);
        policy.setMaxDelayMs(60_000);

        for (int i = 0; i < 100; i++) {
            long delay = policy.calculateDelay(0);
            // Should be between baseDelay and baseDelay + jitter
            assertTrue(delay >= 1000 && delay <= 1500,
                    "Delay " + delay + " should be between 1000 and 1500");
        }
    }

    @Test
    void testIsRetryableWithNoConfiguredExceptions() {
        RetryPolicy policy = new RetryPolicy();
        // No retryable exceptions configured = retry all
        assertTrue(policy.isRetryable(new RuntimeException("test")));
        assertTrue(policy.isRetryable(new IllegalArgumentException("bad arg")));
    }

    @Test
    void testIsRetryableWithConfiguredExceptions() {
        RetryPolicy policy = new RetryPolicy();
        policy.setRetryableExceptions(new String[]{"RuntimeException", "java.io.IOException"});

        assertTrue(policy.isRetryable(new RuntimeException("retryable")));
        assertFalse(policy.isRetryable(new IllegalArgumentException("not retryable")));
    }

    @Test
    void testCustomConstructor() {
        RetryPolicy policy = new RetryPolicy(5, 2000, RetryPolicy.BackoffStrategy.FIXED);

        assertEquals(5, policy.getMaxRetries());
        assertEquals(2000, policy.getBaseDelayMs());
        assertEquals(RetryPolicy.BackoffStrategy.FIXED, policy.getBackoffStrategy());
    }

    @Test
    void testToString() {
        RetryPolicy policy = new RetryPolicy();
        String str = policy.toString();
        assertTrue(str.contains("maxRetries=3"));
        assertTrue(str.contains("baseDelayMs=1000"));
        assertTrue(str.contains("EXPONENTIAL_WITH_JITTER"));
    }
}
