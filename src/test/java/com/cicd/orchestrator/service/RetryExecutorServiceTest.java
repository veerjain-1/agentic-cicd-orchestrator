package com.cicd.orchestrator.service;

import com.cicd.orchestrator.engine.Task;
import com.cicd.orchestrator.model.RetryAttempt;
import com.cicd.orchestrator.model.RetryPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RetryExecutorServiceTest {

    @Mock
    private NotificationService notificationService;

    private RetryExecutorService retryExecutorService;

    @BeforeEach
    void setUp() {
        retryExecutorService = new RetryExecutorService(notificationService);
    }

    @Test
    void testSuccessfulTaskNoRetryNeeded() {
        Task successTask = ctx -> true;
        RetryPolicy policy = new RetryPolicy(3, 10, RetryPolicy.BackoffStrategy.FIXED);

        boolean result = retryExecutorService.executeWithRetry(
                "pipe-1", "veerjain-1/repo", "build", successTask, new HashMap<>(), policy);

        assertTrue(result);
        // No retry notification should be sent
        verify(notificationService, never()).notifyRetryAttempt(anyString(), anyString(), anyString(), anyInt(), anyInt(), anyString());
    }

    @Test
    void testFailedTaskRetriesAndEventuallySucceeds() {
        // Fail twice, succeed on third attempt
        Task flakyTask = new Task() {
            int callCount = 0;
            @Override
            public boolean execute(Map<String, Object> context) {
                callCount++;
                return callCount >= 3;
            }
        };

        RetryPolicy policy = new RetryPolicy(3, 10, RetryPolicy.BackoffStrategy.FIXED);

        boolean result = retryExecutorService.executeWithRetry(
                "pipe-2", "veerjain-1/repo", "test", flakyTask, new HashMap<>(), policy);

        assertTrue(result);

        // Should have sent retry notifications for attempts 1 and 2
        verify(notificationService, times(2)).notifyRetryAttempt(
                eq("pipe-2"), eq("veerjain-1/repo"), eq("test"), anyInt(), eq(3), anyString());
    }

    @Test
    void testAllRetriesExhausted() {
        Task alwaysFailTask = ctx -> false;
        RetryPolicy policy = new RetryPolicy(2, 10, RetryPolicy.BackoffStrategy.FIXED);

        boolean result = retryExecutorService.executeWithRetry(
                "pipe-3", "veerjain-1/repo", "deploy", alwaysFailTask, new HashMap<>(), policy);

        assertFalse(result);

        // Should have sent 2 retry notifications (for attempt 1 and 2)
        verify(notificationService, times(2)).notifyRetryAttempt(
                eq("pipe-3"), eq("veerjain-1/repo"), eq("deploy"), anyInt(), eq(2), anyString());
    }

    @Test
    void testExceptionHandlingWithRetry() {
        Task throwingTask = new Task() {
            int callCount = 0;
            @Override
            public boolean execute(Map<String, Object> context) {
                callCount++;
                if (callCount < 3) {
                    throw new RuntimeException("Transient failure");
                }
                return true;
            }
        };

        RetryPolicy policy = new RetryPolicy(3, 10, RetryPolicy.BackoffStrategy.FIXED);

        boolean result = retryExecutorService.executeWithRetry(
                "pipe-4", "veerjain-1/repo", "build", throwingTask, new HashMap<>(), policy);

        assertTrue(result);
    }

    @Test
    void testNonRetryableExceptionStopsImmediately() {
        Task throwingTask = ctx -> {
            throw new IllegalArgumentException("Bad input");
        };

        RetryPolicy policy = new RetryPolicy(5, 10, RetryPolicy.BackoffStrategy.FIXED);
        policy.setRetryableExceptions(new String[]{"RuntimeException"});

        boolean result = retryExecutorService.executeWithRetry(
                "pipe-5", "veerjain-1/repo", "validate", throwingTask, new HashMap<>(), policy);

        assertFalse(result);

        // Should NOT retry since IllegalArgumentException is not in the retryable list
        verify(notificationService, never()).notifyRetryAttempt(anyString(), anyString(), anyString(), anyInt(), anyInt(), anyString());
    }

    @Test
    void testRetryHistoryTracking() {
        Task failTask = ctx -> false;
        RetryPolicy policy = new RetryPolicy(1, 10, RetryPolicy.BackoffStrategy.FIXED);

        retryExecutorService.executeWithRetry(
                "pipe-6", "veerjain-1/repo", "build", failTask, new HashMap<>(), policy);

        List<RetryAttempt> history = retryExecutorService.getRetryHistory("pipe-6");
        assertFalse(history.isEmpty());
        assertEquals(2, history.size()); // Initial attempt + 1 retry
    }

    @Test
    void testTaskRetryHistoryFiltering() {
        Task failTask = ctx -> false;
        RetryPolicy policy = new RetryPolicy(0, 10, RetryPolicy.BackoffStrategy.FIXED);

        retryExecutorService.executeWithRetry(
                "pipe-7", "veerjain-1/repo", "build", failTask, new HashMap<>(), policy);
        retryExecutorService.executeWithRetry(
                "pipe-7", "veerjain-1/repo", "test", failTask, new HashMap<>(), policy);

        List<RetryAttempt> buildHistory = retryExecutorService.getTaskRetryHistory("pipe-7", "build");
        List<RetryAttempt> testHistory = retryExecutorService.getTaskRetryHistory("pipe-7", "test");

        assertEquals(1, buildHistory.size());
        assertEquals(1, testHistory.size());
        assertEquals("build", buildHistory.get(0).getTaskId());
        assertEquals("test", testHistory.get(0).getTaskId());
    }

    @Test
    void testClearHistory() {
        Task failTask = ctx -> false;
        RetryPolicy policy = new RetryPolicy(0, 10, RetryPolicy.BackoffStrategy.FIXED);

        retryExecutorService.executeWithRetry(
                "pipe-8", "veerjain-1/repo", "build", failTask, new HashMap<>(), policy);

        assertFalse(retryExecutorService.getRetryHistory("pipe-8").isEmpty());

        retryExecutorService.clearHistory("pipe-8");
        assertTrue(retryExecutorService.getRetryHistory("pipe-8").isEmpty());
    }

    @Test
    void testEmptyHistoryForUnknownPipeline() {
        List<RetryAttempt> history = retryExecutorService.getRetryHistory("nonexistent");
        assertTrue(history.isEmpty());
    }
}
