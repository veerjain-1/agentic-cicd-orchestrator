package com.cicd.orchestrator.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configurable retry policy for pipeline tasks.
 * Supports exponential backoff with jitter to prevent thundering herd problems.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RetryPolicy {

    public enum BackoffStrategy {
        /** Fixed delay between retries */
        FIXED,
        /** Delay doubles after each retry: baseDelay * 2^attempt */
        EXPONENTIAL,
        /** Exponential backoff with random jitter: (baseDelay * 2^attempt) + random(0, jitterMs) */
        EXPONENTIAL_WITH_JITTER
    }

    @JsonProperty("maxRetries")
    private int maxRetries;

    @JsonProperty("baseDelayMs")
    private long baseDelayMs;

    @JsonProperty("maxDelayMs")
    private long maxDelayMs;

    @JsonProperty("jitterMs")
    private long jitterMs;

    @JsonProperty("backoffStrategy")
    private BackoffStrategy backoffStrategy;

    @JsonProperty("retryableExceptions")
    private String[] retryableExceptions;

    public RetryPolicy() {
        this.maxRetries = 3;
        this.baseDelayMs = 1000;
        this.maxDelayMs = 30_000;
        this.jitterMs = 500;
        this.backoffStrategy = BackoffStrategy.EXPONENTIAL_WITH_JITTER;
        this.retryableExceptions = new String[]{};
    }

    public RetryPolicy(int maxRetries, long baseDelayMs, BackoffStrategy strategy) {
        this();
        this.maxRetries = maxRetries;
        this.baseDelayMs = baseDelayMs;
        this.backoffStrategy = strategy;
    }

    /**
     * Calculate the delay for a given attempt number (0-indexed).
     */
    public long calculateDelay(int attempt) {
        long delay;
        switch (backoffStrategy) {
            case FIXED:
                delay = baseDelayMs;
                break;
            case EXPONENTIAL:
                delay = baseDelayMs * (long) Math.pow(2, attempt);
                break;
            case EXPONENTIAL_WITH_JITTER:
                delay = baseDelayMs * (long) Math.pow(2, attempt);
                delay += (long) (Math.random() * jitterMs);
                break;
            default:
                delay = baseDelayMs;
        }
        return Math.min(delay, maxDelayMs);
    }

    /**
     * Check if a given exception type should trigger a retry.
     * Returns true if no specific retryable exceptions are configured (retry all),
     * or if the exception's class name matches one of the configured types.
     */
    public boolean isRetryable(Exception e) {
        if (retryableExceptions == null || retryableExceptions.length == 0) {
            return true; // Retry all exceptions by default
        }
        String exceptionName = e.getClass().getName();
        String simpleExceptionName = e.getClass().getSimpleName();
        for (String retryable : retryableExceptions) {
            if (exceptionName.equals(retryable) || simpleExceptionName.equals(retryable)) {
                return true;
            }
        }
        return false;
    }

    // Getters and Setters
    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    public long getBaseDelayMs() { return baseDelayMs; }
    public void setBaseDelayMs(long baseDelayMs) { this.baseDelayMs = baseDelayMs; }
    public long getMaxDelayMs() { return maxDelayMs; }
    public void setMaxDelayMs(long maxDelayMs) { this.maxDelayMs = maxDelayMs; }
    public long getJitterMs() { return jitterMs; }
    public void setJitterMs(long jitterMs) { this.jitterMs = jitterMs; }
    public BackoffStrategy getBackoffStrategy() { return backoffStrategy; }
    public void setBackoffStrategy(BackoffStrategy backoffStrategy) { this.backoffStrategy = backoffStrategy; }
    public String[] getRetryableExceptions() { return retryableExceptions; }
    public void setRetryableExceptions(String[] retryableExceptions) { this.retryableExceptions = retryableExceptions; }

    @Override
    public String toString() {
        return "RetryPolicy{maxRetries=" + maxRetries +
                ", baseDelayMs=" + baseDelayMs +
                ", maxDelayMs=" + maxDelayMs +
                ", backoff=" + backoffStrategy + "}";
    }
}
