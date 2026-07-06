package io.kafkadlq.starter;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Properties for the DLQ manager starter, prefix {@code dlq-manager}.
 */
@ConfigurationProperties(prefix = "dlq-manager")
public class DlqManagerStarterProperties {

    /** Master switch for the starter's auto-configuration. */
    private boolean enabled = true;

    /**
     * Whether to auto-configure a DefaultErrorHandler wired to the enriched
     * recoverer. Set to false if the application configures its own error
     * handling but still wants the recoverer bean.
     */
    private boolean autoErrorHandler = true;

    /** Overrides spring.application.name in DLQ headers when set. */
    private String applicationName;

    /** Overrides spring.kafka.consumer.group-id in DLQ headers when set. */
    private String consumerGroup;

    private final Retry retry = new Retry();

    public static class Retry {
        /** Initial backoff interval before the first retry. */
        private long initialIntervalMs = 1_000;
        /** Backoff multiplier between retries. */
        private double multiplier = 2.0;
        /** Total time to keep retrying before dead-lettering. */
        private long maxElapsedMs = 30_000;

        public long getInitialIntervalMs() { return initialIntervalMs; }
        public void setInitialIntervalMs(long initialIntervalMs) { this.initialIntervalMs = initialIntervalMs; }
        public double getMultiplier() { return multiplier; }
        public void setMultiplier(double multiplier) { this.multiplier = multiplier; }
        public long getMaxElapsedMs() { return maxElapsedMs; }
        public void setMaxElapsedMs(long maxElapsedMs) { this.maxElapsedMs = maxElapsedMs; }
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public boolean isAutoErrorHandler() { return autoErrorHandler; }
    public void setAutoErrorHandler(boolean autoErrorHandler) { this.autoErrorHandler = autoErrorHandler; }
    public String getApplicationName() { return applicationName; }
    public void setApplicationName(String applicationName) { this.applicationName = applicationName; }
    public String getConsumerGroup() { return consumerGroup; }
    public void setConsumerGroup(String consumerGroup) { this.consumerGroup = consumerGroup; }
    public Retry getRetry() { return retry; }
}
