package io.kafkadlq.server.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Server-side properties, prefix {@code dlq-manager.server}.
 */
@ConfigurationProperties(prefix = "dlq-manager.server")
public class DlqManagerServerProperties {

    /**
     * Regex patterns used to discover DLQ topics. A topic matching any pattern
     * is treated as a dead-letter topic. Defaults cover spring-kafka's ".DLT"
     * convention and common "-dlq"/"-dlt" suffixes.
     */
    private List<String> dlqTopicPatterns = List.of(".*\\.DLT$", ".*-dlq$", ".*-dlt$");

    /**
     * How the original topic name is derived from a DLQ topic name when the
     * x-dlq-original-topic header is absent. "{topic}" placeholder style:
     * the first matching suffix is stripped.
     */
    private List<String> dlqSuffixes = List.of(".DLT", "-dlq", "-dlt");

    /** Maximum number of messages returned by a single browse request. */
    private int maxPageSize = 500;

    /** Poll timeout when browsing a DLQ topic. */
    private long browsePollTimeoutMs = 3_000;

    /** Default max records/second when replaying. 0 disables rate limiting. */
    private double defaultReplayRatePerSecond = 50;

    /** Hard cap on messages replayed in one request. */
    private int maxReplayBatchSize = 10_000;

    public List<String> getDlqTopicPatterns() { return dlqTopicPatterns; }
    public void setDlqTopicPatterns(List<String> dlqTopicPatterns) { this.dlqTopicPatterns = dlqTopicPatterns; }
    public List<String> getDlqSuffixes() { return dlqSuffixes; }
    public void setDlqSuffixes(List<String> dlqSuffixes) { this.dlqSuffixes = dlqSuffixes; }
    public int getMaxPageSize() { return maxPageSize; }
    public void setMaxPageSize(int maxPageSize) { this.maxPageSize = maxPageSize; }
    public long getBrowsePollTimeoutMs() { return browsePollTimeoutMs; }
    public void setBrowsePollTimeoutMs(long browsePollTimeoutMs) { this.browsePollTimeoutMs = browsePollTimeoutMs; }
    public double getDefaultReplayRatePerSecond() { return defaultReplayRatePerSecond; }
    public void setDefaultReplayRatePerSecond(double defaultReplayRatePerSecond) { this.defaultReplayRatePerSecond = defaultReplayRatePerSecond; }
    public int getMaxReplayBatchSize() { return maxReplayBatchSize; }
    public void setMaxReplayBatchSize(int maxReplayBatchSize) { this.maxReplayBatchSize = maxReplayBatchSize; }
}
