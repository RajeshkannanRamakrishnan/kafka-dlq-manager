package io.kafkadlq.starter;

/**
 * Standardized header names stamped onto dead-letter records.
 *
 * <p>This is the "protocol" shared between the starter (writer) and the
 * dlq-manager-server (reader). The server degrades gracefully when these
 * headers are absent, but diagnostics (exception grouping, origin tracking,
 * replay lineage) are only possible when they are present.
 *
 * <p>Note: spring-kafka's {@code DeadLetterPublishingRecoverer} already stamps
 * its own {@code kafka_dlt-*} headers. We deliberately use a parallel,
 * framework-agnostic namespace ({@code x-dlq-*}) so that non-Spring producers
 * (Python, Go, Node consumers) can adopt the same protocol.
 */
public final class DlqHeaders {

    private DlqHeaders() {
    }

    /** Fully qualified class name of the exception that caused the failure. */
    public static final String EXCEPTION_CLASS = "x-dlq-exception-class";

    /** Exception message (truncated to {@link #MAX_HEADER_VALUE_BYTES}). */
    public static final String EXCEPTION_MESSAGE = "x-dlq-exception-message";

    /** Truncated stack trace of the failure. */
    public static final String EXCEPTION_STACKTRACE = "x-dlq-exception-stacktrace";

    /** Topic the record was originally consumed from. */
    public static final String ORIGINAL_TOPIC = "x-dlq-original-topic";

    /** Partition the record was originally consumed from. */
    public static final String ORIGINAL_PARTITION = "x-dlq-original-partition";

    /** Offset of the record on the original partition. */
    public static final String ORIGINAL_OFFSET = "x-dlq-original-offset";

    /** Epoch millis timestamp of the original record. */
    public static final String ORIGINAL_TIMESTAMP = "x-dlq-original-timestamp";

    /** Consumer group that failed to process the record. */
    public static final String CONSUMER_GROUP = "x-dlq-consumer-group";

    /** Application name (spring.application.name) of the failing consumer. */
    public static final String APPLICATION = "x-dlq-application";

    /** Epoch millis when the record was published to the DLT. */
    public static final String FAILED_AT = "x-dlq-failed-at";

    /** Number of delivery attempts before the record was dead-lettered. */
    public static final String DELIVERY_ATTEMPTS = "x-dlq-delivery-attempts";

    // ---- Stamped by the server on replay, never by the starter ----

    /** Epoch millis when the record was last replayed by the manager. */
    public static final String REPLAYED_AT = "x-dlq-replayed-at";

    /** Number of times this record has been replayed. */
    public static final String REPLAY_COUNT = "x-dlq-replay-count";

    /** Maximum size for any single header value written by the starter. */
    public static final int MAX_HEADER_VALUE_BYTES = 8 * 1024;
}
