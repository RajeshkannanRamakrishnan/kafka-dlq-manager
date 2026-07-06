package io.kafkadlq.server.model;

import java.util.Map;

/**
 * A single message read from a DLQ topic, with diagnostics extracted from
 * standardized headers when available.
 *
 * @param topic            the DLQ topic the message lives on
 * @param partition        partition on the DLQ topic
 * @param offset           offset on the DLQ topic (stable identity for replay)
 * @param timestamp        record timestamp on the DLQ topic (epoch millis)
 * @param key              record key rendered as UTF-8 (null if absent)
 * @param value            record value rendered as UTF-8 / JSON (null = tombstone)
 * @param valueTruncated   whether the value was truncated for display
 * @param headers          all record headers rendered as UTF-8
 * @param diagnostics      extracted x-dlq-* diagnostics, empty when the
 *                         producing service does not use the starter
 */
public record DlqMessage(
        String topic,
        int partition,
        long offset,
        long timestamp,
        String key,
        String value,
        boolean valueTruncated,
        Map<String, String> headers,
        Diagnostics diagnostics) {

    public record Diagnostics(
            String exceptionClass,
            String exceptionMessage,
            String originalTopic,
            Integer originalPartition,
            Long originalOffset,
            String consumerGroup,
            String application,
            Long failedAt,
            Integer deliveryAttempts,
            Integer replayCount) {
    }
}
