package io.kafkadlq.server.model;

import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

/**
 * Request to replay a set of messages from a DLQ topic.
 *
 * <p>Payloads are never accepted from the client — only coordinates. The
 * server re-reads authoritative bytes from the DLQ itself.
 *
 * @param dlqTopic      the DLQ topic to read from
 * @param messages      coordinates of the messages to replay
 * @param targetTopic   explicit destination; when null the server resolves it
 *                      from the x-dlq-original-topic header, then from naming
 *                      convention
 * @param dryRun        when true, resolve and report but produce nothing
 * @param ratePerSecond max records/second; null uses the server default,
 *                      0 disables rate limiting
 */
public record ReplayRequest(
        @NotBlank String dlqTopic,
        @NotEmpty List<@Valid MessageCoordinates> messages,
        String targetTopic,
        boolean dryRun,
        Double ratePerSecond) {

    public record MessageCoordinates(int partition, long offset) {
    }
}
