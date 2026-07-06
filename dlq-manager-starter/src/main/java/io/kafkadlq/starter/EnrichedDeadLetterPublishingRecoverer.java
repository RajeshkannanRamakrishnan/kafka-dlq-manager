package io.kafkadlq.starter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

/**
 * A {@link DeadLetterPublishingRecoverer} that stamps the standardized
 * {@link DlqHeaders} onto every dead-lettered record, so the dlq-manager-server
 * can group failures by exception, trace origin, and manage replay lineage.
 *
 * <p>Usage (typically wired by {@link DlqManagerAutoConfiguration}, but can be
 * created manually):
 *
 * <pre>{@code
 * var recoverer = new EnrichedDeadLetterPublishingRecoverer(
 *         kafkaTemplate, "orders-service", "orders-consumer-group");
 * var errorHandler = new DefaultErrorHandler(recoverer, new ExponentialBackOff());
 * }</pre>
 */
public class EnrichedDeadLetterPublishingRecoverer extends DeadLetterPublishingRecoverer {

    private final String applicationName;
    private final String consumerGroup;

    public EnrichedDeadLetterPublishingRecoverer(KafkaOperations<?, ?> template,
                                                 String applicationName,
                                                 String consumerGroup) {
        super(template);
        this.applicationName = applicationName;
        this.consumerGroup = consumerGroup;
    }

    public EnrichedDeadLetterPublishingRecoverer(KafkaOperations<?, ?> template,
                                                 String applicationName,
                                                 String consumerGroup,
                                                 BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver) {
        super(template, destinationResolver);
        this.applicationName = applicationName;
        this.consumerGroup = consumerGroup;
    }

    @Override
    protected void maybeAddOriginalHeaders(Headers kafkaHeaders,
                                           ConsumerRecord<?, ?> record,
                                           Exception ex) {
        super.maybeAddOriginalHeaders(kafkaHeaders, record, ex);
        Throwable cause = rootCause(ex);

        put(kafkaHeaders, DlqHeaders.EXCEPTION_CLASS, cause.getClass().getName());
        put(kafkaHeaders, DlqHeaders.EXCEPTION_MESSAGE, safe(cause.getMessage()));
        put(kafkaHeaders, DlqHeaders.EXCEPTION_STACKTRACE, stackTraceOf(cause));
        put(kafkaHeaders, DlqHeaders.ORIGINAL_TOPIC, record.topic());
        put(kafkaHeaders, DlqHeaders.ORIGINAL_PARTITION, String.valueOf(record.partition()));
        put(kafkaHeaders, DlqHeaders.ORIGINAL_OFFSET, String.valueOf(record.offset()));
        put(kafkaHeaders, DlqHeaders.ORIGINAL_TIMESTAMP, String.valueOf(record.timestamp()));
        put(kafkaHeaders, DlqHeaders.CONSUMER_GROUP, consumerGroup);
        put(kafkaHeaders, DlqHeaders.APPLICATION, applicationName);
        put(kafkaHeaders, DlqHeaders.FAILED_AT, String.valueOf(System.currentTimeMillis()));

        byte[] attempts = lastValue(kafkaHeaders, KafkaHeaders.DELIVERY_ATTEMPT);
        if (attempts != null && attempts.length == 4) {
            int n = ((attempts[0] & 0xFF) << 24) | ((attempts[1] & 0xFF) << 16)
                    | ((attempts[2] & 0xFF) << 8) | (attempts[3] & 0xFF);
            put(kafkaHeaders, DlqHeaders.DELIVERY_ATTEMPTS, String.valueOf(n));
        }
    }

    private static Throwable rootCause(Throwable t) {
        Throwable current = t;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    private static String stackTraceOf(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    private static byte[] lastValue(Headers headers, String key) {
        var header = headers.lastHeader(key);
        return header == null ? null : header.value();
    }

    private static void put(Headers headers, String key, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > DlqHeaders.MAX_HEADER_VALUE_BYTES) {
            byte[] truncated = new byte[DlqHeaders.MAX_HEADER_VALUE_BYTES];
            System.arraycopy(bytes, 0, truncated, 0, DlqHeaders.MAX_HEADER_VALUE_BYTES);
            bytes = truncated;
        }
        headers.remove(key);
        headers.add(key, bytes);
    }
}
