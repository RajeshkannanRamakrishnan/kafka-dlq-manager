package io.kafkadlq.server.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import io.kafkadlq.server.config.DlqManagerServerProperties;
import io.kafkadlq.server.model.DlqMessage;
import io.kafkadlq.starter.DlqHeaders;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

/**
 * Reads DLQ messages non-destructively.
 *
 * <p>Design invariants:
 * <ul>
 *   <li>Every browse uses a fresh, random consumer group with auto-commit
 *       disabled. Offsets are never committed. Browsing can never advance
 *       any real consumer group or hide messages.</li>
 *   <li>Messages are read newest-first by seeking relative to end offsets,
 *       because operators almost always care about recent failures.</li>
 *   <li>Values are deserialized as bytes and rendered as UTF-8; pluggable
 *       deserialization (Avro, Protobuf) is a post-v0.1 SPI.</li>
 * </ul>
 */
@Service
public class DlqBrowseService {

    private static final int MAX_VALUE_DISPLAY_BYTES = 64 * 1024;

    private final KafkaAdmin kafkaAdmin;
    private final DlqManagerServerProperties properties;

    public DlqBrowseService(KafkaAdmin kafkaAdmin, DlqManagerServerProperties properties) {
        this.kafkaAdmin = kafkaAdmin;
        this.properties = properties;
    }

    /**
     * Fetches up to {@code limit} of the most recent messages on the topic.
     */
    public List<DlqMessage> browse(String topic, int limit) {
        int effectiveLimit = Math.min(Math.max(limit, 1), properties.getMaxPageSize());

        try (Consumer<byte[], byte[]> consumer = newThrowawayConsumer()) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .toList();
            if (partitions.isEmpty()) {
                return List.of();
            }

            consumer.assign(partitions);
            Map<TopicPartition, Long> beginning = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);

            // Seek each partition back ~limit records from the end so a single
            // poll cycle yields the newest messages without scanning history.
            long perPartition = Math.max(1, (long) Math.ceil((double) effectiveLimit / partitions.size()));
            long expected = 0;
            for (TopicPartition tp : partitions) {
                long start = Math.max(beginning.get(tp), end.get(tp) - perPartition);
                consumer.seek(tp, start);
                expected += end.get(tp) - start;
            }
            if (expected == 0) {
                return List.of();
            }

            List<DlqMessage> messages = new ArrayList<>();
            long deadline = System.currentTimeMillis() + properties.getBrowsePollTimeoutMs();
            while (messages.size() < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
                records.forEach(r -> messages.add(toDlqMessage(r)));
            }

            messages.sort((a, b) -> Long.compare(b.timestamp(), a.timestamp()));
            return messages.size() > effectiveLimit
                    ? messages.subList(0, effectiveLimit)
                    : messages;
        }
    }

    /**
     * Fetches one exact message by coordinates. Used by replay to re-read the
     * authoritative bytes rather than trusting client-supplied payloads.
     */
    public ConsumerRecord<byte[], byte[]> fetchOne(String topic, int partition, long offset) {
        try (Consumer<byte[], byte[]> consumer = newThrowawayConsumer()) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));
            consumer.seek(tp, offset);

            long deadline = System.currentTimeMillis() + properties.getBrowsePollTimeoutMs();
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.partition() == partition && record.offset() == offset) {
                        return record;
                    }
                    if (record.offset() > offset) {
                        return null; // offset compacted / expired
                    }
                }
            }
            return null;
        }
    }

    /**
     * Groups the most recent messages by exception class + original topic.
     * Messages without starter headers land in an "(unknown)" bucket.
     */
    public Map<String, Long> groupByFailure(String topic, int sampleSize) {
        return browse(topic, sampleSize).stream()
                .collect(Collectors.groupingBy(
                        m -> {
                            var d = m.diagnostics();
                            String exception = d != null && d.exceptionClass() != null
                                    ? d.exceptionClass() : "(unknown exception)";
                            String origin = d != null && d.originalTopic() != null
                                    ? d.originalTopic() : "(unknown topic)";
                            return exception + " @ " + origin;
                        },
                        LinkedHashMap::new,
                        Collectors.counting()));
    }

    private Consumer<byte[], byte[]> newThrowawayConsumer() {
        Properties props = new Properties();
        props.putAll(kafkaAdmin.getConfigurationProperties());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-manager-browse-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private DlqMessage toDlqMessage(ConsumerRecord<byte[], byte[]> record) {
        Map<String, String> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), header.value() == null
                    ? null : new String(header.value(), StandardCharsets.UTF_8));
        }

        byte[] value = record.value();
        boolean truncated = value != null && value.length > MAX_VALUE_DISPLAY_BYTES;
        String renderedValue = value == null ? null : new String(
                value, 0, Math.min(value.length, MAX_VALUE_DISPLAY_BYTES), StandardCharsets.UTF_8);

        return new DlqMessage(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                record.key() == null ? null : new String(record.key(), StandardCharsets.UTF_8),
                renderedValue,
                truncated,
                headers,
                extractDiagnostics(headers));
    }

    private DlqMessage.Diagnostics extractDiagnostics(Map<String, String> headers) {
        if (!headers.containsKey(DlqHeaders.EXCEPTION_CLASS)
                && !headers.containsKey(DlqHeaders.ORIGINAL_TOPIC)) {
            return null;
        }
        return new DlqMessage.Diagnostics(
                headers.get(DlqHeaders.EXCEPTION_CLASS),
                headers.get(DlqHeaders.EXCEPTION_MESSAGE),
                headers.get(DlqHeaders.ORIGINAL_TOPIC),
                parseInt(headers.get(DlqHeaders.ORIGINAL_PARTITION)),
                parseLong(headers.get(DlqHeaders.ORIGINAL_OFFSET)),
                headers.get(DlqHeaders.CONSUMER_GROUP),
                headers.get(DlqHeaders.APPLICATION),
                parseLong(headers.get(DlqHeaders.FAILED_AT)),
                parseInt(headers.get(DlqHeaders.DELIVERY_ATTEMPTS)),
                parseInt(headers.get(DlqHeaders.REPLAY_COUNT)));
    }

    private static Integer parseInt(String s) {
        try {
            return s == null ? null : Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long parseLong(String s) {
        try {
            return s == null ? null : Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
