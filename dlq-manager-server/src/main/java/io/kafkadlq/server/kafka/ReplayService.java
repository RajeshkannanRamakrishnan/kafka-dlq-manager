package io.kafkadlq.server.kafka;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.kafkadlq.server.config.DlqManagerServerProperties;
import io.kafkadlq.server.model.ReplayRequest;
import io.kafkadlq.server.model.ReplayResult;
import io.kafkadlq.starter.DlqHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

/**
 * Republishes dead-letter messages back to their original (or an explicit
 * target) topic.
 *
 * <p>Replay safety invariants:
 * <ul>
 *   <li><b>Authoritative bytes:</b> messages are re-read from the DLQ by
 *       (partition, offset) coordinates; the API never accepts payloads from
 *       the client, so what is replayed is exactly what failed.</li>
 *   <li><b>Dry run first:</b> {@code dryRun=true} resolves every target and
 *       reports what <i>would</i> happen without producing anything.</li>
 *   <li><b>Rate limited:</b> a simple token-per-interval limiter caps
 *       records/second so a recovering consumer is not immediately buried.</li>
 *   <li><b>Lineage:</b> replayed records carry {@code x-dlq-replayed-at} and
 *       an incremented {@code x-dlq-replay-count} so repeat failures are
 *       visible and replay storms are diagnosable.</li>
 *   <li><b>Non-destructive:</b> replay never commits or deletes anything on
 *       the DLQ topic. Removing replayed messages is a separate, explicit
 *       discard operation (post-v0.1).</li>
 * </ul>
 */
@Service
public class ReplayService {

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);

    private final KafkaAdmin kafkaAdmin;
    private final DlqBrowseService browseService;
    private final TopicDiscoveryService topicDiscoveryService;
    private final DlqManagerServerProperties properties;

    public ReplayService(KafkaAdmin kafkaAdmin,
                         DlqBrowseService browseService,
                         TopicDiscoveryService topicDiscoveryService,
                         DlqManagerServerProperties properties) {
        this.kafkaAdmin = kafkaAdmin;
        this.browseService = browseService;
        this.topicDiscoveryService = topicDiscoveryService;
        this.properties = properties;
    }

    public ReplayResult replay(ReplayRequest request) {
        if (request.messages().size() > properties.getMaxReplayBatchSize()) {
            throw new IllegalArgumentException(
                    "Replay batch of " + request.messages().size()
                    + " exceeds maximum of " + properties.getMaxReplayBatchSize());
        }

        double rate = request.ratePerSecond() != null
                ? request.ratePerSecond()
                : properties.getDefaultReplayRatePerSecond();
        long intervalNanos = rate > 0 ? (long) (1_000_000_000L / rate) : 0;

        List<ReplayResult.ItemResult> items = new ArrayList<>();
        int succeeded = 0;
        int failed = 0;
        int skipped = 0;

        try (Producer<byte[], byte[]> producer = request.dryRun() ? null : newProducer()) {
            long nextSendAt = System.nanoTime();

            for (ReplayRequest.MessageCoordinates coords : request.messages()) {
                ConsumerRecord<byte[], byte[]> record =
                        browseService.fetchOne(request.dlqTopic(), coords.partition(), coords.offset());

                if (record == null) {
                    skipped++;
                    items.add(ReplayResult.ItemResult.skipped(coords,
                            "Message not found (expired, compacted, or wrong coordinates)"));
                    continue;
                }

                String target = resolveTargetTopic(request, record);
                if (target == null) {
                    skipped++;
                    items.add(ReplayResult.ItemResult.skipped(coords,
                            "Cannot resolve original topic: no x-dlq-original-topic header and "
                            + "no recognized DLQ suffix. Provide targetTopic explicitly."));
                    continue;
                }

                if (request.dryRun()) {
                    succeeded++;
                    items.add(ReplayResult.ItemResult.wouldReplay(coords, target));
                    continue;
                }

                // Rate limit
                if (intervalNanos > 0) {
                    long now = System.nanoTime();
                    if (now < nextSendAt) {
                        TimeUnit.NANOSECONDS.sleep(nextSendAt - now);
                    }
                    nextSendAt = Math.max(now, nextSendAt) + intervalNanos;
                }

                try {
                    producer.send(buildReplayRecord(record, target)).get();
                    succeeded++;
                    items.add(ReplayResult.ItemResult.replayed(coords, target));
                } catch (ExecutionException e) {
                    failed++;
                    log.warn("Replay failed for {}[{}]@{}: {}", request.dlqTopic(),
                            coords.partition(), coords.offset(), e.getCause().toString());
                    items.add(ReplayResult.ItemResult.failed(coords, e.getCause().toString()));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaAccessException("Interrupted during replay", e);
        }

        return new ReplayResult(request.dryRun(), succeeded, failed, skipped, items);
    }

    private String resolveTargetTopic(ReplayRequest request, ConsumerRecord<byte[], byte[]> record) {
        if (request.targetTopic() != null && !request.targetTopic().isBlank()) {
            return request.targetTopic();
        }
        Header original = record.headers().lastHeader(DlqHeaders.ORIGINAL_TOPIC);
        if (original != null && original.value() != null) {
            return new String(original.value(), StandardCharsets.UTF_8);
        }
        return topicDiscoveryService.inferOriginalTopic(record.topic());
    }

    private ProducerRecord<byte[], byte[]> buildReplayRecord(
            ConsumerRecord<byte[], byte[]> source, String targetTopic) {

        // Key is preserved so partition-by-key semantics hold on the target.
        ProducerRecord<byte[], byte[]> out = new ProducerRecord<>(
                targetTopic, null, source.key(), source.value());

        source.headers().forEach(h -> out.headers().add(h));

        int replayCount = 1;
        Header previous = out.headers().lastHeader(DlqHeaders.REPLAY_COUNT);
        if (previous != null && previous.value() != null) {
            try {
                replayCount = Integer.parseInt(
                        new String(previous.value(), StandardCharsets.UTF_8)) + 1;
            } catch (NumberFormatException ignored) {
                // keep 1
            }
        }
        setHeader(out, DlqHeaders.REPLAY_COUNT, String.valueOf(replayCount));
        setHeader(out, DlqHeaders.REPLAYED_AT, String.valueOf(System.currentTimeMillis()));
        return out;
    }

    private static void setHeader(ProducerRecord<byte[], byte[]> record, String key, String value) {
        record.headers().remove(key);
        record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    private Producer<byte[], byte[]> newProducer() {
        Properties props = new Properties();
        props.putAll(kafkaAdmin.getConfigurationProperties());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return new KafkaProducer<>(props);
    }
}
