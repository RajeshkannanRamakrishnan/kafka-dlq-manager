package io.kafkadlq.server.kafka;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.kafkadlq.server.config.DlqManagerServerProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

/**
 * Discovers DLQ topics on the cluster and computes their depth (total number
 * of records per topic, i.e. sum over partitions of endOffset - beginningOffset).
 *
 * <p>Depth is an upper bound: compacted or partially-expired topics may hold
 * fewer live records. Good enough for dashboards; exact counts require a scan.
 */
@Service
public class TopicDiscoveryService {

    private final KafkaAdmin kafkaAdmin;
    private final List<Pattern> dlqPatterns;
    private final List<String> dlqSuffixes;

    public TopicDiscoveryService(KafkaAdmin kafkaAdmin, DlqManagerServerProperties properties) {
        this.kafkaAdmin = kafkaAdmin;
        this.dlqPatterns = properties.getDlqTopicPatterns().stream()
                .map(Pattern::compile)
                .toList();
        this.dlqSuffixes = properties.getDlqSuffixes();
    }

    /**
     * @return all DLQ topics on the cluster with partition count and depth,
     *         sorted by depth descending (noisiest first).
     */
    public List<DlqTopicInfo> discover() {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            List<String> dlqTopicNames = admin.listTopics().names().get().stream()
                    .filter(this::isDlqTopic)
                    .sorted()
                    .toList();

            if (dlqTopicNames.isEmpty()) {
                return List.of();
            }

            Map<String, TopicDescription> descriptions =
                    admin.describeTopics(dlqTopicNames).allTopicNames().get();

            List<TopicPartition> allPartitions = descriptions.values().stream()
                    .flatMap(d -> d.partitions().stream()
                            .map(p -> new TopicPartition(d.name(), p.partition())))
                    .toList();

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest =
                    admin.listOffsets(allPartitions.stream()
                            .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.earliest()))).all().get();
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest =
                    admin.listOffsets(allPartitions.stream()
                            .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()))).all().get();

            List<DlqTopicInfo> result = new ArrayList<>();
            for (TopicDescription description : descriptions.values()) {
                long depth = 0;
                for (var p : description.partitions()) {
                    TopicPartition tp = new TopicPartition(description.name(), p.partition());
                    depth += latest.get(tp).offset() - earliest.get(tp).offset();
                }
                result.add(new DlqTopicInfo(
                        description.name(),
                        inferOriginalTopic(description.name()),
                        description.partitions().size(),
                        depth));
            }
            result.sort(Comparator.comparingLong(DlqTopicInfo::depth).reversed());
            return result;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaAccessException("Interrupted while discovering DLQ topics", e);
        } catch (ExecutionException e) {
            throw new KafkaAccessException("Failed to discover DLQ topics", e.getCause());
        }
    }

    public boolean isDlqTopic(String topic) {
        return dlqPatterns.stream().anyMatch(p -> p.matcher(topic).matches());
    }

    /** Best-effort original-topic inference from naming convention. */
    public String inferOriginalTopic(String dlqTopic) {
        for (String suffix : dlqSuffixes) {
            if (dlqTopic.endsWith(suffix)) {
                return dlqTopic.substring(0, dlqTopic.length() - suffix.length());
            }
        }
        return null;
    }

    public record DlqTopicInfo(String name, String inferredOriginalTopic, int partitions, long depth) {
    }
}
