package io.kafkadlq.server.api;

import java.util.List;
import java.util.Map;

import io.kafkadlq.server.kafka.DlqBrowseService;
import io.kafkadlq.server.kafka.KafkaAccessException;
import io.kafkadlq.server.kafka.ReplayService;
import io.kafkadlq.server.kafka.TopicDiscoveryService;
import io.kafkadlq.server.model.DlqMessage;
import io.kafkadlq.server.model.ReplayRequest;
import io.kafkadlq.server.model.ReplayResult;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * v1 REST API.
 *
 * <pre>
 * GET  /api/v1/topics                          — discovered DLQ topics with depth
 * GET  /api/v1/topics/{topic}/messages         — newest messages (limit param)
 * GET  /api/v1/topics/{topic}/failures         — failure groups (exception @ origin → count)
 * POST /api/v1/replay                          — replay by coordinates (supports dryRun)
 * </pre>
 */
@RestController
@RequestMapping("/api/v1")
public class DlqController {

    private final TopicDiscoveryService topicDiscoveryService;
    private final DlqBrowseService browseService;
    private final ReplayService replayService;

    public DlqController(TopicDiscoveryService topicDiscoveryService,
                         DlqBrowseService browseService,
                         ReplayService replayService) {
        this.topicDiscoveryService = topicDiscoveryService;
        this.browseService = browseService;
        this.replayService = replayService;
    }

    @GetMapping("/topics")
    public List<TopicDiscoveryService.DlqTopicInfo> topics() {
        return topicDiscoveryService.discover();
    }

    @GetMapping("/topics/{topic}/messages")
    public List<DlqMessage> messages(@PathVariable String topic,
                                     @RequestParam(defaultValue = "50") int limit) {
        requireDlqTopic(topic);
        return browseService.browse(topic, limit);
    }

    @GetMapping("/topics/{topic}/failures")
    public Map<String, Long> failures(@PathVariable String topic,
                                      @RequestParam(defaultValue = "500") int sampleSize) {
        requireDlqTopic(topic);
        return browseService.groupByFailure(topic, sampleSize);
    }

    @PostMapping("/replay")
    public ReplayResult replay(@Valid @RequestBody ReplayRequest request) {
        requireDlqTopic(request.dlqTopic());
        return replayService.replay(request);
    }

    /**
     * Defense-in-depth: the server only ever reads from topics that match the
     * configured DLQ patterns. This prevents the browse/replay API from being
     * used as a generic topic reader.
     */
    private void requireDlqTopic(String topic) {
        if (!topicDiscoveryService.isDlqTopic(topic)) {
            throw new IllegalArgumentException(
                    "'" + topic + "' does not match the configured DLQ topic patterns");
        }
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail badRequest(IllegalArgumentException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(KafkaAccessException.class)
    public ProblemDetail kafkaUnavailable(KafkaAccessException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_GATEWAY, e.getMessage());
    }
}
