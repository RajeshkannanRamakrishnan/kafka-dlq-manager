package io.kafkadlq.demo;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Demo service for the docker-compose quickstart.
 *
 * <p>Every second it produces an order to the {@code orders} topic. The
 * listener deliberately fails on ~30% of them with a mix of exception types,
 * so within a minute {@code orders.DLT} contains a nicely varied set of
 * dead letters for the DLQ manager to display, group, and replay.
 *
 * <p>The dlq-manager-starter auto-configures the enriched recoverer and error
 * handler — this class contains zero DLQ wiring, which is the point.
 */
@SpringBootApplication
@EnableScheduling
public class DemoFailingConsumerApplication {

    private static final Logger log = LoggerFactory.getLogger(DemoFailingConsumerApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(DemoFailingConsumerApplication.class, args);
    }

    @Bean
    public NewTopic ordersTopic() {
        return new NewTopic("orders", 3, (short) 1);
    }

    @Bean
    public NewTopic ordersDlt() {
        return new NewTopic("orders.DLT", 3, (short) 1);
    }

    private final KafkaTemplate<Object, Object> template;

    public DemoFailingConsumerApplication(KafkaTemplate<Object, Object> template) {
        this.template = template;
    }

    @Scheduled(fixedRate = 1_000)
    public void produceOrder() {
        String orderId = UUID.randomUUID().toString().substring(0, 8);
        int amount = ThreadLocalRandom.current().nextInt(100, 100_000);
        // ~10% of payloads are malformed on purpose (missing closing brace)
        boolean malformed = ThreadLocalRandom.current().nextInt(10) == 0;
        String payload = malformed
                ? "{\"orderId\":\"" + orderId + "\",\"amountPaise\":" + amount
                : "{\"orderId\":\"" + orderId + "\",\"amountPaise\":" + amount + "}";
        template.send("orders", orderId, payload);
    }

    @KafkaListener(topics = "orders", groupId = "orders-processor")
    public void onOrder(String value) {
        if (!value.endsWith("}")) {
            throw new IllegalArgumentException("Malformed order JSON: " + preview(value));
        }
        int roll = ThreadLocalRandom.current().nextInt(100);
        if (roll < 10) {
            throw new IllegalStateException("Inventory service unavailable");
        }
        if (roll < 20) {
            throw new UnsupportedOperationException("Order version not supported by this consumer");
        }
        log.info("Processed order {}", preview(value));
    }

    private static String preview(String s) {
        return s.length() <= 60 ? s : s.substring(0, 60) + "...";
    }
}
