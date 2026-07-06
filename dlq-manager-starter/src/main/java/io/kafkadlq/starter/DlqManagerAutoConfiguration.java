package io.kafkadlq.starter;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Auto-configures an {@link EnrichedDeadLetterPublishingRecoverer} and a
 * {@link DefaultErrorHandler} that uses it, unless the application defines
 * its own.
 *
 * <p>Disable entirely with {@code dlq-manager.enabled=false}.
 */
@AutoConfiguration
@ConditionalOnClass(KafkaTemplate.class)
@ConditionalOnProperty(prefix = "dlq-manager", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(DlqManagerStarterProperties.class)
public class DlqManagerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(DeadLetterPublishingRecoverer.class)
    public EnrichedDeadLetterPublishingRecoverer enrichedDeadLetterPublishingRecoverer(
            KafkaOperations<Object, Object> kafkaTemplate,
            DlqManagerStarterProperties properties,
            Environment environment) {

        String application = firstNonBlank(
                properties.getApplicationName(),
                environment.getProperty("spring.application.name"),
                "unknown-application");
        String group = firstNonBlank(
                properties.getConsumerGroup(),
                environment.getProperty("spring.kafka.consumer.group-id"),
                "unknown-group");

        return new EnrichedDeadLetterPublishingRecoverer(kafkaTemplate, application, group);
    }

    @Bean
    @ConditionalOnMissingBean(DefaultErrorHandler.class)
    @ConditionalOnProperty(prefix = "dlq-manager", name = "auto-error-handler", havingValue = "true", matchIfMissing = true)
    public DefaultErrorHandler dlqDefaultErrorHandler(
            EnrichedDeadLetterPublishingRecoverer recoverer,
            DlqManagerStarterProperties properties,
            ObjectProvider<ExponentialBackOff> backOffProvider) {

        ExponentialBackOff backOff = backOffProvider.getIfAvailable(() -> {
            ExponentialBackOff bo = new ExponentialBackOff(
                    properties.getRetry().getInitialIntervalMs(),
                    properties.getRetry().getMultiplier());
            bo.setMaxElapsedTime(properties.getRetry().getMaxElapsedMs());
            return bo;
        });
        return new DefaultErrorHandler(recoverer, backOff);
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) {
                return v;
            }
        }
        return null;
    }
}
