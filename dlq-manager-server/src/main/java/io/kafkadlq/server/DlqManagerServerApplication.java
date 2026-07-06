package io.kafkadlq.server;

import io.kafkadlq.server.config.DlqManagerServerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(DlqManagerServerProperties.class)
public class DlqManagerServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DlqManagerServerApplication.class, args);
    }
}
