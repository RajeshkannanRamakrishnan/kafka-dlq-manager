package io.kafkadlq.server.kafka;

/** Wraps cluster access failures so controllers can map them to HTTP 502. */
public class KafkaAccessException extends RuntimeException {

    public KafkaAccessException(String message, Throwable cause) {
        super(message, cause);
    }
}
