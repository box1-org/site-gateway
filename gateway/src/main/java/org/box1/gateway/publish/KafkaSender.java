package org.box1.gateway.publish;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Future;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.box1.gateway.config.SiteConfig;
import org.box1.gateway.outbox.OutboxRow;

/**
 * The Kafka producer of the outbox publisher. Idempotent, acks=all, bounded timeouts so a disconnected site fails a
 * send within a minute instead of blocking. After any failure the producer is discarded and recreated, so a stale
 * connection or producer epoch never outlives an outage.
 */
@ApplicationScoped
public class KafkaSender {
    /** Header naming the producing site (SITE.md). Informational until per-site Kafka identity (box1-docs#71). */
    public static final String SITE_HEADER = "box1-site-id";

    private final SiteConfig config;
    private KafkaProducer<String, byte[]> producer;

    public KafkaSender(SiteConfig config) {
        this.config = config;
    }

    public synchronized Future<RecordMetadata> send(OutboxRow row) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(row.topic(), row.key(),
                row.envelope().getBytes(StandardCharsets.UTF_8));
        record.headers().add(SITE_HEADER, config.id().getBytes(StandardCharsets.UTF_8));
        return producer().send(record);
    }

    /** Closes the producer after a failure; the next send creates a new one. */
    public synchronized void reset() {
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
            producer = null;
        }
    }

    @PreDestroy
    synchronized void close() {
        reset();
    }

    private KafkaProducer<String, byte[]> producer() {
        if (producer == null) {
            producer = new KafkaProducer<>(properties(), new StringSerializer(), new ByteArraySerializer());
        }
        return producer;
    }

    Properties properties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka().bootstrapServers());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "site-gateway-" + config.id());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        properties.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");
        return properties;
    }
}
