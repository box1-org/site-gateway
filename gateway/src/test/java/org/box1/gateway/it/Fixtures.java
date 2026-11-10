package org.box1.gateway.it;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/** Helpers for the containers suite: MQTT publishing, Kafka reads and writes, outbox queries, polling. */
final class Fixtures {
    private Fixtures() {
    }

    static String reading(String assetId, String siteId, Instant measuredAt, double value) {
        return "{\"assetId\":\"" + assetId + "\",\"siteId\":\"" + siteId + "\",\"measuredAt\":\"" + measuredAt
                + "\",\"metricName\":\"vibration\",\"value\":" + value + ",\"unit\":\"mm/s\","
                + "\"traceparent\":\"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\"}";
    }

    static void mqttPublish(List<String> messages) throws Exception {
        MqttClient client = new MqttClient(SiteContainers.mqttUrl(), "test-" + UUID.randomUUID(), new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        client.connect(options);
        try {
            for (String message : messages) {
                MqttMessage mqtt = new MqttMessage(message.getBytes(StandardCharsets.UTF_8));
                mqtt.setQos(1);
                client.publish("box1/sites/site_ds9/readings", mqtt);
            }
        } finally {
            client.disconnect();
            client.close();
        }
    }

    static void kafkaSend(String topic, String key, String value) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SiteContainers.kafka.getBootstrapServers());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties, new StringSerializer(),
                new StringSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        }
    }

    /** Every record on the topic, read from the beginning with a fresh group. */
    static List<ConsumerRecord<String, String>> readAll(String topic, Duration quiet) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SiteContainers.kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties, new StringDeserializer(),
                new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            Instant until = Instant.now().plus(quiet);
            while (Instant.now().isBefore(until)) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
        }
        return records;
    }

    static long count(DataSource dataSource, String sql, Object... params) throws SQLException {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                statement.setObject(i + 1, params[i]);
            }
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getLong(1);
            }
        }
    }

    static void await(String what, Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Instant until = Instant.now().plus(timeout);
        while (Instant.now().isBefore(until)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(250);
        }
        throw new AssertionError("Timed out waiting for " + what);
    }

    static BooleanSupplier quietly(SqlCondition condition) {
        return () -> {
            try {
                return condition.test();
            } catch (SQLException error) {
                return false;
            }
        };
    }

    /** A condition that queries the database. */
    @FunctionalInterface
    interface SqlCondition {
        boolean test() throws SQLException;
    }
}
