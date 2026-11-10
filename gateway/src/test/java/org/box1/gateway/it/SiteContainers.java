package org.box1.gateway.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * PostgreSQL, Kafka and ActiveMQ Artemis (MQTT) for the containers suite. Upstream images stand in for the site's
 * rhel10/postgresql and AMQ Broker quadlets; the protocols are the same. Static, so one test can pause one of them.
 */
public class SiteContainers implements QuarkusTestResourceLifecycleManager {
    static PostgreSQLContainer postgres;
    static KafkaContainer kafka;
    static GenericContainer<?> artemis;

    @Override
    public Map<String, String> start() {
        postgres = new PostgreSQLContainer(DockerImageName.parse("docker.io/library/postgres:16-alpine").asCompatibleSubstituteFor("postgres"))
                .withDatabaseName("site").withUsername("site").withPassword("containers-test-only");
        kafka = new KafkaContainer(DockerImageName.parse("docker.io/apache/kafka:4.2.1").asCompatibleSubstituteFor("apache/kafka"));
        artemis = new GenericContainer<>(DockerImageName.parse("docker.io/apache/activemq-artemis:2.42.0"))
                .withEnv("ANONYMOUS_LOGIN", "true").withExposedPorts(1883)
                .waitingFor(Wait.forLogMessage(".*AMQ221007.*", 1).withStartupTimeout(Duration.ofMinutes(2)));
        postgres.start();
        kafka.start();
        artemis.start();
        Properties admin = new Properties();
        admin.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (Admin client = Admin.create(admin)) {
            client.createTopics(List.of(new NewTopic("box1.telemetry.events.v1", 3, (short) 1),
                    new NewTopic("box1.site.events.v1", 3, (short) 1),
                    new NewTopic("box1.inventory.events.v1", 3, (short) 1),
                    new NewTopic("box1.fulfillment.events.v1", 3, (short) 1))).all().get();
        } catch (Exception error) {
            throw new IllegalStateException("Topic creation failed", error);
        }
        return Map.ofEntries(
                Map.entry("quarkus.datasource.jdbc.url", postgres.getJdbcUrl()),
                Map.entry("quarkus.datasource.username", postgres.getUsername()),
                Map.entry("quarkus.datasource.password", postgres.getPassword()),
                Map.entry("site.kafka.bootstrap-servers", kafka.getBootstrapServers()),
                Map.entry("site.mqtt.broker-url", "tcp://" + artemis.getHost() + ":" + artemis.getMappedPort(1883)));
    }

    @Override
    public void stop() {
        for (AutoCloseable container : new AutoCloseable[] {artemis, kafka, postgres}) {
            try {
                if (container != null) {
                    container.close();
                }
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    static String mqttUrl() {
        return "tcp://" + artemis.getHost() + ":" + artemis.getMappedPort(1883);
    }

    static void pause(GenericContainer<?> container) {
        container.getDockerClient().pauseContainerCmd(container.getContainerId()).exec();
    }

    static void unpause(GenericContainer<?> container) {
        container.getDockerClient().unpauseContainerCmd(container.getContainerId()).exec();
    }
}
