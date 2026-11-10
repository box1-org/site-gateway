package org.box1.gateway.ingest;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.camel.component.paho.PahoConsumer;
import org.box1.gateway.config.SiteConfig;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.jboss.logging.Logger;

/**
 * Camel's Paho consumer connects before it registers its message callback. With a durable session the broker sends
 * the stored backlog right after the connection is accepted, and Paho drops messages that arrive before the callback
 * exists; with manual acknowledgement they stay unacknowledged until the next reconnect. Once per client, after the
 * route has started, this disconnects and reconnects with the callback in place, so the broker redelivers anything
 * unacknowledged at once (the subscription is durable, so it does not need to be renewed). Later automatic reconnects
 * do not have the gap: the callback is already set.
 */
@ApplicationScoped
public class MqttSessionResync {
    private static final Logger LOG = Logger.getLogger(MqttSessionResync.class);

    private final CamelContext camel;
    private final SiteConfig config;
    private MqttClient resynced;

    public MqttSessionResync(CamelContext camel, SiteConfig config) {
        this.camel = camel;
        this.config = config;
    }

    @Scheduled(every = "${gateway.mqtt.resync-check:2s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
            identity = "mqtt-session-resync")
    void check() {
        if (!config.mqtt().enabled()) {
            return;
        }
        Route route = camel.getRoute(ReadingRoute.ROUTE_ID);
        if (route == null || !camel.getRouteController().getRouteStatus(ReadingRoute.ROUTE_ID).isStarted()
                || !(route.getConsumer() instanceof PahoConsumer consumer)) {
            return;
        }
        MqttClient client = consumer.getClient();
        if (client == null || client == resynced) {
            return;
        }
        try {
            // If an earlier attempt disconnected but could not reconnect, only the connect is retried.
            if (client.isConnected()) {
                client.disconnect(5000);
            }
            client.connect(options());
            resynced = client;
            LOG.info("MQTT session resynchronised; unacknowledged readings are redelivered");
        } catch (MqttException error) {
            LOG.warnf("MQTT session resync failed, retrying: %s", error.getMessage());
        }
    }

    /** The same session settings as {@link ReadingRoute}'s endpoint. */
    static MqttConnectOptions options() {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        options.setAutomaticReconnect(true);
        options.setConnectionTimeout(10);
        options.setKeepAliveInterval(30);
        return options;
    }
}
