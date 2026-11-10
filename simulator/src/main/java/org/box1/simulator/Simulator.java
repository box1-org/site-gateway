package org.box1.simulator;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.ProducerTemplate;
import org.jboss.logging.Logger;

/**
 * Publishes one reading per fixture asset every interval (default 5 s) to box1/sites/<siteId>/readings at QoS 1 on
 * the local AMQ Broker (box1-docs#127). Each reading starts a trace, carried as traceparent in the message. A breach
 * scenario sends the asset's breach value for a number of ticks, then returns to normal, so asset-service raises one
 * alert and later clears it (box1-docs#128). A failed publish is logged and counted; the next tick continues.
 */
@ApplicationScoped
public class Simulator {
    private static final Logger LOG = Logger.getLogger(Simulator.class);

    private final SimulatorConfig config;
    private final ProducerTemplate producer;
    private final Tracer tracer;
    private final Clock clock = Clock.systemUTC();
    private final ObjectMapper mapper = new ObjectMapper();
    private final List<SimulatedAsset> assets;
    private final AtomicLong tick = new AtomicLong();
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicInteger breachTicks = new AtomicInteger();
    private volatile boolean lastFailed;

    public Simulator(SimulatorConfig config, ProducerTemplate producer, Tracer tracer) {
        this.config = config;
        this.producer = producer;
        this.tracer = tracer;
        this.assets = config.assets().map(list -> list.stream().map(SimulatedAsset::parse).toList())
                .orElseGet(() -> SimulatedAsset.fixtures(config.siteId()));
    }

    public List<SimulatedAsset> assets() {
        return assets;
    }

    /** Starts a breach: the next {@code ticks} readings of every asset carry its breach value. */
    public void breach(int ticks) {
        breachTicks.set(ticks);
        LOG.infof("Breach scenario started for %d ticks", ticks);
    }

    public Map<String, Object> status() {
        return Map.of("siteId", config.siteId(), "assets", assets.stream().map(SimulatedAsset::assetId).toList(),
                "tick", tick.get(), "published", published.get(), "failed", failed.get(),
                "breachTicksRemaining", breachTicks.get());
    }

    @Scheduled(every = "${sim.interval:5s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
            identity = "simulator", skipExecutionIf = Disabled.class)
    void publishTick() {
        long n = tick.getAndIncrement();
        boolean breaching = breachTicks.getAndUpdate(t -> Math.max(0, t - 1)) > 0;
        Instant measuredAt = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        for (SimulatedAsset asset : assets) {
            double value = breaching ? asset.breachValue() : ReadingSequence.value(config.seed(), asset, n);
            publish(asset, measuredAt, value);
        }
    }

    String message(SimulatedAsset asset, Instant measuredAt, double value, String traceparent) {
        Map<String, Object> message = new LinkedHashMap<>();
        message.put("assetId", asset.assetId());
        message.put("siteId", config.siteId());
        message.put("measuredAt", measuredAt.toString());
        message.put("metricName", asset.metricName());
        message.put("value", value);
        message.put("unit", asset.unit());
        if (traceparent != null) {
            message.put("traceparent", traceparent);
        }
        try {
            return mapper.writeValueAsString(message);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException(error);
        }
    }

    private void publish(SimulatedAsset asset, Instant measuredAt, double value) {
        Span span = tracer.spanBuilder("simulator reading").setSpanKind(SpanKind.PRODUCER)
                .setAttribute("site.id", config.siteId()).setAttribute("asset.id", asset.assetId())
                .setAttribute("metric.name", asset.metricName()).startSpan();
        try (Scope ignored = span.makeCurrent()) {
            producer.sendBody(endpoint(), message(asset, measuredAt, value, traceparent(span)));
            published.incrementAndGet();
            if (lastFailed) {
                LOG.info("Publishing to the site broker recovered");
            }
            lastFailed = false;
        } catch (RuntimeException error) {
            failed.incrementAndGet();
            span.recordException(error);
            if (!lastFailed) {
                LOG.warnf("Publishing to the site broker failed: %s", error.getMessage());
            }
            lastFailed = true;
        } finally {
            span.end();
        }
    }

    private String endpoint() {
        return "paho:box1/sites/" + config.siteId() + "/readings?brokerUrl=" + config.brokerUrl()
                + "&clientId=site-simulator-" + config.siteId() + "&qos=1&persistence=MEMORY";
    }

    private static String traceparent(Span span) {
        SpanContext context = span.getSpanContext();
        return context.isValid()
                ? "00-" + context.getTraceId() + "-" + context.getSpanId() + "-" + context.getTraceFlags().asHex() : null;
    }

    /** Skips ticks when the simulator is turned off. */
    @ApplicationScoped
    public static class Disabled implements Scheduled.SkipPredicate {
        private final SimulatorConfig config;

        public Disabled(SimulatorConfig config) {
            this.config = config;
        }

        @Override
        public boolean test(io.quarkus.scheduler.ScheduledExecution execution) {
            return !config.enabled();
        }
    }
}
