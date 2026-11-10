package org.box1.gateway.ingest;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.box1.gateway.config.SiteConfig;
import org.box1.gateway.outbox.Envelopes;
import org.box1.gateway.outbox.EventClass;
import org.box1.gateway.outbox.OutboxStore;
import org.box1.gateway.support.Db;
import org.jboss.logging.Logger;

/**
 * Turns one MQTT reading message into one outbox row (box1-docs#127). Returning normally acknowledges the MQTT
 * message, so this returns only after the row is committed, or when the message can never be valid. A database
 * outage blocks here with backoff instead of losing the reading; at shutdown it throws, so the broker redelivers.
 */
@ApplicationScoped
public class ReadingIngest {
    private static final Logger LOG = Logger.getLogger(ReadingIngest.class);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);
    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier == null ? null : carrier.get(key);
        }
    };

    private final Db db;
    private final OutboxStore store;
    private final Clock clock;
    private final MeterRegistry registry;
    private final Tracer tracer;
    private final ReadingParser parser;
    private volatile boolean running = true;

    public ReadingIngest(Db db, OutboxStore store, SiteConfig config, Clock clock, MeterRegistry registry,
            Tracer tracer) {
        this.db = db;
        this.store = store;
        this.clock = clock;
        this.registry = registry;
        this.tracer = tracer;
        this.parser = new ReadingParser(config.id());
    }

    /** Returns true when a new row was recorded, false for a duplicate or a rejected message. */
    public boolean accept(byte[] body) throws InterruptedException {
        InboundReading reading;
        try {
            reading = parser.parse(body);
        } catch (RejectedReading rejected) {
            registry.counter("site.readings.rejected", "reason", rejected.reason().toLowerCase(Locale.ROOT)).increment();
            LOG.warnf("Rejected an MQTT reading message: %s", rejected.reason());
            return false;
        }
        Context parent = reading.traceparent() == null ? Context.root()
                : W3CTraceContextPropagator.getInstance().extract(Context.root(),
                        Map.of("traceparent", reading.traceparent()), GETTER);
        Span span = tracer.spanBuilder("site-gateway reading").setParent(parent).setSpanKind(SpanKind.CONSUMER)
                .setAttribute("site.id", reading.siteId()).setAttribute("asset.id", reading.assetId())
                .setAttribute("metric.name", reading.metricName()).startSpan();
        try (Scope ignored = span.makeCurrent()) {
            UUID eventId = Envelopes.readingEventId(reading.assetId(), reading.metricName(), reading.measuredAt());
            span.setAttribute("event.id", eventId.toString());
            Instant occurredAt = clock.instant();
            Map<String, Object> envelope = envelope(eventId, reading, occurredAt, traceparent(span));
            boolean added = storeWithRetry(eventId, reading.assetId(), occurredAt, reading.measuredAt(), envelope);
            span.setAttribute("duplicate", !added);
            return added;
        } finally {
            span.end();
        }
    }

    void onStop(@Observes ShutdownEvent event) {
        running = false;
    }

    static Map<String, Object> envelope(UUID eventId, InboundReading reading, Instant occurredAt, String traceparent) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("assetId", reading.assetId());
        payload.put("siteId", reading.siteId());
        payload.put("measuredAt", reading.measuredAt().toString());
        payload.put("metricName", reading.metricName());
        payload.put("value", reading.value());
        payload.put("unit", reading.unit());
        return Envelopes.envelope(eventId, Envelopes.READING_EVENT, reading.assetId(), occurredAt,
                eventId.toString(), Envelopes.SITE_INGEST, traceparent, payload);
    }

    private boolean storeWithRetry(UUID eventId, String assetId, Instant occurredAt, Instant measuredAt,
            Map<String, Object> envelope) throws InterruptedException {
        Duration backoff = Duration.ofSeconds(1);
        while (true) {
            try {
                OutboxStore.ReadingInsert result = db.inTransaction((Connection connection) ->
                        store.addReading(connection, eventId, assetId, occurredAt, measuredAt, envelope));
                if (result.added()) {
                    store.countRecorded(EventClass.READING);
                }
                store.countDropped("size", result.dropped());
                return result.added();
            } catch (SQLException error) {
                if (!running) {
                    throw new IllegalStateException("Shutting down; the broker redelivers the reading", error);
                }
                LOG.warnf("Site database unavailable, holding the MQTT reading (retry in %ds): %s",
                        backoff.toSeconds(), error.getMessage());
                Thread.sleep(backoff.toMillis());
                backoff = backoff.multipliedBy(2).compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : backoff.multipliedBy(2);
            }
        }
    }

    private static String traceparent(Span span) {
        SpanContext context = span.getSpanContext();
        if (!context.isValid()) {
            return null;
        }
        return "00-" + context.getTraceId() + "-" + context.getSpanId() + "-" + context.getTraceFlags().asHex();
    }
}
