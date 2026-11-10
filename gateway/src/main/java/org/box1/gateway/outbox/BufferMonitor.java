package org.box1.gateway.outbox;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.gateway.support.Db;
import org.jboss.logging.Logger;

/**
 * Keeps the buffer bound (age) and the published-row retention, and caches the buffer statistics that metrics and
 * the site agent read, so a metrics scrape never queries the database.
 */
@Startup
@ApplicationScoped
public class BufferMonitor {
    private static final Logger LOG = Logger.getLogger(BufferMonitor.class);

    private final Db db;
    private final OutboxStore store;
    private final AtomicReference<BufferStats> latest = new AtomicReference<>(BufferStats.EMPTY);

    public BufferMonitor(Db db, OutboxStore store, MeterRegistry registry) {
        this.db = db;
        this.store = store;
        Gauge.builder("site.outbox.depth", latest, s -> s.get().readings()).tag("class", "reading")
                .description("Unpublished readings in the site outbox").register(registry);
        Gauge.builder("site.outbox.depth", latest, s -> s.get().siteEvents()).tag("class", "site_event")
                .description("Unpublished site events in the site outbox").register(registry);
        Gauge.builder("site.outbox.oldest.age.seconds", latest, s -> s.get().oldestAgeSeconds())
                .description("Age of the oldest unpublished outbox row").register(registry);
    }

    public BufferStats latest() {
        return latest.get();
    }

    @Scheduled(every = "${gateway.buffer.stats-interval:5s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
            identity = "buffer-stats")
    void refresh() {
        try {
            latest.set(db.inTransaction(store::stats));
        } catch (SQLException error) {
            LOG.debugf("Buffer statistics unavailable: %s", error.getMessage());
        }
    }

    @Scheduled(every = "${gateway.buffer.guard-interval:30s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
            identity = "buffer-guard")
    void guard() {
        try {
            long expired = db.inTransaction((Connection connection) -> store.dropExpiredReadings(connection));
            store.countDropped("age", expired);
            db.inTransaction((Connection connection) -> store.prunePublished(connection));
        } catch (SQLException error) {
            LOG.warnf("Buffer guard could not run: %s", error.getMessage());
        }
    }
}
