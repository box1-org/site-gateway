package org.box1.gateway.agent;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.gateway.config.SiteConfig;
import org.box1.gateway.outbox.BufferMonitor;
import org.box1.gateway.outbox.BufferStats;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * The site agent (box1-docs#130): every 15 seconds, PUT /api/v1/sites/{siteId}/status on asset-service with the
 * site's own client token. A failed heartbeat is not buffered; the next one replaces it. Logs only state changes.
 */
@ApplicationScoped
public class HeartbeatAgent {
    private static final Logger LOG = Logger.getLogger(HeartbeatAgent.class);

    private final SiteConfig config;
    private final BufferMonitor buffer;
    private final MeterRegistry registry;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final String version;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5))
            .followRedirects(HttpClient.Redirect.NEVER).build();
    private final Optional<TokenSource> tokens;
    private String lastOutcome = "";

    public HeartbeatAgent(SiteConfig config, BufferMonitor buffer, MeterRegistry registry, ObjectMapper mapper,
            Clock clock, @ConfigProperty(name = "quarkus.application.version", defaultValue = "unknown") String version) {
        this.config = config;
        this.buffer = buffer;
        this.registry = registry;
        this.mapper = mapper;
        this.clock = clock;
        this.version = version;
        SiteConfig.Agent agent = config.agent();
        if (agent.tokenUrl().isPresent() && agent.clientId().isPresent() && agent.clientSecretFile().isPresent()) {
            this.tokens = Optional.of(new TokenSource(http, mapper, clock, URI.create(agent.tokenUrl().get()),
                    agent.clientId().get(), Path.of(agent.clientSecretFile().get())));
        } else {
            this.tokens = Optional.empty();
        }
    }

    @Scheduled(every = "${gateway.agent.interval:15s}", delayed = "5s",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP, identity = "site-heartbeat",
            skipExecutionIf = AgentDisabled.class)
    void beat() {
        if (tokens.isEmpty()) {
            outcome("unconfigured", "Site agent has no Keycloak client configured; heartbeats are off");
            return;
        }
        try {
            String body = mapper.writeValueAsString(report(buffer.latest(),
                    SiteFacts.read(Path.of(config.agent().factsFile()), mapper)));
            HttpRequest request = HttpRequest.newBuilder(statusUri()).timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json").header("Accept", "application/json")
                    .header("Authorization", "Bearer " + tokens.get().token())
                    .PUT(HttpRequest.BodyPublishers.ofString(body)).build();
            HttpResponse<Void> response = http.send(request, HttpResponse.BodyHandlers.discarding());
            int status = response.statusCode();
            if (status == 401) {
                tokens.get().invalidate();
            }
            if (status / 100 == 2) {
                outcome("ok", "Site heartbeat accepted by asset-service");
            } else {
                outcome("http_" + status, "Site heartbeat refused by asset-service with HTTP " + status);
            }
        } catch (IOException error) {
            outcome("unreachable", "Site heartbeat failed: " + error.getClass().getSimpleName());
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }

    Map<String, Object> report(BufferStats stats, SiteFacts facts) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("reportedAt", clock.instant().toString());
        body.put("bufferDepth", stats.depth());
        body.put("siteEventBacklog", stats.siteEvents());
        body.put("bufferOldestAgeSeconds", stats.oldestAgeSeconds());
        body.put("readingsDropped", dropped());
        body.put("bootedImageDigest", facts.bootedImageDigest());
        body.put("stagedImageDigest", facts.stagedImageDigest());
        body.put("rollbackImageDigest", facts.rollbackImageDigest());
        body.put("greenbootStatus", facts.greenbootStatus());
        body.put("bundleDigest", facts.bundleDigest());
        body.put("appVersions", Map.of("gateway", version, "ops", version, "simulator", version));
        return body;
    }

    private long dropped() {
        return Math.round(registry.find("site.outbox.dropped").counters().stream().mapToDouble(c -> c.count()).sum());
    }

    private URI statusUri() {
        String base = config.agent().assetServiceUrl().replaceAll("/+$", "");
        return URI.create(base + "/api/v1/sites/" + config.id() + "/status");
    }

    private void outcome(String outcome, String message) {
        registry.counter("site.heartbeat", "outcome", outcome).increment();
        if (!outcome.equals(lastOutcome)) {
            if ("ok".equals(outcome)) {
                LOG.info(message);
            } else {
                LOG.warn(message);
            }
            lastOutcome = outcome;
        }
    }

    /** Skips heartbeats when the agent is turned off. */
    @ApplicationScoped
    public static class AgentDisabled implements Scheduled.SkipPredicate {
        private final SiteConfig config;

        public AgentDisabled(SiteConfig config) {
            this.config = config;
        }

        @Override
        public boolean test(io.quarkus.scheduler.ScheduledExecution execution) {
            return !config.agent().enabled();
        }
    }
}
