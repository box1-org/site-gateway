package org.box1.gateway.health;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import org.box1.gateway.support.Db;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;

/**
 * Ready only when the site PostgreSQL answers and every outbox migration packaged in this release is applied
 * (box1-docs#133 greenboot required check "outbox migrations applied"). Kafka is deliberately not checked: a site must
 * stay healthy while its link is down.
 */
@Readiness
@ApplicationScoped
public class DatabaseReadinessCheck implements HealthCheck {
    private final Db db;
    private final Instance<Flyway> flyway;
    private final boolean enabled;

    public DatabaseReadinessCheck(Db db, Instance<Flyway> flyway,
            @ConfigProperty(name = "gateway.readiness.database", defaultValue = "true") boolean enabled) {
        this.db = db;
        this.flyway = flyway;
        this.enabled = enabled;
    }

    @Override
    public HealthCheckResponse call() {
        if (!enabled) {
            return HealthCheckResponse.named("site-database").up().withData("check", "disabled").build();
        }
        try (Connection connection = db.dataSource().getConnection(); Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(2);
            statement.execute("SELECT 1");
        } catch (SQLException error) {
            return HealthCheckResponse.named("site-database").down().withData("error", error.getClass().getSimpleName())
                    .build();
        }
        MigrationInfo[] pending = flyway.get().info().pending();
        MigrationInfo current = flyway.get().info().current();
        boolean failed = current != null && current.getState().isFailed();
        return HealthCheckResponse.named("site-database").status(pending.length == 0 && !failed)
                .withData("schemaVersion", current == null ? "none" : current.getVersion().toString())
                .withData("pendingMigrations", pending.length).build();
    }
}
