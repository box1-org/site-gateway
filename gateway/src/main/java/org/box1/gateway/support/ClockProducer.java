package org.box1.gateway.support;

import java.time.Clock;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/** One clock for the whole gateway, replaceable in tests. */
@ApplicationScoped
public class ClockProducer {
    @Produces
    @ApplicationScoped
    Clock clock() {
        return Clock.systemUTC();
    }
}
