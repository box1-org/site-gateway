package org.box1.gateway.it;

import java.util.Map;

/** The publisher off and tiny bounds, so overflow, backpressure and crash cases can be driven step by step. */
public class BoundedProfile extends LiveProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> overrides = new java.util.HashMap<>(super.getConfigOverrides());
        overrides.put("site.kafka.publisher-enabled", "false");
        overrides.put("site.kafka.pick-list-enabled", "false");
        overrides.put("site.buffer.max-readings", "5");
        overrides.put("site.buffer.max-site-events", "2");
        return overrides;
    }
}
