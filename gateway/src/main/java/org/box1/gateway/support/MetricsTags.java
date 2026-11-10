package org.box1.gateway.support;

import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.box1.gateway.config.SiteConfig;

/** Every metric carries the site, so the cluster can tell the two sites apart (label site_id). */
@Singleton
public class MetricsTags {
    @Produces
    @Singleton
    MeterFilter siteTag(SiteConfig config) {
        return MeterFilter.commonTags(Tags.of("site_id", config.id()));
    }
}
