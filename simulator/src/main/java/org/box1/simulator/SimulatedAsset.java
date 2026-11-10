package org.box1.simulator;

import java.util.List;
import java.util.regex.Pattern;

/**
 * One simulated sensor.
 *
 * @param breachValue the value sent while a breach scenario runs, above the metric's critical threshold
 */
public record SimulatedAsset(String assetId, String metricName, String unit, double nominal, double amplitude,
        double breachValue) {
    private static final Pattern ASSET_ID = Pattern.compile("^asset_[A-Za-z0-9_-]{1,64}$");

    public SimulatedAsset {
        if (!ASSET_ID.matcher(assetId).matches() || metricName.isBlank() || unit.isBlank() || amplitude < 0) {
            throw new IllegalArgumentException("Invalid simulated asset " + assetId);
        }
    }

    public static SimulatedAsset parse(String spec) {
        String[] parts = spec.split(":");
        if (parts.length != 6) {
            throw new IllegalArgumentException("Expected assetId:metric:unit:nominal:amplitude:breachValue, got " + spec);
        }
        return new SimulatedAsset(parts[0], parts[1], parts[2], Double.parseDouble(parts[3]),
                Double.parseDouble(parts[4]), Double.parseDouble(parts[5]));
    }

    /**
     * The fixture assets of a site. Nominal values and breach values follow asset-service's metric policy:
     * vibration warning 7.1, critical 11.2, clear 6.0 mm/s; temperature warning 80, critical 95, clear 75 C.
     */
    public static List<SimulatedAsset> fixtures(String siteId) {
        return switch (siteId) {
            case "site_ds9" -> List.of(new SimulatedAsset("asset_litterbox_transporter_ds9", "vibration", "mm/s", 3.0, 0.8,
                    12.5));
            case "site_spacedock" -> List.of(new SimulatedAsset("asset_replicator_spacedock", "temperature", "C", 60.0, 4.0,
                    98.0));
            default -> List.of();
        };
    }
}
