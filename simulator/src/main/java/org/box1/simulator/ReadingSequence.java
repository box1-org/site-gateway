package org.box1.simulator;

import java.util.SplittableRandom;

/**
 * Deterministic values: tick n of an asset always gives the same value for the same seed, so a run can be repeated
 * and compared. A slow sine plus seeded noise, kept between 0 and the nominal value plus twice the amplitude, which
 * stays below every warning threshold of the fixture assets.
 */
public final class ReadingSequence {
    private ReadingSequence() {
    }

    public static double value(long seed, SimulatedAsset asset, long tick) {
        long mixed = seed * 0x9E3779B97F4A7C15L ^ asset.assetId().hashCode() * 31L ^ tick;
        double noise = new SplittableRandom(mixed).nextDouble(-0.25, 0.25);
        double value = asset.nominal() + asset.amplitude() * (Math.sin(tick / 12.0) * 0.75 + noise);
        double rounded = Math.round(value * 100.0) / 100.0;
        return Math.max(0.0, rounded);
    }
}
