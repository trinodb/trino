/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import io.trino.spi.metrics.Metric;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.math.IntMath.isPowerOfTwo;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Records CPU and wall time usage over the lifetime of an operator as a
 * fixed-size, time-bucketed histogram.
 *
 * <p>Samples are accumulated into time buckets of equal width. The initial
 * bucket width is one second. When all buckets are full, adjacent pairs of
 * buckets are merged and the width is doubled, so the fixed bucket array
 * always covers the entire recording period at the coarsest resolution that
 * fits.
 *
 * <p>A point-in-time view of the recorded data is obtained via
 * {@link #snapshot()}. Multiple snapshots produced by independent recorders
 * (for example, one per operator instance) can be combined into a single
 * aligned histogram with {@link #merge(ResourceUsageTimeSeriesSnapshot...)}.
 * Merging finds the union time range and the coarsest bucket width across all
 * inputs, then re-bins each snapshot into that common grid before summing.
 *
 */
public class ResourceUsageTimeSeriesRecorder
{
    private static final long ONE_SECOND = 1_000_000_000L;
    private static final int DEFAULT_BUCKET_COUNT = 32;

    private final Ticker ticker;
    private final Clock clock;
    private final long[] cpuNanosBuckets;
    private final long[] wallNanosBuckets;
    private long startTimeEpochSeconds = -1;
    private long startNanos = -1;
    private long bucketWidthNanos = ONE_SECOND;
    private int size;

    public ResourceUsageTimeSeriesRecorder()
    {
        this(DEFAULT_BUCKET_COUNT, Ticker.systemTicker(), Clock.systemUTC());
    }

    @VisibleForTesting
    ResourceUsageTimeSeriesRecorder(Ticker ticker)
    {
        this(DEFAULT_BUCKET_COUNT, ticker, Clock.systemUTC());
    }

    @VisibleForTesting
    ResourceUsageTimeSeriesRecorder(int bucketCount, Ticker ticker)
    {
        this(bucketCount, ticker, Clock.systemUTC());
    }

    @VisibleForTesting
    ResourceUsageTimeSeriesRecorder(int bucketCount, Ticker ticker, Clock clock)
    {
        checkArgument(bucketCount > 0, "bucketCount must be positive");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.clock = requireNonNull(clock, "clock is null");
        this.cpuNanosBuckets = new long[bucketCount];
        this.wallNanosBuckets = new long[bucketCount];
    }

    public synchronized void record(long wallNanos, long cpuNanos)
    {
        long nowNanos = ticker.read();
        if (startTimeEpochSeconds < 0) {
            Instant now = clock.instant();
            startTimeEpochSeconds = now.getEpochSecond();
            // Set the startNanos at the beginning of the second to match startTime
            startNanos = nowNanos - now.getNano();
        }
        int bucket = (int) ((nowNanos - startNanos) / bucketWidthNanos);
        // The bucket width expansion could be implemented without a loop, but it is unlikely
        // that the loop with have more than 1 iteration and expanding by 2 is both simpler and more efficient.
        while (bucket >= cpuNanosBuckets.length) {
            bucketWidthNanos = bucketWidthNanos * 2;
            // We need to start counting on the same "unit" to match different time series,
            // so we move back start time to a multiple of bucket width
            long startNanosOffset = startNanos % bucketWidthNanos;
            startNanos = startNanos - startNanosOffset;
            bucket = (int) ((nowNanos - startNanos) / bucketWidthNanos);
            int sourceOffset = 0;
            int targetOffset = 0;
            if (startNanosOffset > 0) {
                // Because we moved back startNanos, the new first bucket is merged virtually with an empty past.
                sourceOffset = 1;
                targetOffset = 1;
            }
            for (; sourceOffset < size; sourceOffset += 2, targetOffset++) {
                cpuNanosBuckets[targetOffset] = cpuNanosBuckets[sourceOffset];
                wallNanosBuckets[targetOffset] = wallNanosBuckets[sourceOffset];
                if (sourceOffset + 1 < size) {
                    cpuNanosBuckets[targetOffset] += cpuNanosBuckets[sourceOffset + 1];
                    wallNanosBuckets[targetOffset] += wallNanosBuckets[sourceOffset + 1];
                }
            }
            for (int i = targetOffset; i < cpuNanosBuckets.length; i++) {
                cpuNanosBuckets[i] = 0;
                wallNanosBuckets[i] = 0;
            }
            size = targetOffset;
        }
        cpuNanosBuckets[bucket] += cpuNanos;
        wallNanosBuckets[bucket] += wallNanos;
        size = bucket + 1;
    }

    public synchronized ResourceUsageTimeSeriesSnapshot snapshot()
    {
        if (size == 0) {
            return ResourceUsageTimeSeriesSnapshot.EMPTY;
        }
        int bucketWidthSeconds = toIntExact(bucketWidthNanos / ONE_SECOND);
        return new ResourceUsageTimeSeriesSnapshot(
                truncateTo(startTimeEpochSeconds, bucketWidthSeconds),
                bucketWidthSeconds,
                Arrays.copyOf(cpuNanosBuckets, size),
                Arrays.copyOf(wallNanosBuckets, size));
    }

    public static ResourceUsageTimeSeriesSnapshot merge(ResourceUsageTimeSeriesSnapshot... snapshots)
    {
        return merge(ImmutableList.copyOf(snapshots));
    }

    public static ResourceUsageTimeSeriesSnapshot merge(List<ResourceUsageTimeSeriesSnapshot> snapshots)
    {
        try {
            // Each time-series snapshot may start and end at a different time, and have a different bucket width.
            // To merge them, we first calculate the earliest start time, the latest end time, and the biggest bucket width. Those will be the parameters of the merged result.
            int biggestBucketWidthSeconds = 0;
            long earliestStartTimeSeconds = Long.MAX_VALUE;
            long latestEndTimeSeconds = -1;
            for (ResourceUsageTimeSeriesSnapshot snapshot : snapshots) {
                if (snapshot.isEmpty()) {
                    continue;
                }
                if (biggestBucketWidthSeconds < snapshot.bucketWidthSeconds) {
                    biggestBucketWidthSeconds = snapshot.bucketWidthSeconds;
                }
                if (earliestStartTimeSeconds > snapshot.startTimeEpochSeconds) {
                    earliestStartTimeSeconds = snapshot.startTimeEpochSeconds;
                }
                long endTimeSeconds = snapshot.startTimeEpochSeconds + ((long) snapshot.cpuNanosBuckets.length * snapshot.bucketWidthSeconds);
                if (endTimeSeconds > latestEndTimeSeconds) {
                    latestEndTimeSeconds = endTimeSeconds;
                }
            }
            if (biggestBucketWidthSeconds < 1) {
                return ResourceUsageTimeSeriesSnapshot.EMPTY;
            }
            // Start time must be a multiple of the bucket width
            long adjustedStartTimeSeconds = earliestStartTimeSeconds - earliestStartTimeSeconds % biggestBucketWidthSeconds;

            // Use ceiling division: the last snapshot bucket may fall in the middle of a merged bucket,
            // requiring one extra merged bucket to cover it.
            int bucketCount = toIntExact((latestEndTimeSeconds - adjustedStartTimeSeconds + biggestBucketWidthSeconds - 1) / biggestBucketWidthSeconds);

            long[] finalCpuNanosBuckets = new long[bucketCount];
            long[] finalWallNanosBuckets = new long[bucketCount];

            for (ResourceUsageTimeSeriesSnapshot snapshot : snapshots) {
                if (snapshot.isEmpty()) {
                    continue;
                }
                if (snapshot.bucketWidthSeconds == biggestBucketWidthSeconds && snapshot.startTimeEpochSeconds == adjustedStartTimeSeconds) {
                    // Fast path for the simple case
                    for (int i = 0; i < snapshot.cpuNanosBuckets.length; i++) {
                        finalCpuNanosBuckets[i] += snapshot.cpuNanosBuckets[i];
                        finalWallNanosBuckets[i] += snapshot.wallNanosBuckets[i];
                    }
                }
                else {
                    // The mergeSize defines how many snapshot buckets we need to merge to the final bucket
                    int mergeSize = biggestBucketWidthSeconds / snapshot.bucketWidthSeconds;
                    int firstBucket = toIntExact((snapshot.startTimeEpochSeconds - adjustedStartTimeSeconds) / biggestBucketWidthSeconds);
                    // The first bucket merge size may be smaller than mergeSize when the snapshot starts in the middle of a merged bucket.
                    // offsetSeconds is how far into the first merged bucket the snapshot starts.
                    // The remaining part of that merged bucket holds (biggestBucketWidthSeconds - offsetSeconds) / snapshot.bucketWidthSeconds snapshot buckets.
                    // We need to clamp the firstBucketMergeSize to snapshot length in case the snapshot is shorter than the remaining capacity of the
                    // first merged bucket.
                    int offsetSeconds = toIntExact((snapshot.startTimeEpochSeconds - adjustedStartTimeSeconds) % biggestBucketWidthSeconds);
                    int firstBucketMergeSize = offsetSeconds > 0 ?
                            Math.min((biggestBucketWidthSeconds - offsetSeconds) / snapshot.bucketWidthSeconds, snapshot.cpuNanosBuckets.length) : 0;

                    if (firstBucketMergeSize > 0) {
                        for (int i = 0; i < firstBucketMergeSize; i++) {
                            finalCpuNanosBuckets[firstBucket] += snapshot.cpuNanosBuckets[i];
                            finalWallNanosBuckets[firstBucket] += snapshot.wallNanosBuckets[i];
                        }
                        firstBucket += 1;
                    }
                    for (int targetIndex = firstBucket, sourceIndex = firstBucketMergeSize; sourceIndex < snapshot.cpuNanosBuckets.length; sourceIndex += mergeSize, targetIndex++) {
                        // The last bucket can have currentMergeSize smaller than mergeSize
                        int currentMergeSize = Math.min(snapshot.cpuNanosBuckets.length - sourceIndex, mergeSize);
                        for (int i = 0; i < currentMergeSize; i++) {
                            finalCpuNanosBuckets[targetIndex] += snapshot.cpuNanosBuckets[sourceIndex + i];
                            finalWallNanosBuckets[targetIndex] += snapshot.wallNanosBuckets[sourceIndex + i];
                        }
                    }
                }
            }
            return new ResourceUsageTimeSeriesSnapshot(adjustedStartTimeSeconds, biggestBucketWidthSeconds, finalCpuNanosBuckets, finalWallNanosBuckets);
        }
        catch (RuntimeException e) {
            throw new RuntimeException("merge failed for: " + snapshots, e);
        }
    }

    private static long truncateTo(long epochSeconds, int seconds)
    {
        if (seconds == 1) {
            return epochSeconds;
        }
        return epochSeconds - epochSeconds % seconds;
    }

    private static boolean truncatedTo(long epochSeconds, int seconds)
    {
        return epochSeconds % seconds == 0;
    }

    /**
     * An immutable, point-in-time view of a {@link ResourceUsageTimeSeriesRecorder}.
     *
     * <p>Each element {@code i} of {@link #cpuNanosBuckets()} and
     * {@link #wallNanosBuckets()} represents the total nanoseconds spent in CPU
     * or wall time during the half-open interval
     * {@code [startTimeEpochSeconds + i * bucketWidthSeconds,
     *          startTimeEpochSeconds + (i+1) * bucketWidthSeconds)}.
     *
     * <p>{@code startTimeEpochSeconds} is always truncated to a multiple of
     * {@code bucketWidthSeconds}, and {@code bucketWidthSeconds} is always a
     * power of two, so snapshots from different recorders can be aligned and
     * merged without remainder arithmetic.
     *
     * <p>Implements {@link io.trino.spi.metrics.Metric} so it can be reported
     * directly as an operator metric and aggregated across tasks.
     *
     * <p>It is implemented as a class and not record to have control over construction
     * and avoid unnecessary array copies
     */
    public static class ResourceUsageTimeSeriesSnapshot
            implements Metric<ResourceUsageTimeSeriesSnapshot>
    {
        private final long startTimeEpochSeconds;
        private final int bucketWidthSeconds;
        private final long[] cpuNanosBuckets;
        private final long[] wallNanosBuckets;

        public static final ResourceUsageTimeSeriesSnapshot EMPTY = new ResourceUsageTimeSeriesSnapshot(-1, 1, new long[0], new long[0]);

        @JsonCreator
        public static ResourceUsageTimeSeriesSnapshot create(
                @JsonProperty("startTimeEpochSeconds") long startTimeEpochSeconds,
                @JsonProperty("bucketWidthSeconds") int bucketWidthSeconds,
                @JsonProperty("cpuNanosBuckets") long[] cpuNanosBuckets,
                @JsonProperty("wallNanosBuckets") long[] wallNanosBuckets)
        {
            return new ResourceUsageTimeSeriesSnapshot(
                    startTimeEpochSeconds,
                    bucketWidthSeconds,
                    cpuNanosBuckets.clone(),
                    wallNanosBuckets.clone());
        }

        private ResourceUsageTimeSeriesSnapshot(
                long startTimeEpochSeconds,
                int bucketWidthSeconds,
                long[] cpuNanosBuckets,
                long[] wallNanosBuckets)
        {
            checkArgument(bucketWidthSeconds >= 1, "bucketWidthSeconds must be >= 1");
            checkArgument(isPowerOfTwo(bucketWidthSeconds), "bucketWidthSeconds must be a power of 2");
            checkArgument(truncatedTo(startTimeEpochSeconds, bucketWidthSeconds), "startTime must be truncated to bucket width (%ss) but was %s"
                    .formatted(bucketWidthSeconds, startTimeEpochSeconds));
            this.startTimeEpochSeconds = startTimeEpochSeconds;
            this.bucketWidthSeconds = bucketWidthSeconds;
            this.cpuNanosBuckets = requireNonNull(cpuNanosBuckets, "cpuNanosBuckets is null");
            this.wallNanosBuckets = requireNonNull(wallNanosBuckets, "wallNanosBuckets is null");
        }

        @JsonProperty
        public long startTimeEpochSeconds()
        {
            return startTimeEpochSeconds;
        }

        @JsonProperty
        public int bucketWidthSeconds()
        {
            return bucketWidthSeconds;
        }

        @JsonProperty
        public long[] cpuNanosBuckets()
        {
            return cpuNanosBuckets.clone();
        }

        @JsonProperty
        public long[] wallNanosBuckets()
        {
            return wallNanosBuckets.clone();
        }

        public boolean isEmpty()
        {
            return cpuNanosBuckets.length == 0;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ResourceUsageTimeSeriesSnapshot that = (ResourceUsageTimeSeriesSnapshot) o;
            return bucketWidthSeconds == that.bucketWidthSeconds && startTimeEpochSeconds == that.startTimeEpochSeconds
                    && Arrays.equals(cpuNanosBuckets, that.cpuNanosBuckets) && Arrays.equals(wallNanosBuckets, that.wallNanosBuckets);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(startTimeEpochSeconds, bucketWidthSeconds, Arrays.hashCode(cpuNanosBuckets), Arrays.hashCode(wallNanosBuckets));
        }

        @Override
        public ResourceUsageTimeSeriesSnapshot mergeWith(ResourceUsageTimeSeriesSnapshot other)
        {
            if (!this.isEmpty() && !other.isEmpty()) {
                return merge(ImmutableList.of(this, other));
            }
            if (this.isEmpty()) {
                return other;
            }
            return this;
        }

        @Override
        public ResourceUsageTimeSeriesSnapshot mergeWith(List<ResourceUsageTimeSeriesSnapshot> others)
        {
            if (!this.isEmpty()) {
                return merge(ImmutableList.<ResourceUsageTimeSeriesSnapshot>builderWithExpectedSize(others.size() + 1)
                        .add(this)
                        .addAll(others)
                        .build());
            }

            return merge(others);
        }

        @Override
        public String toString()
        {
            return "ResourceUsageTimeSeriesSnapshot{" +
                    "startTimeEpochSeconds=" + startTimeEpochSeconds +
                    ", bucketWidthSeconds=" + bucketWidthSeconds +
                    ", cpuNanosBuckets=" + Arrays.toString(cpuNanosBuckets) +
                    ", wallNanosBuckets=" + Arrays.toString(wallNanosBuckets) +
                    '}';
        }
    }
}
