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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.stats.TDigest;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.operator.ResourceUsageTimeSeriesRecorder.ResourceUsageTimeSeriesSnapshot;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.PlanNodeId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkResourceUsageTimeSeriesRecorder
{
    private static final int OPERATIONS = 1_000;

    @State(Scope.Thread)
    public static class RecordData
    {
        @Param({"100", "500", "1000", "2000", "32000"})
        private int recordDelayMillis = 1000;

        private ResourceUsageTimeSeriesRecorder recorder;
        private TestingTicker ticker;
        private int[] tickerDelay;
        private long[] cpuTime;
        private long[] wallTime;

        @Setup
        public void setup()
        {
            ticker = new TestingTicker();
            ticker.increment(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            recorder = new ResourceUsageTimeSeriesRecorder(ticker);
            tickerDelay = new int[OPERATIONS];
            cpuTime = new long[OPERATIONS];
            wallTime = new long[OPERATIONS];
            Random random = ThreadLocalRandom.current();
            for (int i = 0; i < OPERATIONS; i++) {
                tickerDelay[i] = random.nextInt(recordDelayMillis);
                cpuTime[i] = random.nextLong(recordDelayMillis) * 1000_000L;
                wallTime[i] = random.nextLong(recordDelayMillis) * 1000_000L;
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public ResourceUsageTimeSeriesRecorder record(RecordData data)
    {
        ResourceUsageTimeSeriesRecorder recorder = data.recorder;
        for (int i = 0; i < OPERATIONS; i++) {
            data.ticker.increment(data.tickerDelay[i], TimeUnit.MILLISECONDS);
            recorder.record(data.wallTime[i], data.cpuTime[i]);
        }
        return recorder;
    }

    @State(Scope.Thread)
    public static class MergeData
    {
        @Param({"2", "10", "100"})
        private int snapshotCount = 2;

        @Param({"false", "true"})
        private boolean randomBucketWidth;

        @Param({"false", "true"})
        private boolean randomStartTime;

        private List<List<ResourceUsageTimeSeriesSnapshot>> snapshots;

        @Setup
        public void setup()
        {
            ImmutableList.Builder<List<ResourceUsageTimeSeriesSnapshot>> snapshots = ImmutableList.builder();
            Random random = ThreadLocalRandom.current();
            for (int i = 0; i < OPERATIONS; i++) {
                ImmutableList.Builder<ResourceUsageTimeSeriesSnapshot> batch = ImmutableList.builder();
                for (int j = 0; j < snapshotCount; j++) {
                    batch.add(randomSnapshot(random, randomBucketWidth, randomStartTime));
                }
                snapshots.add(batch.build());
            }

            this.snapshots = snapshots.build();
        }
    }

    private static ResourceUsageTimeSeriesSnapshot randomSnapshot(Random random, boolean randomBucketWidth, boolean randomStartTime)
    {
        long[] cpuNanosBuckets = new long[32];
        long[] wallNanosBuckets = new long[32];
        for (int i = 0; i < 32; i++) {
            cpuNanosBuckets[i] = random.nextLong(1_000_000_000);
            wallNanosBuckets[i] = random.nextLong(1_000_000_000);
        }
        int bucketWidthSeconds = randomBucketWidth ? Math.powExact(2, random.nextInt(3)) : 1;
        long startTimeEpochSeconds = randomStartTime ? random.nextLong(4) * bucketWidthSeconds : 0;
        return ResourceUsageTimeSeriesSnapshot.create(startTimeEpochSeconds, bucketWidthSeconds, cpuNanosBuckets, wallNanosBuckets);
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void merge(MergeData data, Blackhole blackhole)
    {
        for (List<ResourceUsageTimeSeriesSnapshot> batch : data.snapshots) {
            blackhole.consume(ResourceUsageTimeSeriesRecorder.merge(batch));
        }
    }

    @State(Scope.Thread)
    public static class OperatorStatsAddData
    {
        @Param({"2", "10", "100"})
        private int operatorCount = 10;

        @Param({"false", "true"})
        private boolean resourceTimeSeries;

        private List<OperatorStats> baseStats;
        private List<List<OperatorStats>> otherStats;

        @Setup
        public void setup()
        {
            Random random = ThreadLocalRandom.current();
            ImmutableList.Builder<OperatorStats> baseStats = ImmutableList.builder();
            ImmutableList.Builder<List<OperatorStats>> otherStats = ImmutableList.builder();
            for (int i = 0; i < OPERATIONS; i++) {
                baseStats.add(createOperatorStats(random));
                ImmutableList.Builder<OperatorStats> stats = ImmutableList.builder();
                for (int j = 1; j < operatorCount; j++) {
                    stats.add(createOperatorStats(random));
                }
                otherStats.add(stats.build());
            }
            this.baseStats = baseStats.build();
            this.otherStats = otherStats.build();
        }

        private OperatorStats createOperatorStats(Random random)
        {
            ImmutableMap.Builder<String, Metric<?>> metricsBuilder = ImmutableMap.<String, Metric<?>>builder().putAll(ImmutableMap.of(
                    "Input rows distribution", new TDigestHistogram(radomTDigest(random)),
                    "CPU time distribution (s)", new TDigestHistogram(radomTDigest(random)),
                    "Scheduled time distribution (s)", new TDigestHistogram(radomTDigest(random)),
                    "Blocked time distribution (s)", new TDigestHistogram(radomTDigest(random))));
            if (resourceTimeSeries) {
                metricsBuilder.put("CPU and scheduled time usage over time", randomSnapshot(random, true, true));
            }
            Metrics metrics = new Metrics(metricsBuilder.buildOrThrow());
            return new OperatorStats(
                    0, 0, 0,
                    new PlanNodeId("test"),
                    Optional.empty(),
                    "test",
                    1,
                    0, Duration.ZERO, Duration.ZERO,
                    DataSize.ofBytes(0), 0, Duration.ZERO,
                    DataSize.ofBytes(0), 0,
                    DataSize.ofBytes(0), 0, 0d,
                    0, Duration.ZERO, Duration.ZERO,
                    DataSize.ofBytes(0), 0,
                    0, metrics, Metrics.EMPTY, Metrics.EMPTY,
                    DataSize.ofBytes(0),
                    Duration.ZERO,
                    0, Duration.ZERO, Duration.ZERO,
                    DataSize.ofBytes(0), DataSize.ofBytes(0),
                    DataSize.ofBytes(0), DataSize.ofBytes(0), DataSize.ofBytes(0),
                    DataSize.ofBytes(0),
                    Optional.empty(),
                    null);
        }

        private static TDigest radomTDigest(Random random)
        {
            TDigest tdigest = new TDigest();
            for (int i = 0; i < 20; i++) {
                tdigest.add(random.nextLong(1_000_000_000));
            }
            return tdigest;
        }
    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void operatorStatsAdd(OperatorStatsAddData data, Blackhole blackhole)
    {
        for (int i = 0; i < OPERATIONS; i++) {
            blackhole.consume(data.baseStats.get(i).add(data.otherStats.get(i)));
        }
    }

    static void main()
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        RecordData recordData = new RecordData();
        recordData.setup();
        BenchmarkResourceUsageTimeSeriesRecorder benchmarkInstance = new BenchmarkResourceUsageTimeSeriesRecorder();
        benchmarkInstance.record(recordData);

        MergeData mergeData = new MergeData();
        mergeData.setup();
        Blackhole blackhole = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
        benchmarkInstance.merge(mergeData, blackhole);

        OperatorStatsAddData operatorStatsAddData = new OperatorStatsAddData();
        operatorStatsAddData.setup();
        benchmarkInstance.operatorStatsAdd(operatorStatsAddData, blackhole);

        benchmark(BenchmarkResourceUsageTimeSeriesRecorder.class)
                .withOptions(options -> options.jvmArgs("-Xmx4g")
                ).run();
    }
}
