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

package io.trino.operator.scalar;

import io.airlift.stats.TDigest;
import io.trino.jmh.Benchmarks;
import io.trino.plugin.base.metrics.DistributionSnapshot;
import io.trino.plugin.base.metrics.TDigestHistogram;
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

import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkHistogram
{
    private static final int HISTOGRAM_VALUES_SETS = 100;

    @Benchmark
    @OperationsPerInvocation(HISTOGRAM_VALUES_SETS)
    public void benchmarkTDigestSnapshot(BenchmarkData data, Blackhole bh)
    {
        for (TDigest digest : data.tDigests()) {
            bh.consume(DistributionSnapshot.fromDistribution(new TDigestHistogram(digest)));
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"10", "100", "1000"})
        private int valuesPerHistogram = 100;

        TDigest[] tDigests;

        @Setup
        public void setup()
        {
            SecureRandom random = new SecureRandom();
            tDigests = new TDigest[HISTOGRAM_VALUES_SETS];
            for (int i = 0; i < HISTOGRAM_VALUES_SETS; i++) {
                TDigest digest = new TDigest();
                for (int j = 0; j < valuesPerHistogram; j++) {
                    long value = random.nextLong(10_000_000, 10_000_000_000L);
                    digest.add(value);
                }
                tDigests[i] = digest;
            }
        }

        public TDigest[] tDigests()
        {
            return tDigests;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        Blackhole bh = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
        new BenchmarkHistogram().benchmarkTDigestSnapshot(data, bh);

        Benchmarks.benchmark(BenchmarkHistogram.class).run();
    }
}
