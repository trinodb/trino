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
package io.trino.array;

import io.airlift.slice.Slice;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.jmh.Benchmarks.benchmark;

@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(4)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkReferenceCountMap
{
    private static final int NUMBER_OF_ENTRIES = 1_000_000;
    private static final int NUMBER_OF_BASES = 100;

    @State(Scope.Thread)
    public static class Data
    {
        @Param("byte")
        private String arrayType = "byte";
        private Object[] bases = new Object[NUMBER_OF_BASES];
        private Slice[] slices = new Slice[NUMBER_OF_ENTRIES];

        @Setup
        public void setup()
        {
            for (int i = 0; i < NUMBER_OF_BASES; i++) {
                switch (arrayType) {
                    case "byte":
                        bases[i] = new byte[ThreadLocalRandom.current().nextInt(NUMBER_OF_BASES)];
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
                Object base = bases[ThreadLocalRandom.current().nextInt(NUMBER_OF_BASES)];
                switch (arrayType) {
                    case "byte":
                        byte[] byteBase = (byte[]) base;
                        slices[i] = wrappedBuffer(byteBase, 0, byteBase.length);
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUMBER_OF_ENTRIES)
    public ReferenceCountMap benchmarkInserts(Data data)
    {
        ReferenceCountMap map = new ReferenceCountMap();
        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            map.incrementAndGet(data.slices[i]);
            map.incrementAndGet(data.slices[i].byteArray());
        }
        return map;
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkReferenceCountMap.class, WarmupMode.BULK)
                .withOptions(optionsBuilder -> optionsBuilder
                        .addProfiler(GCProfiler.class)
                        .jvmArgs("-XX:+UseG1GC"))
                .run();
    }
}
