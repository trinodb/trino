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
package io.trino.block;

import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkRowBlockBuilder
{
    @Benchmark
    public void benchmarkBeginBlockEntry(BenchmarkData data, Blackhole blackhole)
    {
        for (int i = 0; i < data.rows; i++) {
            data.getBlockBuilder().buildEntry(fieldBuilders -> {
                for (int fieldIndex = 0; fieldIndex < data.getTypes().size(); fieldIndex++) {
                    BIGINT.writeLong(fieldBuilders.get(fieldIndex), data.getRandom().nextLong());
                }
            });
        }
        blackhole.consume(data.getBlockBuilder());
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private RowBlockBuilder blockBuilder;
        private List<Type> types;
        @Param({"10", "100", "1000"})
        private int rows = 10;

        @Param({"2", "4", "8"})
        private int typesLength = 2;

        private Random random;

        @Setup(Level.Iteration)
        public void setup()
        {
            types = Collections.nCopies(typesLength, BIGINT);
            RowType rowType = RowType.anonymous(types);
            blockBuilder = (RowBlockBuilder) rowType.createBlockBuilder(null, rows);
            random = new Random(1024L);
        }

        public int getRows()
        {
            return rows;
        }

        public Random getRandom()
        {
            return random;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public RowBlockBuilder getBlockBuilder()
        {
            return blockBuilder;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkRowBlockBuilder.class)
                .withOptions(
                        options -> options
                                .addProfiler(GCProfiler.class))
                .run();
    }
}
