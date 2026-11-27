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
package io.trino.spi.predicate;

import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;

@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkDiscreteRangeStorage
{
    @Benchmark
    public SortedRangeSet create(Data data)
    {
        return SortedRangeSet.of(data.type, data.values);
    }

    @Benchmark
    public SortedRangeSet intersect(Data data)
    {
        return data.left.intersect(data.right);
    }

    @Benchmark
    public SortedRangeSet union(Data data)
    {
        return data.left.union(data.right);
    }

    @Benchmark
    public ValueSet unionMany(Data data)
    {
        return data.left.union(data.others);
    }

    @Benchmark
    public SortedRangeSet intersectRange(Data data)
    {
        return data.left.intersect(data.interval);
    }

    @Benchmark
    public SortedRangeSet intersectIntervals(Data data)
    {
        return data.left.intersect(data.intervals);
    }

    @Benchmark
    public Block legacyBounds(Data data)
    {
        return data.left.getSortedRanges();
    }

    @State(Scope.Thread)
    public static class Data
    {
        @Param({"BIGINT", "VARCHAR", "DOUBLE"})
        public String typeName;

        @Param({"1000", "10000"})
        public int valueCount;

        private Type type;
        private List<Object> values;
        private SortedRangeSet left;
        private SortedRangeSet right;
        private SortedRangeSet interval;
        private SortedRangeSet intervals;
        private List<ValueSet> others;

        @Setup
        public void setup()
        {
            type = switch (typeName) {
                case "BIGINT" -> BIGINT;
                case "VARCHAR" -> VARCHAR;
                case "DOUBLE" -> DOUBLE;
                default -> throw new IllegalArgumentException(typeName);
            };
            values = IntStream.range(0, valueCount).mapToObj(index -> value(index * 2L)).toList();
            left = SortedRangeSet.of(type, values);
            right = SortedRangeSet.of(type, IntStream.range(0, valueCount).mapToObj(index -> value(index * 2L + valueCount)).toList());
            interval = SortedRangeSet.of(Range.range(type, value(valueCount / 2), true, value(valueCount), false));
            intervals = SortedRangeSet.copyOf(type, IntStream.range(0, valueCount)
                    .mapToObj(index -> Range.range(type, value(index * 2L), true, value(index * 2L + 1), true)).toList());
            others = IntStream.range(1, 10)
                    .mapToObj(offset -> (ValueSet) SortedRangeSet.of(type, IntStream.range(0, valueCount).mapToObj(index -> value(index * 20L + offset)).toList()))
                    .toList();
        }

        private Object value(long value)
        {
            if (type.equals(BIGINT)) {
                return value;
            }
            if (type.equals(DOUBLE)) {
                return (double) value;
            }
            return utf8Slice("%012d".formatted(value));
        }
    }

    static void main(String[] args)
            throws RunnerException
    {
        if (args.length == 1 && args[0].equals("--retained-sizes")) {
            for (String type : List.of("BIGINT", "VARCHAR", "DOUBLE")) {
                Data data = new Data();
                data.typeName = type;
                data.valueCount = 10_000;
                data.setup();
                System.out.println(type + " retained bytes: " + data.left.getRetainedSizeInBytes());
            }
            return;
        }
        benchmark(BenchmarkDiscreteRangeStorage.class).run();
    }
}
