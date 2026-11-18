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

import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;

@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSortedRangeSetIntersection
{
    @Benchmark
    public SortedRangeSet intersect(Data data)
    {
        return data.left.intersect(data.right);
    }

    @State(Scope.Thread)
    public static class Data
    {
        @Param({"1000", "100000"})
        public int size;

        @Param({"BINARY_SPARSE", "BINARY_DENSE", "DISCRETE_SPARSE", "DISCRETE_DENSE", "RANGE_SPARSE", "RANGE_DENSE"})
        public String shape;

        private SortedRangeSet left;
        private SortedRangeSet right;

        @Setup
        public void setup()
        {
            left = SortedRangeSet.of(BIGINT, LongStream.range(0, size).map(value -> value * 4).boxed().toList());
            right = switch (shape) {
                case "BINARY_SPARSE" -> SortedRangeSet.of(Range.range(BIGINT, 2L * size - 1, true, 2L * size + 1, true));
                case "BINARY_DENSE" -> SortedRangeSet.of(Range.range(BIGINT, (long) size, true, 3L * size, true));
                case "DISCRETE_SPARSE" -> SortedRangeSet.of(BIGINT, LongStream.range(size - 1, 2L * size - 1).map(value -> value * 4).boxed().toList());
                case "DISCRETE_DENSE" -> SortedRangeSet.of(BIGINT, LongStream.range(0, size).map(value -> value * 4).boxed().toList());
                case "RANGE_SPARSE", "RANGE_DENSE" -> {
                    left = SortedRangeSet.copyOf(BIGINT, LongStream.range(0, size)
                            .mapToObj(value -> Range.range(BIGINT, value * 4, true, value * 4 + 2, true))
                            .toList());
                    long start = shape.equals("RANGE_SPARSE") ? size - 1 : 0;
                    yield SortedRangeSet.copyOf(BIGINT, LongStream.range(start, start + size)
                            .mapToObj(value -> Range.range(BIGINT, value * 4 + 1, true, value * 4 + 3, true))
                            .toList());
                }
                default -> throw new IllegalArgumentException("Unknown shape: " + shape);
            };
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkSortedRangeSetIntersection.class).run();
    }
}
