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

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.DoubleType.DOUBLE;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkFloatingPointValueSet
{
    @Benchmark
    public boolean contains(Data data)
    {
        return data.domain.contains(data.candidateDomain);
    }

    @Benchmark
    public boolean containsRanges(Data data)
    {
        return data.ranges.contains(data.candidateRanges);
    }

    @Benchmark
    public boolean overlaps(Data data)
    {
        return data.domain.overlaps(data.candidateDomain);
    }

    @Benchmark
    public boolean overlapsRanges(Data data)
    {
        return data.ranges.overlaps(data.candidateRanges);
    }

    @Benchmark
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public ValueSet create(SetOperationData data)
    {
        return ValueSet.copyOf(DOUBLE, data.values);
    }

    @Benchmark
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public ValueSet union(SetOperationData data)
    {
        return data.left.union(data.right);
    }

    @Benchmark
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public ValueSet intersect(SetOperationData data)
    {
        return data.left.intersect(data.right);
    }

    @Benchmark
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public ValueSet unionUnbounded(SetOperationData data)
    {
        return data.below.union(data.above);
    }

    @Benchmark
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public ValueSet unionIdentity(SetOperationData data)
    {
        return data.left.union(data.left);
    }

    @Benchmark
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public Optional<Collection<Object>> expand(SetOperationData data)
    {
        return data.left.tryExpandRanges(data.valueCount);
    }

    @Benchmark
    @Warmup(iterations = 3, time = 1)
    @Measurement(iterations = 5, time = 1)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public Optional<Collection<Object>> rejectExpansion(SetOperationData data)
    {
        return data.left.tryExpandRanges(1);
    }

    @State(Scope.Thread)
    public static class Data
    {
        @Param({"1000", "10000"})
        public int pointCount;

        private Domain domain;
        private Domain candidateDomain;
        private SortedRangeSet ranges;
        private SortedRangeSet candidateRanges;

        @Setup
        public void setup()
        {
            List<Range> points = IntStream.range(0, pointCount)
                    .mapToObj(index -> Range.equal(DOUBLE, 2.0 * index))
                    .toList();
            ranges = SortedRangeSet.copyOf(DOUBLE, points);
            candidateRanges = SortedRangeSet.copyOf(DOUBLE, List.of(Range.range(DOUBLE, 0.0, true, 1.0, true)));
            domain = Domain.create(ranges, false);
            candidateDomain = Domain.create(candidateRanges, false);
        }
    }

    @State(Scope.Thread)
    public static class SetOperationData
    {
        @Param({"16", "1000"})
        public int valueCount;

        private List<Double> values;
        private ValueSet left;
        private ValueSet right;
        private ValueSet below;
        private ValueSet above;

        @Setup
        public void setup()
        {
            values = IntStream.range(0, valueCount).mapToObj(value -> (double) value).toList();
            left = ValueSet.copyOf(DOUBLE, values);
            right = ValueSet.copyOf(DOUBLE, IntStream.range(valueCount / 2, valueCount + valueCount / 2).mapToObj(value -> (double) value).toList());
            below = ValueSet.ofRanges(Range.lessThan(DOUBLE, 0.0));
            above = ValueSet.ofRanges(Range.greaterThan(DOUBLE, 1.0));
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkFloatingPointValueSet.class).run();
    }
}
