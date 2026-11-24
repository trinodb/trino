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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.TestingIdType.ID;

@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkEquatableValueSet
{
    @Benchmark
    public ValueSet create(Data data)
    {
        return ValueSet.copyOf(ID, data.values);
    }

    @Benchmark
    public ValueSet complement(Data data)
    {
        return data.left.complement();
    }

    @Benchmark
    public ValueSet intersect(Data data)
    {
        return data.left.intersect(data.overlap);
    }

    @Benchmark
    public ValueSet union(Data data)
    {
        return data.left.union(data.overlap);
    }

    @Benchmark
    public ValueSet unionMany(Data data)
    {
        return data.left.union(data.others);
    }

    @Benchmark
    public ValueSet unionMixed(Data data)
    {
        return data.excluded.union(data.others);
    }

    @State(Scope.Thread)
    public static class Data
    {
        @Param({"16", "1000"})
        public int valueCount;

        private List<Long> values;
        private ValueSet left;
        private ValueSet overlap;
        private ValueSet excluded;
        private List<ValueSet> others;

        @Setup
        public void setup()
        {
            values = LongStream.range(0, valueCount).boxed().toList();
            left = ValueSet.copyOf(ID, values);
            overlap = ValueSet.copyOf(ID, LongStream.range(valueCount / 2, valueCount + valueCount / 2).boxed().toList());
            others = IntStream.range(1, 20)
                    .mapToObj(index -> ValueSet.copyOf(ID, LongStream.range((long) index * valueCount, (index + 1L) * valueCount).boxed().toList()))
                    .toList();
            excluded = ValueSet.copyOf(ID, LongStream.range(0, 20L * valueCount).boxed().toList()).complement();
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkEquatableValueSet.class).run();
    }
}
