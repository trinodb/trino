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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;

@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkTupleDomain
{
    @Benchmark
    public TupleDomain<Integer> construct(Data data)
    {
        return TupleDomain.withColumnDomains(data.columns);
    }

    @Benchmark
    public TupleDomain<Integer> intersect(Data data)
    {
        return data.narrow.intersect(data.wide);
    }

    @Benchmark
    public TupleDomain<Integer> simplify(Data data)
    {
        return data.narrow.simplify();
    }

    @Benchmark
    public TupleDomain<Integer> filterUnchanged(Data data)
    {
        return data.narrow.filter((_, _) -> true);
    }

    @Benchmark
    public TupleDomain<Integer> transformKeys(Data data)
    {
        return data.narrow.transformKeys(key -> key + 1);
    }

    @Benchmark
    public TupleDomain<Integer> columnWiseUnion(Data data)
    {
        return TupleDomain.columnWiseUnion(data.inputs);
    }

    @Benchmark
    public Optional<TupleDomain<Integer>> strictUnion(Data data)
    {
        return TupleDomain.strictUnion(data.inputs);
    }

    @State(Scope.Thread)
    public static class Data
    {
        @Param({"1", "10", "100"})
        public int columnCount;

        private Map<Integer, Domain> columns;
        private TupleDomain<Integer> narrow;
        private TupleDomain<Integer> wide;
        private List<TupleDomain<Integer>> inputs;

        @Setup
        public void setup()
        {
            columns = new LinkedHashMap<>();
            Map<Integer, Domain> wider = new LinkedHashMap<>();
            for (int column = 0; column < columnCount; column++) {
                columns.put(column, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, true, 10L, true)), false));
                wider.put(column, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, true, 20L, true)), false));
            }
            narrow = TupleDomain.withColumnDomains(columns);
            wide = TupleDomain.withColumnDomains(wider);
            inputs = List.of(narrow, wide);
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkTupleDomain.class).run();
    }
}
