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
package io.trino.sql;

import com.google.common.collect.ImmutableList;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Reference;
import org.junit.jupiter.api.Test;
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
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Fork(1)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkExpressionInterpreter
{
    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Benchmark)
    public static class BenchmarkData
    {
        @Param({"200", "500", "1000", "5000"})
        private int inValuesCount = 2;

        private List<Expression> expressions;

        @Setup
        public void setup()
        {
            expressions = ImmutableList.of(
                    new In(new Reference(INTEGER, "bound_value"), IntStream.range(0, inValuesCount).mapToObj(i -> new Constant(INTEGER, (long) i))
                            .collect(Collectors.toList())));
        }
    }

    @Benchmark
    public List<Object> optimize(BenchmarkData benchmarkData)
    {
        return benchmarkData.expressions.stream()
                .map(TestExpressionInterpreter::optimize)
                .collect(toImmutableList());
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        BenchmarkExpressionInterpreter benchmark = new BenchmarkExpressionInterpreter();
        assertThat(benchmark.optimize(data).size()).isEqualTo(data.expressions.size());
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkExpressionInterpreter.class, WarmupMode.BULK).run();
    }
}
