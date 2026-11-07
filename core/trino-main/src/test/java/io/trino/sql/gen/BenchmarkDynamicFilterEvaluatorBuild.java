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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.connector.TestingColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.sql.gen.columnar.ColumnarFilterCompiler;
import io.trino.sql.gen.columnar.DynamicPageFilter;
import io.trino.sql.gen.columnar.FilterEvaluator;
import io.trino.sql.planner.Symbol;
import io.trino.util.DynamicFiltersTestUtil.TestingDynamicFilter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * Measures the cost of building the dynamic filter evaluator from a {@link TupleDomain}, i.e.
 * the domain to expression translation, expression optimization and filter class generation.
 */
@State(Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class BenchmarkDynamicFilterEvaluatorBuild
{
    private static final ColumnHandle COLUMN = new TestingColumnHandle("column");

    @Param({"BIGINT", "VARCHAR"})
    public String typeName = "BIGINT";

    // number of discrete values, or 0 for a single range
    @Param({"0", "10", "1000", "50000"})
    public int size = 10;

    private Symbol symbol;
    private TupleDomain<ColumnHandle> domain;
    // the cache of generated filters is disabled, so that every build pays for class generation
    private final ColumnarFilterCompiler compiler = new ColumnarFilterCompiler(PLANNER_CONTEXT, 0);

    @Setup
    public void setup()
    {
        Type type = typeName.equals("BIGINT") ? BIGINT : VARCHAR;
        symbol = new Symbol(type, "col");
        Random random = new Random(42);
        ValueSet values;
        if (size == 0) {
            values = ValueSet.ofRanges(Range.range(type, literal(type, 1_000_000L), true, literal(type, 2_000_000L), true));
        }
        else {
            List<Object> discreteValues = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                discreteValues.add(literal(type, random.nextLong() & Long.MAX_VALUE));
            }
            values = ValueSet.copyOf(type, discreteValues);
        }
        domain = TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN, Domain.create(values, false)));
    }

    private static Object literal(Type type, long value)
    {
        if (type == BIGINT) {
            return value;
        }
        return Slices.utf8Slice("value-%016d".formatted(value));
    }

    @Benchmark
    public FilterEvaluator buildEvaluator()
    {
        TestingDynamicFilter dynamicFilter = new TestingDynamicFilter(1);
        dynamicFilter.update(domain);
        // a fresh instance for every invocation, so the cached evaluator does not hide the build cost
        DynamicPageFilter pageFilter = new DynamicPageFilter(
                PLANNER_CONTEXT,
                testSessionBuilder().build(),
                ImmutableMap.of(symbol, COLUMN),
                ImmutableMap.of(symbol, 0),
                1,
                true);
        return pageFilter.createDynamicPageFilterEvaluator(compiler, dynamicFilter).get();
    }

    static void main()
            throws Throwable
    {
        benchmark(BenchmarkDynamicFilterEvaluatorBuild.class, WarmupMode.BULK).run();
    }
}
