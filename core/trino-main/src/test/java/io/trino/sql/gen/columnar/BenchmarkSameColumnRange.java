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
package io.trino.sql.gen.columnar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.type.CharVarcharCoercion;
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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.testing.TestingConnectorSession.SESSION;

/**
 * A lower and an upper bound on one column, as a conjunction, over a column of uniformly distributed values.
 * Selectivity is the fraction of rows the range keeps; the lower bound alone keeps that fraction plus the rest above it.
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSameColumnRange
{
    private static final CharVarcharCoercion CHAR_VARCHAR_COERCION = getCharVarcharCoercion(TEST_SESSION);
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final ColumnarFilterCompiler COMPILER = FUNCTION_RESOLUTION.getColumnarFilterCompiler();
    private static final String COLUMN = "$col_0";
    private static final int POSITIONS_PER_PAGE = 8192;
    private static final int PAGES = 128;
    private static final int DISTINCT_VALUES = 100_000;

    @Param({"bigint", "double", "varchar"})
    String type;

    @Param({"0.02", "0.15", "0.5"})
    double selectivity;

    private List<SourcePage> pages;
    private FilterEvaluator evaluator;

    @Setup
    public void setup()
    {
        Type columnType = switch (type) {
            case "bigint" -> BIGINT;
            case "double" -> DOUBLE;
            case "varchar" -> VARCHAR;
            default -> throw new IllegalArgumentException(type);
        };
        Random random = new Random(42);
        ImmutableList.Builder<SourcePage> pages = ImmutableList.builder();
        for (int i = 0; i < PAGES; i++) {
            BlockBuilder builder = columnType.createBlockBuilder(null, POSITIONS_PER_PAGE);
            for (int position = 0; position < POSITIONS_PER_PAGE; position++) {
                writeValue(columnType, builder, random.nextInt(DISTINCT_VALUES));
            }
            pages.add(SourcePage.create(new Page(builder.build())));
        }
        this.pages = pages.build();

        // the range sits in the middle of the value space so the lower bound alone keeps about half the rows plus the range
        long low = (long) (DISTINCT_VALUES * (0.5 - selectivity / 2));
        long high = (long) (DISTINCT_VALUES * (0.5 + selectivity / 2));
        Reference reference = new Reference(columnType, COLUMN);
        Expression filter = new Logical(Logical.Operator.AND, ImmutableList.of(
                comparison(GREATER_THAN_OR_EQUAL, reference, constant(columnType, low)),
                comparison(LESS_THAN, reference, constant(columnType, high))));
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(columnType, COLUMN), 0);
        evaluator = FilterEvaluator.createColumnarFilterEvaluator(CHAR_VARCHAR_COERCION, filter, layout, COMPILER, true, false).orElseThrow().get();
    }

    @Benchmark
    public long filter()
    {
        long selected = 0;
        for (SourcePage page : pages) {
            selected += evaluator.evaluate(SESSION, SelectedPositions.positionsRange(0, page.getPositionCount()), page).selectedPositions().size();
        }
        return selected;
    }

    private static void writeValue(Type type, BlockBuilder builder, int value)
    {
        if (type == BIGINT) {
            BIGINT.writeLong(builder, value);
        }
        else if (type == DOUBLE) {
            DOUBLE.writeDouble(builder, value);
        }
        else {
            VARCHAR.writeSlice(builder, Slices.utf8Slice(String.format("%08d", value)));
        }
    }

    private static Constant constant(Type type, long value)
    {
        if (type == BIGINT) {
            return new Constant(BIGINT, value);
        }
        if (type == DOUBLE) {
            return new Constant(DOUBLE, (double) value);
        }
        return new Constant(VARCHAR, Slices.utf8Slice(String.format("%08d", value)));
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkSameColumnRange.class).run();
    }
}
