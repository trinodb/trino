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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.join.JoinFilterFunction;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.QUERY_EXCEEDED_COMPILER_LIMIT;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertThatTrinoException;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class TestJoinFilterFunctionCompiler
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    // left.col > right.col
    private static final Expression JOIN_FILTER = comparison(
            GREATER_THAN,
            new Reference(BIGINT, "left_col"),
            new Reference(BIGINT, "right_col"));

    @Test
    public void testMethodTooLargeSurfacesAsTrinoException()
    {
        // a join condition wide enough that the generated filter method overflows the
        // JVM's 64KB method size limit; the compiler must translate that into a TrinoException
        // instead of letting the raw compilation failure propagate
        JoinFilterFunctionCompiler compiler = new JoinFilterFunctionCompiler(
                FUNCTION_RESOLUTION.getPlannerContext().getFunctionManager(),
                FUNCTION_RESOLUTION.getMetadata(),
                FUNCTION_RESOLUTION.getPlannerContext().getTypeManager());
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(BIGINT, "left_col"), 0,
                new Symbol(BIGINT, "right_col"), 1);
        Expression filter = comparison(GREATER_THAN, hugeSumExpression(), new Reference(BIGINT, "right_col"));

        Throwable thrown = catchThrowable(() -> compiler.compileJoinFilterFunction(filter, layout, 1, SQL_STANDARD));
        assertThatTrinoException(thrown.getCause())
                .hasErrorCode(QUERY_EXCEEDED_COMPILER_LIMIT);
        assertThat(thrown.getCause()).hasMessage("Failed to execute query; the join condition may be too complex");
    }

    // a balanced tree of 8192 leaves, large enough to overflow the generated method's bytecode
    private static Expression hugeSumExpression()
    {
        List<Expression> leaves = new ArrayList<>();
        for (int i = 0; i < 8192; i++) {
            leaves.add(i % 2 == 0 ? new Reference(BIGINT, "left_col") : new Constant(BIGINT, (long) i));
        }
        while (leaves.size() > 1) {
            List<Expression> parents = new ArrayList<>();
            for (int i = 0; i < leaves.size(); i += 2) {
                parents.add(call(FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT)), leaves.get(i), leaves.get(i + 1)));
            }
            leaves = parents;
        }
        return leaves.get(0);
    }

    @Test
    public void testCache()
    {
        JoinFilterFunctionCompiler compiler = new JoinFilterFunctionCompiler(
                FUNCTION_RESOLUTION.getPlannerContext().getFunctionManager(),
                FUNCTION_RESOLUTION.getMetadata(),
                FUNCTION_RESOLUTION.getPlannerContext().getTypeManager());

        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(BIGINT, "left_col"), 0,
                new Symbol(BIGINT, "right_col"), 1);

        // First compile: cache miss
        compiler.compileJoinFilterFunction(JOIN_FILTER, layout, 1, SQL_STANDARD);
        assertThat(compiler.getJoinFilterFunctionFactoryStats().getRequestCount()).isEqualTo(1);
        assertThat(compiler.getJoinFilterFunctionFactoryStats().getLoadCount()).isEqualTo(1);

        // Second compile with same expression and layout: cache hit
        compiler.compileJoinFilterFunction(JOIN_FILTER, layout, 1, SQL_STANDARD);
        assertThat(compiler.getJoinFilterFunctionFactoryStats().getRequestCount()).isEqualTo(2);
        assertThat(compiler.getJoinFilterFunctionFactoryStats().getLoadCount()).isEqualTo(1);

        // Compiled filter produces correct results
        JoinFilterFunctionFactory factory = compiler.compileJoinFilterFunction(JOIN_FILTER, layout, 1, SQL_STANDARD);
        Page leftPage = createLongBlockPage(10, 1, 5);
        Page rightPage = createLongBlockPage(3, 3, 3);
        // Addresses: packing (pageIndex=0, positionIndex) for the left page
        LongArrayList addresses = new LongArrayList(new long[] {0, 1, 2}); // positions 0, 1, 2 of page 0
        JoinFilterFunction filterFunction = factory.create(SESSION, addresses, List.of(leftPage));
        // left[0]=10 > right[0]=3 → true
        assertThat(filterFunction.filter(0, 0, rightPage)).isTrue();
        // left[1]=1 > right[1]=3 → false
        assertThat(filterFunction.filter(1, 1, rightPage)).isFalse();
        // left[2]=5 > right[2]=3 → true
        assertThat(filterFunction.filter(2, 2, rightPage)).isTrue();
    }

    @Test
    public void testCacheWithDifferentSymbolNames()
    {
        // Different symbol names but same field positions should share a cache entry
        JoinFilterFunctionCompiler compiler = new JoinFilterFunctionCompiler(
                FUNCTION_RESOLUTION.getPlannerContext().getFunctionManager(),
                FUNCTION_RESOLUTION.getMetadata(),
                FUNCTION_RESOLUTION.getPlannerContext().getTypeManager());

        Expression filter1 = comparison(
                GREATER_THAN,
                new Reference(BIGINT, "a"),
                new Reference(BIGINT, "b"));
        Map<Symbol, Integer> layout1 = ImmutableMap.of(
                new Symbol(BIGINT, "a"), 0,
                new Symbol(BIGINT, "b"), 1);

        Expression filter2 = comparison(
                GREATER_THAN,
                new Reference(BIGINT, "x"),
                new Reference(BIGINT, "y"));
        Map<Symbol, Integer> layout2 = ImmutableMap.of(
                new Symbol(BIGINT, "x"), 0,
                new Symbol(BIGINT, "y"), 1);

        compiler.compileJoinFilterFunction(filter1, layout1, 1, SQL_STANDARD);
        compiler.compileJoinFilterFunction(filter2, layout2, 1, SQL_STANDARD);

        // Same positions → cache hit, only one compilation
        assertThat(compiler.getJoinFilterFunctionFactoryStats().getRequestCount()).isEqualTo(2);
        assertThat(compiler.getJoinFilterFunctionFactoryStats().getLoadCount()).isEqualTo(1);
    }

    @Test
    public void testCacheWithDifferentPositions()
    {
        // Same symbol names but different field positions should NOT share a cache entry
        JoinFilterFunctionCompiler compiler = new JoinFilterFunctionCompiler(
                FUNCTION_RESOLUTION.getPlannerContext().getFunctionManager(),
                FUNCTION_RESOLUTION.getMetadata(),
                FUNCTION_RESOLUTION.getPlannerContext().getTypeManager());

        Map<Symbol, Integer> layout1 = ImmutableMap.of(
                new Symbol(BIGINT, "left_col"), 0,
                new Symbol(BIGINT, "right_col"), 1);

        Map<Symbol, Integer> layout2 = ImmutableMap.of(
                new Symbol(BIGINT, "left_col"), 1,
                new Symbol(BIGINT, "right_col"), 2);

        compiler.compileJoinFilterFunction(JOIN_FILTER, layout1, 1, SQL_STANDARD);
        compiler.compileJoinFilterFunction(JOIN_FILTER, layout2, 2, SQL_STANDARD);

        // Different positions → cache miss, two compilations
        assertThat(compiler.getJoinFilterFunctionFactoryStats().getRequestCount()).isEqualTo(2);
        assertThat(compiler.getJoinFilterFunctionFactoryStats().getLoadCount()).isEqualTo(2);
    }

    @Test
    public void testFilterTemplateReuse()
    {
        JoinFilterFunctionCompiler compiler = new JoinFilterFunctionCompiler(
                FUNCTION_RESOLUTION.getPlannerContext().getFunctionManager(),
                FUNCTION_RESOLUTION.getMetadata(),
                FUNCTION_RESOLUTION.getPlannerContext().getTypeManager());

        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(BIGINT, "left_col"), 0,
                new Symbol(BIGINT, "right_col"), 1);

        // same structure with different constants shares one compiled template
        // left[0]=10 > 3 + right[0]=3 → true; 10 > 8 + 3 → false
        assertThat(filterWithThreshold(compiler, layout, 3)).isTrue();
        assertThat(filterWithThreshold(compiler, layout, 8)).isFalse();

        // two lookups, one template stored on the first miss and hit by the second
        assertThat(compiler.getJoinFilterTemplateCache().getRequestCount()).isEqualTo(2);
        assertThat(compiler.getJoinFilterTemplateCache().size()).isEqualTo(1);
        assertThat(compiler.getJoinFilterTemplateCache().getHitRate()).isEqualTo(0.5);
    }

    private static boolean filterWithThreshold(JoinFilterFunctionCompiler compiler, Map<Symbol, Integer> layout, long threshold)
    {
        // left_col > threshold + right_col
        Expression filter = comparison(
                GREATER_THAN,
                new Reference(BIGINT, "left_col"),
                call(
                        FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT)),
                        new Constant(BIGINT, threshold),
                        new Reference(BIGINT, "right_col")));
        JoinFilterFunctionFactory factory = compiler.compileJoinFilterFunction(filter, layout, 1, SQL_STANDARD);

        Page leftPage = createLongBlockPage(10);
        Page rightPage = createLongBlockPage(3);
        LongArrayList addresses = new LongArrayList(new long[] {0});
        JoinFilterFunction filterFunction = factory.create(SESSION, addresses, List.of(leftPage));
        return filterFunction.filter(0, 0, rightPage);
    }

    private static Page createLongBlockPage(long... values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            BIGINT.writeLong(builder, value);
        }
        return new Page(builder.build());
    }
}
