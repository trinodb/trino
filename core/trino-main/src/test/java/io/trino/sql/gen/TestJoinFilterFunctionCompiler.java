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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.join.JoinFilterFunction;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJoinFilterFunctionCompiler
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    // left.col > right.col
    private static final Expression JOIN_FILTER = new Comparison(
            GREATER_THAN,
            new Reference(BIGINT, "left_col"),
            new Reference(BIGINT, "right_col"));

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
        compiler.compileJoinFilterFunction(JOIN_FILTER, layout, 1);
        assertThat(compiler.getJoinFilterFunctionFactoryStats().getRequestCount()).isEqualTo(1);
        assertThat(compiler.getJoinFilterFunctionFactoryStats().getLoadCount()).isEqualTo(1);

        // Second compile with same expression and layout: cache hit
        compiler.compileJoinFilterFunction(JOIN_FILTER, layout, 1);
        assertThat(compiler.getJoinFilterFunctionFactoryStats().getRequestCount()).isEqualTo(2);
        assertThat(compiler.getJoinFilterFunctionFactoryStats().getLoadCount()).isEqualTo(1);

        // Compiled filter produces correct results
        JoinFilterFunctionFactory factory = compiler.compileJoinFilterFunction(JOIN_FILTER, layout, 1);
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

        Expression filter1 = new Comparison(GREATER_THAN,
                new Reference(BIGINT, "a"), new Reference(BIGINT, "b"));
        Map<Symbol, Integer> layout1 = ImmutableMap.of(
                new Symbol(BIGINT, "a"), 0,
                new Symbol(BIGINT, "b"), 1);

        Expression filter2 = new Comparison(GREATER_THAN,
                new Reference(BIGINT, "x"), new Reference(BIGINT, "y"));
        Map<Symbol, Integer> layout2 = ImmutableMap.of(
                new Symbol(BIGINT, "x"), 0,
                new Symbol(BIGINT, "y"), 1);

        compiler.compileJoinFilterFunction(filter1, layout1, 1);
        compiler.compileJoinFilterFunction(filter2, layout2, 1);

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

        compiler.compileJoinFilterFunction(JOIN_FILTER, layout1, 1);
        compiler.compileJoinFilterFunction(JOIN_FILTER, layout2, 2);

        // Different positions → cache miss, two compilations
        assertThat(compiler.getJoinFilterFunctionFactoryStats().getRequestCount()).isEqualTo(2);
        assertThat(compiler.getJoinFilterFunctionFactoryStats().getLoadCount()).isEqualTo(2);
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
