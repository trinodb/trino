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

import com.google.common.collect.ImmutableMap;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestColumnarFilterCompiler
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final Expression FILTER = new Comparison(GREATER_THAN, new Reference(BIGINT, "$col_0"), new Constant(BIGINT, 2L));

    @Test
    public void testCache()
    {
        ColumnarFilterCompiler compiler = FUNCTION_RESOLUTION.getColumnarFilterCompiler(100);
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 2);

        // First compile: cache miss
        compiler.generateFilter(FILTER, layout);
        assertThat(compiler.getFilterCache().getRequestCount()).isEqualTo(1);
        assertThat(compiler.getFilterCache().getLoadCount()).isEqualTo(1);

        // Second compile with same expression: cache hit
        compiler.generateFilter(FILTER, layout);
        assertThat(compiler.getFilterCache().getRequestCount()).isEqualTo(2);
        assertThat(compiler.getFilterCache().getLoadCount()).isEqualTo(1);

        // Cached filter produces correct results
        ColumnarFilter filter = compiler.generateFilter(FILTER, layout).orElseThrow().get();
        Block block = createLongSequenceBlock(0, 5);
        SourcePage page = filter.getInputChannels().getInputChannels(SourcePage.create(new Page(
                createLongSequenceBlock(0, 5),
                createLongSequenceBlock(0, 5),
                block)));
        int[] output = new int[5];
        int count = filter.filterPositionsRange(SESSION, output, 0, 5, page);
        // values > 2 at positions 3, 4
        assertThat(count).isEqualTo(2);
    }

    @Test
    public void testCacheWithDifferentLayouts()
    {
        ColumnarFilterCompiler compiler = FUNCTION_RESOLUTION.getColumnarFilterCompiler(100);
        Map<Symbol, Integer> layout1 = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 2);
        Map<Symbol, Integer> layout2 = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 3);

        Optional<Supplier<ColumnarFilter>> supplier1 = compiler.generateFilter(FILTER, layout1);
        Optional<Supplier<ColumnarFilter>> supplier2 = compiler.generateFilter(FILTER, layout2);
        assertThat(supplier1).isPresent();
        assertThat(supplier2).isPresent();

        // Verify cache hit: only one compilation despite two calls with different layouts
        assertThat(compiler.getFilterCache().getRequestCount()).isEqualTo(2);
        assertThat(compiler.getFilterCache().getLoadCount()).isEqualTo(1);

        ColumnarFilter filter1 = supplier1.get().get();
        ColumnarFilter filter2 = supplier2.get().get();

        // Source page with four columns: dummies at 0,1, data at 2 and 3
        SourcePage sourcePage = SourcePage.create(new Page(
                createLongSequenceBlock(0, 5),
                createLongSequenceBlock(0, 5),
                createLongSequenceBlock(0, 5),
                createLongSequenceBlock(10, 15)));

        // filter1 reads source column 2: values > 2 at positions 3, 4
        SourcePage page1 = filter1.getInputChannels().getInputChannels(sourcePage);
        int[] output1 = new int[5];
        assertThat(filter1.filterPositionsRange(SESSION, output1, 0, 5, page1)).isEqualTo(2);

        // filter2 reads source column 3: all values (10-14) > 2, so all 5 positions
        SourcePage page2 = filter2.getInputChannels().getInputChannels(sourcePage);
        int[] output2 = new int[5];
        assertThat(filter2.filterPositionsRange(SESSION, output2, 0, 5, page2)).isEqualTo(5);
    }

    @Test
    public void testNoCache()
    {
        ColumnarFilterCompiler compiler = FUNCTION_RESOLUTION.getColumnarFilterCompiler();
        assertThat(compiler.getFilterCache()).isNull();

        // Still produces correct results
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 2);
        ColumnarFilter filter = compiler.generateFilter(FILTER, layout).orElseThrow().get();
        Block block = createLongSequenceBlock(0, 5);
        SourcePage page = filter.getInputChannels().getInputChannels(SourcePage.create(new Page(
                createLongSequenceBlock(0, 5),
                createLongSequenceBlock(0, 5),
                block)));
        int[] output = new int[5];
        assertThat(filter.filterPositionsRange(SESSION, output, 0, 5, page)).isEqualTo(2);
    }
}
