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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.Iterators.getOnlyElement;
import static io.trino.SequencePageBuilder.createSequencePage;
import static io.trino.SequencePageBuilder.createSequencePageWithDictionaryBlocks;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;

public class TestColumnarPageProcessor
{
    private static final int POSITIONS = 100;
    private final List<Type> types = ImmutableList.of(BIGINT, VARCHAR);

    @Test
    public void testProcess()
    {
        PageProcessor processor = newPageProcessor();
        Page page = createPage(types, false);
        Page outputPage = getOnlyElement(
                processor.process(
                        SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(page)))
                .orElseThrow(() -> new AssertionError("page is not present"));
        Page expectedPage = new Page(page.getBlock(3), page.getBlock(1));
        assertPageEquals(types, outputPage, expectedPage);
    }

    @Test
    public void testProcessWithDictionary()
    {
        PageProcessor processor = newPageProcessor();
        Page page = createPage(types, true);
        Page outputPage = getOnlyElement(
                processor.process(
                        SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        SourcePage.create(page)))
                .orElseThrow(() -> new AssertionError("page is not present"));
        Page expectedPage = new Page(page.getBlock(3), page.getBlock(1));
        assertPageEquals(types, outputPage, expectedPage);
    }

    private static Page createPage(List<? extends Type> types, boolean dictionary)
    {
        Page data = dictionary ? createSequencePageWithDictionaryBlocks(types, POSITIONS) : createSequencePage(types, POSITIONS);
        // Place data at out-of-order channels: $col_0 (BIGINT) at channel 3, $col_1 (VARCHAR) at channel 1
        Block dummyBlock = createLongSequenceBlock(0, POSITIONS);
        return new Page(dummyBlock, data.getBlock(1), dummyBlock, data.getBlock(0));
    }

    private PageProcessor newPageProcessor()
    {
        TestingFunctionResolution functionResolution = new TestingFunctionResolution();
        ExpressionCompiler compiler = functionResolution.getExpressionCompiler();
        List<Expression> projections = ImmutableList.of(
                new Reference(types.get(0), "$col_0"),
                new Reference(types.get(1), "$col_1"));
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(types.get(0), "$col_0"), 3,
                new Symbol(types.get(1), "$col_1"), 1);
        return compiler.compilePageProcessor(true, Optional.empty(), Optional.empty(), projections, layout, Optional.empty(), OptionalInt.of(MAX_BATCH_SIZE))
                .apply(DynamicFilter.EMPTY);
    }
}
