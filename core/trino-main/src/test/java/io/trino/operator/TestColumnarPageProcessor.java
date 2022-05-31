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
import io.trino.metadata.FunctionManager;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterators.getOnlyElement;
import static io.trino.SequencePageBuilder.createSequencePage;
import static io.trino.SequencePageBuilder.createSequencePageWithDictionaryBlocks;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingConnectorSession.SESSION;

public class TestColumnarPageProcessor
{
    private static final int POSITIONS = 100;
    private final List<Type> types = ImmutableList.of(BIGINT, VARCHAR);
    private final FunctionManager functionManager = createTestingFunctionManager();

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
                        page))
                .orElseThrow(() -> new AssertionError("page is not present"));
        assertPageEquals(types, outputPage, page);
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
                        page))
                .orElseThrow(() -> new AssertionError("page is not present"));
        assertPageEquals(types, outputPage, page);
    }

    private static Page createPage(List<? extends Type> types, boolean dictionary)
    {
        return dictionary ? createSequencePageWithDictionaryBlocks(types, POSITIONS) : createSequencePage(types, POSITIONS);
    }

    private PageProcessor newPageProcessor()
    {
        return new ExpressionCompiler(functionManager, new PageFunctionCompiler(functionManager, 0))
                .compilePageProcessor(Optional.empty(), ImmutableList.of(field(0, types.get(0)), field(1, types.get(1))), MAX_BATCH_SIZE).get();
    }
}
