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
package io.trino.operator.index;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.trino.metadata.FunctionManager;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.type.BlockTypeOperators;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalInt;

import static com.google.common.collect.Iterators.getOnlyElement;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;

public class TestTupleFilterProcessor
{
    @Test
    public void testFilter()
    {
        Page tuplePage = Iterables.getOnlyElement(rowPagesBuilder(BIGINT, VARCHAR, DOUBLE)
                .row(1L, "a", 0.1)
                .build());

        List<Type> outputTypes = ImmutableList.of(VARCHAR, BIGINT, BOOLEAN, DOUBLE, DOUBLE);

        Page inputPage = Iterables.getOnlyElement(rowPagesBuilder(outputTypes)
                .row("a", 1L, true, 0.1, 0.0)
                .row("b", 1L, true, 0.1, 2.0)
                .row("a", 1L, false, 0.1, 2.0)
                .row("a", 0L, false, 0.2, 0.2)
                .build());

        FunctionManager functionManager = createTestingFunctionManager();

        DynamicTupleFilterFactory filterFactory = new DynamicTupleFilterFactory(
                42,
                new PlanNodeId("42"),
                new int[] {0, 1, 2},
                new int[] {1, 0, 3},
                outputTypes,
                new PageFunctionCompiler(functionManager, 0),
                new BlockTypeOperators(new TypeOperators()));
        PageProcessor tupleFilterProcessor = filterFactory.createPageProcessor(tuplePage, OptionalInt.of(MAX_BATCH_SIZE)).get();
        Page actualPage = getOnlyElement(
                tupleFilterProcessor.process(
                        SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        inputPage))
                .orElseThrow(() -> new AssertionError("page is not present"));

        Page expectedPage = Iterables.getOnlyElement(rowPagesBuilder(outputTypes)
                .row("a", 1L, true, 0.1, 0.0)
                .row("a", 1L, false, 0.1, 2.0)
                .build());

        assertPageEquals(outputTypes, actualPage, expectedPage);
    }
}
