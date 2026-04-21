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
import io.airlift.units.DataSize;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.connector.DynamicFilter;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestFilterAndProjectOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;

    @BeforeEach
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterEach
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void test()
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .addSequencePage(100, 0, 0)
                .build();

        TestingFunctionResolution functionResolution = new TestingFunctionResolution();

        Reference col0 = new Reference(VARCHAR, "$col_0");
        Reference col1 = new Reference(BIGINT, "$col_1");
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(VARCHAR, "$col_0"), 0,
                new Symbol(BIGINT, "$col_1"), 1);

        Expression filter = call(
                functionResolution.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(BIGINT, BIGINT)),
                col1, new Constant(BIGINT, 9L));

        Expression field0 = col0;
        Expression add5 = call(
                functionResolution.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT)),
                col1, new Constant(BIGINT, 5L));

        ExpressionCompiler compiler = functionResolution.getExpressionCompiler();
        Function<DynamicFilter, PageProcessor> processorFactory = compiler.compilePageProcessor(true, Optional.of(filter), Optional.empty(), ImmutableList.of(field0, add5), layout, Optional.empty(), OptionalInt.empty());
        Supplier<PageProcessor> processor = () -> processorFactory.apply(DynamicFilter.EMPTY);

        OperatorFactory operatorFactory = FilterAndProjectOperator.createOperatorFactory(
                0,
                new PlanNodeId("test"),
                processor,
                ImmutableList.of(VARCHAR, BIGINT),
                DataSize.ofBytes(0),
                0);

        MaterializedResult expected = MaterializedResult.resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("0", 5L)
                .row("1", 6L)
                .row("2", 7L)
                .row("3", 8L)
                .row("4", 9L)
                .row("5", 10L)
                .row("6", 11L)
                .row("7", 12L)
                .row("8", 13L)
                .row("9", 14L)

                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testMergeOutput()
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .addSequencePage(100, 0, 0)
                .addSequencePage(100, 0, 0)
                .addSequencePage(100, 0, 0)
                .addSequencePage(100, 0, 0)
                .build();

        TestingFunctionResolution functionResolution = new TestingFunctionResolution();

        Reference col1 = new Reference(BIGINT, "$col_1");
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(BIGINT, "$col_1"), 1);

        Expression filter = call(
                functionResolution.resolveOperator(EQUAL, ImmutableList.of(BIGINT, BIGINT)),
                col1, new Constant(BIGINT, 10L));

        ExpressionCompiler compiler = functionResolution.getExpressionCompiler();
        Function<DynamicFilter, PageProcessor> processorFactory = compiler.compilePageProcessor(true, Optional.of(filter), Optional.empty(), ImmutableList.of(col1), layout, Optional.empty(), OptionalInt.empty());
        Supplier<PageProcessor> processor = () -> processorFactory.apply(DynamicFilter.EMPTY);

        OperatorFactory operatorFactory = FilterAndProjectOperator.createOperatorFactory(
                0,
                new PlanNodeId("test"),
                processor,
                ImmutableList.of(BIGINT),
                DataSize.of(64, KILOBYTE),
                2);

        List<Page> expected = rowPagesBuilder(BIGINT)
                .row(10L)
                .row(10L)
                .row(10L)
                .row(10L)
                .build();

        assertOperatorEquals(operatorFactory, ImmutableList.of(BIGINT), driverContext, input, expected);
    }
}
