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
import io.airlift.units.DataSize;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.relational.RowExpression;
import io.trino.testing.MaterializedResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
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
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@Test(singleThreaded = true)
public class TestFilterAndProjectOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterMethod(alwaysRun = true)
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
        RowExpression filter = call(
                functionResolution.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(BIGINT, BIGINT)),
                field(1, BIGINT),
                constant(9L, BIGINT));

        RowExpression field0 = field(0, VARCHAR);
        RowExpression add5 = call(
                functionResolution.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT)),
                field(1, BIGINT),
                constant(5L, BIGINT));

        ExpressionCompiler compiler = functionResolution.getExpressionCompiler();
        Supplier<PageProcessor> processor = compiler.compilePageProcessor(Optional.of(filter), ImmutableList.of(field0, add5));

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
        RowExpression filter = call(
                functionResolution.resolveOperator(EQUAL, ImmutableList.of(BIGINT, BIGINT)),
                field(1, BIGINT),
                constant(10L, BIGINT));

        ExpressionCompiler compiler = functionResolution.getExpressionCompiler();
        Supplier<PageProcessor> processor = compiler.compilePageProcessor(Optional.of(filter), ImmutableList.of(field(1, BIGINT)));

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
