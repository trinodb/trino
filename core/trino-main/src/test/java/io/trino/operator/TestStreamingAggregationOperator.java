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
import io.trino.RowPagesBuilder;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.spi.Page;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.tree.QualifiedName;
import io.trino.testing.MaterializedResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@Test(singleThreaded = true)
public class TestStreamingAggregationOperator
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final TestingAggregationFunction LONG_SUM = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("sum"), fromTypes(BIGINT));
    private static final TestingAggregationFunction COUNT = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("count"), ImmutableList.of());

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;
    private OperatorFactory operatorFactory;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        operatorFactory = StreamingAggregationOperator.createOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BOOLEAN, VARCHAR, BIGINT),
                ImmutableList.of(VARCHAR),
                ImmutableList.of(1),
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                        LONG_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(2), OptionalInt.empty())),
                new JoinCompiler(new TypeOperators()));
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
        OperatorFactory operatorFactory = StreamingAggregationOperator.createOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BOOLEAN, DOUBLE, BIGINT),
                ImmutableList.of(DOUBLE),
                ImmutableList.of(1),
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                        LONG_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(2), OptionalInt.empty())),
                new JoinCompiler(new TypeOperators()));

        RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(BOOLEAN, DOUBLE, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(3, 0, 0, 1)
                .row(true, 3.0, 4)
                .row(false, 3.0, 5)
                .pageBreak()
                .row(true, 3.0, 6)
                .row(false, 4.0, 7)
                .row(true, 4.0, 8)
                .row(false, 4.0, 9)
                .row(true, 4.0, 10)
                .pageBreak()
                .row(false, 5.0, 11)
                .row(true, 5.0, 12)
                .row(false, 5.0, 13)
                .row(true, 5.0, 14)
                .row(false, 5.0, 15)
                .pageBreak()
                .addSequencePage(3, 0, 6, 16)
                .row(false, Double.NaN, 1)
                .row(false, Double.NaN, 10)
                .row(false, null, 2)
                .row(false, null, 20)
                .build();

        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(0.0, 1L, 1L)
                .row(1.0, 1L, 2L)
                .row(2.0, 1L, 3L)
                .row(3.0, 3L, 15L)
                .row(4.0, 4L, 34L)
                .row(5.0, 5L, 65L)
                .row(6.0, 1L, 16L)
                .row(7.0, 1L, 17L)
                .row(8.0, 1L, 18L)
                .row(Double.NaN, 2L, 11L)
                .row(null, 2L, 22L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testLargeInputPage()
    {
        RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(BOOLEAN, VARCHAR, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(1_000_000, 0, 0, 1)
                .build();

        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT);
        for (int i = 0; i < 1_000_000; ++i) {
            expectedBuilder.row(String.valueOf(i), 1L, i + 1L);
        }

        assertOperatorEquals(operatorFactory, driverContext, input, expectedBuilder.build());
    }

    @Test
    public void testEmptyInput()
    {
        RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(BOOLEAN, VARCHAR, BIGINT);
        List<Page> input = rowPagesBuilder.build();

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT).build();
        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testSinglePage()
    {
        RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(BOOLEAN, VARCHAR, BIGINT);
        List<Page> input = rowPagesBuilder
                .row(false, "a", 5)
                .build();

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT)
                .row("a", 1L, 5L)
                .build();
        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testUniqueGroupingValues()
    {
        RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(BOOLEAN, VARCHAR, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 0, 0, 0)
                .addSequencePage(10, 0, 10, 10)
                .build();

        MaterializedResult.Builder builder = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT);
        for (int i = 0; i < 20; i++) {
            builder.row(format("%s", i), 1L, Long.valueOf(i));
        }

        assertOperatorEquals(operatorFactory, driverContext, input, builder.build());
    }

    @Test
    public void testSingleGroupingValue()
    {
        RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(BOOLEAN, VARCHAR, BIGINT);
        List<Page> input = rowPagesBuilder
                .row(true, "a", 1)
                .row(false, "a", 2)
                .row(true, "a", 3)
                .row(false, "a", 4)
                .row(true, "a", 5)
                .pageBreak()
                .row(false, "a", 6)
                .row(true, "a", 7)
                .row(false, "a", 8)
                .pageBreak()
                .pageBreak()
                .row(true, "a", 9)
                .row(false, "a", 10)
                .build();

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT)
                .row("a", 10L, 55L)
                .build();
        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }
}
