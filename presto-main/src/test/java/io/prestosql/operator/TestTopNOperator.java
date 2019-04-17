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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.ExceededMemoryLimitException;
import io.prestosql.operator.TopNOperator.TopNOperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEquals;
import static io.prestosql.spi.block.SortOrder.ASC_NULLS_LAST;
import static io.prestosql.spi.block.SortOrder.DESC_NULLS_LAST;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestTopNOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
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

    @DataProvider(name = "useWorkProcessorOperator")
    public static Object[][] useWorkProcessorOperator()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "useWorkProcessorOperator")
    public void testSingleFieldKey(boolean useWorkProcessorOperator)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .pageBreak()
                .row(5L, 0.5)
                .row(4L, 0.41)
                .row(6L, 0.6)
                .pageBreak()
                .build();

        OperatorFactory operatorFactory = topNOperatorFactory(
                useWorkProcessorOperator,
                ImmutableList.of(BIGINT, DOUBLE),
                2,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE)
                .row(6L, 0.6)
                .row(5L, 0.5)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test(dataProvider = "useWorkProcessorOperator")
    public void testMultiFieldKey(boolean useWorkProcessorOperator)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1L)
                .row("b", 2L)
                .pageBreak()
                .row("f", 3L)
                .row("a", 4L)
                .pageBreak()
                .row("d", 5L)
                .row("d", 7L)
                .row("e", 6L)
                .build();

        OperatorFactory operatorFactory = topNOperatorFactory(
                useWorkProcessorOperator,
                ImmutableList.of(VARCHAR, BIGINT),
                3,
                ImmutableList.of(0, 1),
                ImmutableList.of(DESC_NULLS_LAST, DESC_NULLS_LAST));

        MaterializedResult expected = MaterializedResult.resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("f", 3L)
                .row("e", 6L)
                .row("d", 7L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test(dataProvider = "useWorkProcessorOperator")
    public void testReverseOrder(boolean useWorkProcessorOperator)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .pageBreak()
                .row(5L, 0.5)
                .row(4L, 0.41)
                .row(6L, 0.6)
                .pageBreak()
                .build();

        OperatorFactory operatorFactory = topNOperatorFactory(
                useWorkProcessorOperator,
                ImmutableList.of(BIGINT, DOUBLE),
                2,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, DOUBLE)
                .row(-1L, -0.1)
                .row(1L, 0.1)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test(dataProvider = "useWorkProcessorOperator")
    public void testLimitZero(boolean useWorkProcessorOperator)
            throws Exception
    {
        OperatorFactory factory = topNOperatorFactory(
                useWorkProcessorOperator,
                ImmutableList.of(BIGINT),
                0,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST));

        try (Operator operator = factory.createOperator(driverContext)) {
            assertNull(operator.getOutput());
            assertTrue(operator.isFinished());
            assertFalse(operator.needsInput());
            assertNull(operator.getOutput());
        }
    }

    @Test(dataProvider = "useWorkProcessorOperator")
    public void testExceedMemoryLimit(boolean useWorkProcessorOperator)
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1L)
                .build();

        DriverContext smallDiverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(1, BYTE))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OperatorFactory operatorFactory = topNOperatorFactory(
                useWorkProcessorOperator,
                ImmutableList.of(BIGINT),
                100,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST));
        try (Operator operator = operatorFactory.createOperator(smallDiverContext)) {
            operator.addInput(input.get(0));
            operator.getOutput();
            fail("must fail because of exceeding local memory limit");
        }
        catch (ExceededMemoryLimitException ignore) {
        }
    }

    private OperatorFactory topNOperatorFactory(
            boolean workProcessorOperator,
            List<? extends Type> types,
            int n,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders)
    {
        OperatorFactory factory = new TopNOperatorFactory(
                0,
                new PlanNodeId("test"),
                types,
                n,
                sortChannels,
                sortOrders);
        if (workProcessorOperator) {
            factory = new TestWorkProcessorOperator.TestWorkProcessorOperatorFactory(
                    99,
                    new PlanNodeId("test"),
                    (WorkProcessorOperatorFactory) factory);
        }

        return factory;
    }
}
