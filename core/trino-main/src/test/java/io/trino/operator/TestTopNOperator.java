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
import io.trino.ExceededMemoryLimitException;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestTopNOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;
    private final TypeOperators typeOperators = new TypeOperators();

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
    public void testSingleFieldKey()
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

    @Test
    public void testMultiFieldKey()
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

    @Test
    public void testReverseOrder()
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

    @Test
    public void testLimitZero()
            throws Exception
    {
        OperatorFactory factory = topNOperatorFactory(
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

    @Test
    public void testExceedMemoryLimit()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1L)
                .build();

        DriverContext smallDiverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.ofBytes(1))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OperatorFactory operatorFactory = topNOperatorFactory(
                ImmutableList.of(BIGINT),
                100,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST));
        Operator operator = operatorFactory.createOperator(smallDiverContext);
        operator.addInput(input.get(0));
        assertThatThrownBy(() -> operator.getOutput())
                .isInstanceOf(ExceededMemoryLimitException.class)
                .hasMessageStartingWith("Query exceeded per-node memory limit of ");
    }

    private OperatorFactory topNOperatorFactory(
            List<? extends Type> types,
            int n,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders)
    {
        return TopNOperator.createOperatorFactory(
                0,
                new PlanNodeId("test"),
                types,
                n,
                sortChannels,
                sortOrders,
                typeOperators);
    }
}
