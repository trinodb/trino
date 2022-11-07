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
import io.trino.operator.OrderByOperator.OrderByOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingTaskContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestOrderByOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DummySpillerFactory spillerFactory;
    private final TypeOperators typeOperators = new TypeOperators();

    @DataProvider
    public static Object[][] spillEnabled()
    {
        return new Object[][] {
                {false, false, 0},
                {true, false, 8},
                {true, true, 8},
                {true, false, 0},
                {true, true, 0}};
    }

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
        spillerFactory = new DummySpillerFactory();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
        spillerFactory = null;
    }

    @Test(dataProvider = "spillEnabled")
    public void testMultipleOutputPages(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        // make operator produce multiple pages during finish phase
        int numberOfRows = 80_000;
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .addSequencePage(numberOfRows, 0, 0)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(spillerFactory),
                new OrderingCompiler(typeOperators));

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), DOUBLE);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row((double) numberOfRows - i - 1);
        }
        MaterializedResult expected = expectedBuilder.build();

        List<Page> pages = toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
        assertGreaterThan(pages.size(), 1, "Expected more than one output page");

        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());

        assertTrue(spillEnabled == (spillerFactory.getSpillsCount() > 0), format("Spill state mismatch. Expected spill: %s, spill count: %s", spillEnabled, spillerFactory.getSpillsCount()));
    }

    @Test(dataProvider = "spillEnabled")
    public void testSingleFieldKey(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(spillerFactory),
                new OrderingCompiler(typeOperators));

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE)
                .row(-0.1)
                .row(0.1)
                .row(0.2)
                .row(0.4)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testMultiFieldKey(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1L)
                .row("b", 2L)
                .pageBreak()
                .row("b", 3L)
                .row("a", 4L)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT),
                ImmutableList.of(0, 1),
                10,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(spillerFactory),
                new OrderingCompiler(typeOperators));

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = MaterializedResult.resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("a", 4L)
                .row("a", 1L)
                .row("b", 3L)
                .row("b", 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testReverseOrder(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(0),
                10,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(spillerFactory),
                new OrderingCompiler(typeOperators));

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(4L)
                .row(2L)
                .row(1L)
                .row(-1L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testMemoryLimit()
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.ofBytes(10))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                false,
                Optional.of(spillerFactory),
                new OrderingCompiler(typeOperators));

        assertThatThrownBy(() -> toPages(operatorFactory, driverContext, input))
                .isInstanceOf(ExceededMemoryLimitException.class)
                .hasMessageMatching("Query exceeded per-node memory limit of 10B.*");
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
