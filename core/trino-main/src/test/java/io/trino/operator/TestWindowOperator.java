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
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.trino.ExceededMemoryLimitException;
import io.trino.RowPagesBuilder;
import io.trino.operator.WindowOperator.WindowOperatorFactory;
import io.trino.operator.window.FirstValueFunction;
import io.trino.operator.window.FrameInfo;
import io.trino.operator.window.LagFunction;
import io.trino.operator.window.LastValueFunction;
import io.trino.operator.window.LeadFunction;
import io.trino.operator.window.NthValueFunction;
import io.trino.operator.window.RankFunction;
import io.trino.operator.window.ReflectionWindowFunctionSupplier;
import io.trino.operator.window.RegularPartitionerSupplier;
import io.trino.operator.window.RowNumberFunction;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.operator.PositionSearcher.findEndPosition;
import static io.trino.operator.WindowFunctionDefinition.window;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestWindowOperator
{
    private static final TypeOperators TYPE_OPERATORS_CACHE = new TypeOperators();
    private static final FrameInfo UNBOUNDED_FRAME = new FrameInfo(RANGE, UNBOUNDED_PRECEDING, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    public static final List<WindowFunctionDefinition> ROW_NUMBER = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier(0, RowNumberFunction.class), BIGINT, UNBOUNDED_FRAME, false, ImmutableList.of()));

    public static final List<WindowFunctionDefinition> RANK = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier(0, RankFunction.class), BIGINT, UNBOUNDED_FRAME, false, ImmutableList.of()));

    private static final List<WindowFunctionDefinition> FIRST_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier(1, FirstValueFunction.class), VARCHAR, UNBOUNDED_FRAME, false, ImmutableList.of(), 1));

    private static final List<WindowFunctionDefinition> LAST_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier(1, LastValueFunction.class), VARCHAR, UNBOUNDED_FRAME, false, ImmutableList.of(), 1));

    private static final List<WindowFunctionDefinition> NTH_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier(2, NthValueFunction.class), VARCHAR, UNBOUNDED_FRAME, false, ImmutableList.of(), 1, 3));

    private static final List<WindowFunctionDefinition> LAG = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier(3, LagFunction.class), VARCHAR, UNBOUNDED_FRAME, false, ImmutableList.of(), 1, 3, 4));

    private static final List<WindowFunctionDefinition> LEAD = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier(3, LeadFunction.class), VARCHAR, UNBOUNDED_FRAME, false, ImmutableList.of(), 1, 3, 4));

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testMultipleOutputPages()
    {
        testMultipleOutputPages(false, false, 0);
        testMultipleOutputPages(true, false, 8);
        testMultipleOutputPages(true, true, 8);
        testMultipleOutputPages(true, false, 0);
        testMultipleOutputPages(true, true, 0);
    }

    private void testMultipleOutputPages(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        DummySpillerFactory spillerFactory = new DummySpillerFactory();

        // make operator produce multiple pages during finish phase
        int numberOfRows = 80_000;
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .addSequencePage(numberOfRows, 0, 0)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.DESC_NULLS_FIRST}),
                spillerFactory,
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row((double) numberOfRows - i - 1, (long) numberOfRows - i - 1, (long) i + 1);
        }
        MaterializedResult expected = expectedBuilder.build();

        List<Page> pages = toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
        assertThat(pages).as("Expected more than one output page").hasSizeGreaterThan(1);

        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertThat(actual.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());

        assertThat(spillEnabled == (spillerFactory.getSpillsCount() > 0))
                .describedAs(format("Spill state mismatch. Expected spill: %s, spill count: %s", spillEnabled, spillerFactory.getSpillsCount()))
                .isTrue();
    }

    @Test
    public void testRowNumber()
    {
        testRowNumber(false, false, 0);
        testRowNumber(true, false, 8);
        testRowNumber(true, true, 8);
        testRowNumber(true, false, 0);
        testRowNumber(true, true, 0);
    }

    private void testRowNumber(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(2L, 0.3)
                .row(4L, 0.2)
                .row(6L, 0.1)
                .pageBreak()
                .row(-1L, -0.1)
                .row(5L, 0.4)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                new DummySpillerFactory(),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(-0.1, -1L, 1L)
                .row(0.3, 2L, 2L)
                .row(0.2, 4L, 3L)
                .row(0.4, 5L, 4L)
                .row(0.1, 6L, 5L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testRowNumberPartition()
    {
        testRowNumberPartition(false, false, 0);
        testRowNumberPartition(true, false, 8);
        testRowNumberPartition(true, true, 8);
        testRowNumberPartition(true, false, 0);
        testRowNumberPartition(true, true, 0);
    }

    private void testRowNumberPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT, DOUBLE, BOOLEAN)
                .row("b", -1L, -0.1, true)
                .row("a", 2L, 0.3, false)
                .row("a", 4L, 0.2, true)
                .pageBreak()
                .row("b", 5L, 0.4, false)
                .row("a", 6L, 0.1, true)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, BIGINT, DOUBLE, BOOLEAN),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                new DummySpillerFactory(),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, DOUBLE, BOOLEAN, BIGINT)
                .row("a", 2L, 0.3, false, 1L)
                .row("a", 4L, 0.2, true, 2L)
                .row("a", 6L, 0.1, true, 3L)
                .row("b", -1L, -0.1, true, 1L)
                .row("b", 5L, 0.4, false, 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testRowNumberArbitrary()
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1L)
                .row(3L)
                .row(5L)
                .row(7L)
                .pageBreak()
                .row(2L)
                .row(4L)
                .row(6L)
                .row(8L)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT),
                Ints.asList(0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(),
                ImmutableList.copyOf(new SortOrder[] {}),
                new DummySpillerFactory(),
                false);

        DriverContext driverContext = createDriverContext();
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(3L, 2L)
                .row(5L, 3L)
                .row(7L, 4L)
                .row(2L, 5L)
                .row(4L, 6L)
                .row(6L, 7L)
                .row(8L, 8L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testRowNumberArbitraryWithSpill()
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1L)
                .row(3L)
                .row(5L)
                .row(7L)
                .pageBreak()
                .row(2L)
                .row(4L)
                .row(6L)
                .row(8L)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT),
                Ints.asList(0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(),
                ImmutableList.copyOf(new SortOrder[] {}),
                new DummySpillerFactory(),
                true);

        DriverContext driverContext = createDriverContext();
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(3L, 3L)
                .row(4L, 4L)
                .row(5L, 5L)
                .row(6L, 6L)
                .row(7L, 7L)
                .row(8L, 8L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testDistinctPartitionAndPeers()
    {
        testDistinctPartitionAndPeers(false, false, 0);
        testDistinctPartitionAndPeers(true, false, 8);
        testDistinctPartitionAndPeers(true, true, 8);
        testDistinctPartitionAndPeers(true, false, 0);
        testDistinctPartitionAndPeers(true, true, 0);
    }

    public void testDistinctPartitionAndPeers(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(DOUBLE, DOUBLE)
                .row(1.0, 1.0)
                .row(1.0, 0.0)
                .row(1.0, Double.NaN)
                .row(1.0, null)
                .row(2.0, 2.0)
                .row(2.0, Double.NaN)
                .row(Double.NaN, Double.NaN)
                .row(Double.NaN, Double.NaN)
                .row(null, null)
                .row(null, 1.0)
                .row(null, null)
                .pageBreak()
                .row(1.0, Double.NaN)
                .row(1.0, null)
                .row(2.0, 2.0)
                .row(2.0, null)
                .row(Double.NaN, 3.0)
                .row(Double.NaN, null)
                .row(null, 2.0)
                .row(null, null)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(DOUBLE, DOUBLE),
                Ints.asList(0, 1),
                RANK,
                Ints.asList(0),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                new DummySpillerFactory(),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE, DOUBLE, BIGINT)
                .row(1.0, 0.0, 1L)
                .row(1.0, 1.0, 2L)
                .row(1.0, Double.NaN, 3L)
                .row(1.0, Double.NaN, 3L)
                .row(1.0, null, 5L)
                .row(1.0, null, 5L)
                .row(2.0, 2.0, 1L)
                .row(2.0, 2.0, 1L)
                .row(2.0, Double.NaN, 3L)
                .row(2.0, null, 4L)
                .row(Double.NaN, 3.0, 1L)
                .row(Double.NaN, Double.NaN, 2L)
                .row(Double.NaN, Double.NaN, 2L)
                .row(Double.NaN, null, 4L)
                .row(null, 1.0, 1L)
                .row(null, 2.0, 2L)
                .row(null, null, 3L)
                .row(null, null, 3L)
                .row(null, null, 3L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testMemoryLimit()
    {
        assertThatThrownBy(() -> {
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

            WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                    ImmutableList.of(BIGINT, DOUBLE),
                    Ints.asList(1),
                    ROW_NUMBER,
                    Ints.asList(),
                    Ints.asList(0),
                    ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                    new DummySpillerFactory(),
                    false);

            toPages(operatorFactory, driverContext, input);
        })
                .isInstanceOf(ExceededMemoryLimitException.class)
                .hasMessageMatching("Query exceeded per-node memory limit of 10B.*");
    }

    @Test
    public void testFirstValuePartition()
    {
        testFirstValuePartition(false, false, 0);
        testFirstValuePartition(true, false, 8);
        testFirstValuePartition(true, true, 8);
        testFirstValuePartition(true, false, 0);
        testFirstValuePartition(true, true, 0);
    }

    private void testFirstValuePartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, true, "")
                .row("a", "A2", 1L, false, "")
                .row("a", "B1", 2L, true, "")
                .pageBreak()
                .row("b", "C1", 2L, false, "")
                .row("a", "C2", 3L, true, "")
                .row("c", "A3", 1L, true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                FIRST_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                new DummySpillerFactory(),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "A2")
                .row("a", "B1", 2L, true, "A2")
                .row("a", "C2", 3L, true, "A2")
                .row("b", "A1", 1L, true, "A1")
                .row("b", "C1", 2L, false, "A1")
                .row("c", "A3", 1L, true, "A3")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testClose()
            throws Exception
    {
        RowPagesBuilder pageBuilder = rowPagesBuilder(VARCHAR, BIGINT);
        for (int i = 0; i < 500_000; ++i) {
            pageBuilder.row("a", 0L);
        }
        for (int i = 0; i < 500_000; ++i) {
            pageBuilder.row("b", 0L);
        }
        List<Page> input = pageBuilder.build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, BIGINT),
                Ints.asList(0, 1),
                ROW_NUMBER,
                Ints.asList(0),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                new DummySpillerFactory(),
                false);

        DriverContext driverContext = createDriverContext(1000);
        Operator operator = operatorFactory.createOperator(driverContext);
        operatorFactory.noMoreOperators();
        assertThat(operator.isFinished()).isFalse();
        assertThat(operator.needsInput()).isTrue();
        operator.addInput(input.get(0));
        operator.finish();
        operator.getOutput();

        // this should not fail
        operator.close();
    }

    @Test
    public void testLastValuePartition()
    {
        testLastValuePartition(false, false, 0);
        testLastValuePartition(true, false, 8);
        testLastValuePartition(true, true, 8);
        testLastValuePartition(true, false, 0);
        testLastValuePartition(true, true, 0);
    }

    private void testLastValuePartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, true, "")
                .row("a", "A2", 1L, false, "")
                .row("a", "B1", 2L, true, "")
                .pageBreak()
                .row("b", "C1", 2L, false, "")
                .row("a", "C2", 3L, true, "")
                .row("c", "A3", 1L, true, "")
                .build();

        DriverContext driverContext = createDriverContext(memoryLimit);
        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                LAST_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                new DummySpillerFactory(),
                spillEnabled);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "C2")
                .row("a", "B1", 2L, true, "C2")
                .row("a", "C2", 3L, true, "C2")
                .row("b", "A1", 1L, true, "C1")
                .row("b", "C1", 2L, false, "C1")
                .row("c", "A3", 1L, true, "A3")
                .build();
        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testNthValuePartition()
    {
        testNthValuePartition(false, false, 0);
        testNthValuePartition(true, false, 8);
        testNthValuePartition(true, true, 8);
        testNthValuePartition(true, false, 0);
        testNthValuePartition(true, true, 0);
    }

    private void testNthValuePartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BIGINT, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, 2L, true, "")
                .row("a", "A2", 1L, 3L, false, "")
                .row("a", "B1", 2L, 2L, true, "")
                .pageBreak()
                .row("b", "C1", 2L, 3L, false, "")
                .row("a", "C2", 3L, 1L, true, "")
                .row("c", "A3", 1L, null, true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 4),
                NTH_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                new DummySpillerFactory(),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "C2")
                .row("a", "B1", 2L, true, "B1")
                .row("a", "C2", 3L, true, "A2")
                .row("b", "A1", 1L, true, "C1")
                .row("b", "C1", 2L, false, null)
                .row("c", "A3", 1L, true, null)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testLagPartition()
    {
        testLagPartition(false, false, 0);
        testLagPartition(true, false, 8);
        testLagPartition(true, true, 8);
        testLagPartition(true, false, 0);
        testLagPartition(true, true, 0);
    }

    private void testLagPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, 1L, "D", true, "")
                .row("a", "A2", 1L, 2L, "D", false, "")
                .row("a", "B1", 2L, 2L, "D", true, "")
                .pageBreak()
                .row("b", "C1", 2L, 1L, "D", false, "")
                .row("a", "C2", 3L, 2L, "D", true, "")
                .row("c", "A3", 1L, 1L, "D", true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 5),
                LAG,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                new DummySpillerFactory(),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "D")
                .row("a", "B1", 2L, true, "D")
                .row("a", "C2", 3L, true, "A2")
                .row("b", "A1", 1L, true, "D")
                .row("b", "C1", 2L, false, "A1")
                .row("c", "A3", 1L, true, "D")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testLeadPartition()
    {
        testLeadPartition(false, false, 0);
        testLeadPartition(true, false, 8);
        testLeadPartition(true, true, 8);
        testLeadPartition(true, false, 0);
        testLeadPartition(true, true, 0);
    }

    private void testLeadPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, 1L, "D", true, "")
                .row("a", "A2", 1L, 2L, "D", false, "")
                .row("a", "B1", 2L, 2L, "D", true, "")
                .pageBreak()
                .row("b", "C1", 2L, 1L, "D", false, "")
                .row("a", "C2", 3L, 2L, "D", true, "")
                .row("c", "A3", 1L, 1L, "D", true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 5),
                LEAD,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                new DummySpillerFactory(),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "C2")
                .row("a", "B1", 2L, true, "D")
                .row("a", "C2", 3L, true, "D")
                .row("b", "A1", 1L, true, "C1")
                .row("b", "C1", 2L, false, "D")
                .row("c", "A3", 1L, true, "D")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testPartiallyPreGroupedPartitionWithEmptyInput()
    {
        testPartiallyPreGroupedPartitionWithEmptyInput(false, false, 0);
        testPartiallyPreGroupedPartitionWithEmptyInput(true, false, 8);
        testPartiallyPreGroupedPartitionWithEmptyInput(true, true, 8);
        testPartiallyPreGroupedPartitionWithEmptyInput(true, false, 0);
        testPartiallyPreGroupedPartitionWithEmptyInput(true, true, 0);
    }

    private void testPartiallyPreGroupedPartitionWithEmptyInput(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .pageBreak()
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0, 1),
                Ints.asList(1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                0,
                new DummySpillerFactory(),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testPartiallyPreGroupedPartition()
    {
        testPartiallyPreGroupedPartition(false, false, 0);
        testPartiallyPreGroupedPartition(true, false, 8);
        testPartiallyPreGroupedPartition(true, true, 8);
        testPartiallyPreGroupedPartition(true, false, 0);
        testPartiallyPreGroupedPartition(true, true, 0);
    }

    private void testPartiallyPreGroupedPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .row(2L, "a", 101L, "B")
                .pageBreak()
                .row(3L, "b", 102L, "E")
                .row(1L, "b", 103L, "D")
                .pageBreak()
                .row(3L, "b", 104L, "C")
                .row(1L, "c", 105L, "F")
                .pageBreak()
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0, 1),
                Ints.asList(1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                0,
                new DummySpillerFactory(),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 101L, "B", 1L)
                .row(3L, "b", 104L, "C", 1L)
                .row(3L, "b", 102L, "E", 2L)
                .row(1L, "b", 103L, "D", 1L)
                .row(1L, "c", 105L, "F", 1L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testFullyPreGroupedPartition()
    {
        testFullyPreGroupedPartition(false, false, 0);
        testFullyPreGroupedPartition(true, false, 8);
        testFullyPreGroupedPartition(true, true, 8);
        testFullyPreGroupedPartition(true, false, 0);
        testFullyPreGroupedPartition(true, true, 0);
    }

    private void testFullyPreGroupedPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .pageBreak()
                .row(2L, "a", 101L, "B")
                .pageBreak()
                .row(2L, "b", 102L, "D")
                .row(2L, "b", 103L, "C")
                .row(1L, "b", 104L, "E")
                .pageBreak()
                .row(1L, "b", 105L, "F")
                .row(3L, "c", 106L, "G")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(1, 0),
                Ints.asList(0, 1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                0,
                new DummySpillerFactory(),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 101L, "B", 1L)
                .row(2L, "b", 103L, "C", 1L)
                .row(2L, "b", 102L, "D", 2L)
                .row(1L, "b", 104L, "E", 1L)
                .row(1L, "b", 105L, "F", 2L)
                .row(3L, "c", 106L, "G", 1L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testFullyPreGroupedAndPartiallySortedPartition()
    {
        testFullyPreGroupedAndPartiallySortedPartition(false, false, 0);
        testFullyPreGroupedAndPartiallySortedPartition(true, false, 8);
        testFullyPreGroupedAndPartiallySortedPartition(true, true, 8);
        testFullyPreGroupedAndPartiallySortedPartition(true, false, 0);
        testFullyPreGroupedAndPartiallySortedPartition(true, true, 0);
    }

    private void testFullyPreGroupedAndPartiallySortedPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .pageBreak()
                .row(2L, "a", 100L, "A")
                .pageBreak()
                .row(2L, "b", 102L, "A")
                .row(2L, "b", 101L, "A")
                .row(2L, "b", 100L, "B")
                .row(1L, "b", 101L, "A")
                .pageBreak()
                .row(1L, "b", 100L, "A")
                .row(3L, "c", 100L, "A")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(1, 0),
                Ints.asList(0, 1),
                Ints.asList(3, 2),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST, SortOrder.ASC_NULLS_LAST),
                1,
                new DummySpillerFactory(),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 100L, "A", 1L)
                .row(2L, "b", 101L, "A", 1L)
                .row(2L, "b", 102L, "A", 2L)
                .row(2L, "b", 100L, "B", 3L)
                .row(1L, "b", 100L, "A", 1L)
                .row(1L, "b", 101L, "A", 2L)
                .row(3L, "c", 100L, "A", 1L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testFullyPreGroupedAndFullySortedPartition()
    {
        testFullyPreGroupedAndFullySortedPartition(false, false, 0);
        testFullyPreGroupedAndFullySortedPartition(true, false, 8);
        testFullyPreGroupedAndFullySortedPartition(true, true, 8);
        testFullyPreGroupedAndFullySortedPartition(true, false, 0);
        testFullyPreGroupedAndFullySortedPartition(true, true, 0);
    }

    private void testFullyPreGroupedAndFullySortedPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .pageBreak()
                .row(2L, "a", 101L, "A")
                .pageBreak()
                .row(2L, "b", 102L, "A")
                .row(2L, "b", 103L, "A")
                .row(2L, "b", 104L, "B")
                .row(1L, "b", 105L, "A")
                .pageBreak()
                .row(1L, "b", 106L, "A")
                .row(3L, "c", 107L, "A")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(1, 0),
                Ints.asList(0, 1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                1,
                new DummySpillerFactory(),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 101L, "A", 1L)
                .row(2L, "b", 102L, "A", 1L)
                .row(2L, "b", 103L, "A", 2L)
                .row(2L, "b", 104L, "B", 3L)
                .row(1L, "b", 105L, "A", 1L)
                .row(1L, "b", 106L, "A", 2L)
                .row(3L, "c", 107L, "A", 1L)
                .build();

        // Since fully grouped and sorted already, should respect original input order
        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testFindEndPosition()
    {
        assertFindEndPosition("0", 1);
        assertFindEndPosition("11", 2);
        assertFindEndPosition("1111111111", 10);

        assertFindEndPosition("01", 1);
        assertFindEndPosition("011", 1);
        assertFindEndPosition("0111", 1);
        assertFindEndPosition("0111111111", 1);

        assertFindEndPosition("012", 1);
        assertFindEndPosition("01234", 1);
        assertFindEndPosition("0123456789", 1);

        assertFindEndPosition("001", 2);
        assertFindEndPosition("0001", 3);
        assertFindEndPosition("0000000001", 9);

        assertFindEndPosition("000111", 3);
        assertFindEndPosition("0001111", 3);
        assertFindEndPosition("0000111", 4);
        assertFindEndPosition("000000000000001111111111", 14);
    }

    private static void assertFindEndPosition(String values, int expected)
    {
        char[] array = values.toCharArray();
        assertThat(findEndPosition(0, array.length, (first, second) -> array[first] == array[second])).isEqualTo(expected);
    }

    private WindowOperatorFactory createFactoryUnbounded(
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> functions,
            List<Integer> partitionChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            SpillerFactory spillerFactory,
            boolean spillEnabled)
    {
        return createFactoryUnbounded(
                sourceTypes,
                outputChannels,
                functions,
                partitionChannels,
                ImmutableList.of(),
                sortChannels,
                sortOrder,
                0,
                spillerFactory,
                spillEnabled);
    }

    private WindowOperatorFactory createFactoryUnbounded(
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> functions,
            List<Integer> partitionChannels,
            List<Integer> preGroupedChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            int preSortedChannelPrefix,
            DummySpillerFactory spillerFactory,
            boolean spillEnabled)
    {
        return new WindowOperatorFactory(
                0,
                new PlanNodeId("test"),
                sourceTypes,
                outputChannels,
                functions,
                partitionChannels,
                preGroupedChannels,
                sortChannels,
                sortOrder,
                preSortedChannelPrefix,
                10,
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                spillerFactory,
                new OrderingCompiler(TYPE_OPERATORS_CACHE),
                ImmutableList.of(),
                new RegularPartitionerSupplier());
    }

    public static WindowOperatorFactory createFactoryUnbounded(
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> functions,
            List<Integer> partitionChannels,
            List<Integer> preGroupedChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            int preSortedChannelPrefix,
            SpillerFactory spillerFactory,
            boolean spillEnabled)
    {
        return new WindowOperatorFactory(
                0,
                new PlanNodeId("test"),
                sourceTypes,
                outputChannels,
                functions,
                partitionChannels,
                preGroupedChannels,
                sortChannels,
                sortOrder,
                preSortedChannelPrefix,
                10,
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                spillerFactory,
                new OrderingCompiler(TYPE_OPERATORS_CACHE),
                ImmutableList.of(),
                new RegularPartitionerSupplier());
    }

    private DriverContext createDriverContext()
    {
        return createDriverContext(Long.MAX_VALUE);
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
