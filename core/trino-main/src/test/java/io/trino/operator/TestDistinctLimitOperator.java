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
import io.trino.RowPagesBuilder;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.GroupByHashYieldAssertion.createPagesWithDistinctHashKeys;
import static io.trino.operator.GroupByHashYieldAssertion.finishOperatorWithYieldingGroupByHash;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDistinctLimitOperator
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    private final JoinCompiler joinCompiler = new JoinCompiler(new TypeOperators());

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testDistinctLimit()
    {
        testDistinctLimit(true);
        testDistinctLimit(false);
    }

    public void testDistinctLimit(boolean hashEnabled)
    {
        DriverContext driverContext = newDriverContext();
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(3, 1)
                .addSequencePage(5, 2)
                .build();

        OperatorFactory operatorFactory = new DistinctLimitOperator.DistinctLimitOperatorFactory(
                0,
                new PlanNodeId("test"),
                rowPagesBuilder.getTypes(),
                Ints.asList(0),
                5,
                rowPagesBuilder.getHashChannel(),
                joinCompiler);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(1L)
                .row(2L)
                .row(3L)
                .row(4L)
                .row(5L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, hashEnabled, ImmutableList.of(1));
    }

    @Test
    public void testDistinctLimitWithPageAlignment()
    {
        testDistinctLimitWithPageAlignment(true);
        testDistinctLimitWithPageAlignment(false);
    }

    public void testDistinctLimitWithPageAlignment(boolean hashEnabled)
    {
        DriverContext driverContext = newDriverContext();
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(3, 1)
                .addSequencePage(3, 2)
                .build();

        OperatorFactory operatorFactory = new DistinctLimitOperator.DistinctLimitOperatorFactory(
                0,
                new PlanNodeId("test"),
                rowPagesBuilder.getTypes(),
                Ints.asList(0),
                3,
                rowPagesBuilder.getHashChannel(),
                joinCompiler);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(1L)
                .row(2L)
                .row(3L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, hashEnabled, ImmutableList.of(1));
    }

    @Test
    public void testDistinctLimitValuesLessThanLimit()
    {
        testDistinctLimitValuesLessThanLimit(true);
        testDistinctLimitValuesLessThanLimit(false);
    }

    public void testDistinctLimitValuesLessThanLimit(boolean hashEnabled)
    {
        DriverContext driverContext = newDriverContext();

        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(3, 1)
                .addSequencePage(3, 2)
                .build();

        OperatorFactory operatorFactory = new DistinctLimitOperator.DistinctLimitOperatorFactory(
                0,
                new PlanNodeId("test"),
                rowPagesBuilder.getTypes(),
                Ints.asList(0),
                5,
                rowPagesBuilder.getHashChannel(),
                joinCompiler);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(1L)
                .row(2L)
                .row(3L)
                .row(4L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, hashEnabled, ImmutableList.of(1));
    }

    @Test
    public void testMemoryReservationYield()
    {
        testMemoryReservationYield(VARCHAR);
        testMemoryReservationYield(BIGINT);
    }

    public void testMemoryReservationYield(Type type)
    {
        List<Page> input = createPagesWithDistinctHashKeys(type, 6_000, 600);

        OperatorFactory operatorFactory = new DistinctLimitOperator.DistinctLimitOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(type, BIGINT),
                ImmutableList.of(0),
                Integer.MAX_VALUE,
                Optional.of(1),
                joinCompiler);

        GroupByHashYieldAssertion.GroupByHashYieldResult result = finishOperatorWithYieldingGroupByHash(input, type, operatorFactory, operator -> ((DistinctLimitOperator) operator).getCapacity(), 450_000);
        assertGreaterThanOrEqual(result.getYieldCount(), 5);
        assertGreaterThanOrEqual(result.getMaxReservedBytes(), 20L << 20);
        assertThat(result.getOutput().stream().mapToInt(Page::getPositionCount).sum()).isEqualTo(6_000 * 600);
    }

    private DriverContext newDriverContext()
    {
        return createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
