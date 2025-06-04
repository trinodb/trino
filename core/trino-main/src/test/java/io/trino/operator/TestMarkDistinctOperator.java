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
import io.trino.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
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

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.GroupByHashYieldAssertion.createPagesWithDistinctHashKeys;
import static io.trino.operator.GroupByHashYieldAssertion.finishOperatorWithYieldingGroupByHash;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
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
public class TestMarkDistinctOperator
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    private final TypeOperators typeOperators = new TypeOperators();
    private final FlatHashStrategyCompiler hashStrategyCompiler = new FlatHashStrategyCompiler(typeOperators);

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testMarkDistinct()
    {
        testMarkDistinct(true, newDriverContext());
        testMarkDistinct(false, newDriverContext());
    }

    private void testMarkDistinct(boolean hashEnabled, DriverContext driverContext)
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .build();

        OperatorFactory operatorFactory = new MarkDistinctOperatorFactory(
                0,
                new PlanNodeId("test"),
                rowPagesBuilder.getTypes(),
                ImmutableList.of(0),
                rowPagesBuilder.getHashChannel(),
                hashStrategyCompiler);

        MaterializedResult.Builder expected = resultBuilder(driverContext.getSession(), BIGINT, BOOLEAN);
        for (long i = 0; i < 100; i++) {
            expected.row(i, true);
            expected.row(i, false);
        }

        OperatorAssertion.assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected.build(), hashEnabled, Optional.of(1));
    }

    @Test
    public void testRleDistinctMask()
    {
        testRleDistinctMask(true, newDriverContext());
        testRleDistinctMask(false, newDriverContext());
    }

    private void testRleDistinctMask(boolean hashEnabled, DriverContext driverContext)
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT);
        List<Page> inputs = rowPagesBuilder
                .addSequencePage(100, 0)
                .addSequencePage(100, 50)
                .addSequencePage(1, 200)
                .addSequencePage(1, 100)
                .build();
        Page firstInput = inputs.get(0);
        Page secondInput = inputs.get(1);
        Page singleDistinctPage = inputs.get(2);
        Page singleNotDistinctPage = inputs.get(3);
        OperatorFactory operatorFactory = new MarkDistinctOperatorFactory(
                0,
                new PlanNodeId("test"),
                rowPagesBuilder.getTypes(),
                ImmutableList.of(0),
                rowPagesBuilder.getHashChannel(),
                hashStrategyCompiler);

        int maskChannel = firstInput.getChannelCount(); // mask channel is appended to the input
        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            operator.addInput(firstInput);
            Block allDistinctOutput = operator.getOutput().getBlock(maskChannel);
            operator.addInput(firstInput);
            Block noDistinctOutput = operator.getOutput().getBlock(maskChannel);
            // all distinct and no distinct conditions produce RLE blocks
            assertThat(allDistinctOutput).isInstanceOf(RunLengthEncodedBlock.class);
            assertThat(BOOLEAN.getBoolean(allDistinctOutput, 0)).isTrue();
            assertThat(noDistinctOutput).isInstanceOf(RunLengthEncodedBlock.class);
            assertThat(BOOLEAN.getBoolean(noDistinctOutput, 0)).isFalse();

            operator.addInput(secondInput);
            Block halfDistinctOutput = operator.getOutput().getBlock(maskChannel);
            // [0,50) is not distinct
            for (int position = 0; position < 50; position++) {
                assertThat(BOOLEAN.getBoolean(halfDistinctOutput, position)).isFalse();
            }
            for (int position = 50; position < 100; position++) {
                assertThat(BOOLEAN.getBoolean(halfDistinctOutput, position)).isTrue();
            }

            operator.addInput(singleDistinctPage);
            Block singleDistinctBlock = operator.getOutput().getBlock(maskChannel);
            assertThat(singleDistinctBlock instanceof RunLengthEncodedBlock)
                    .describedAs("single position inputs should not be RLE")
                    .isFalse();
            assertThat(BOOLEAN.getBoolean(singleDistinctBlock, 0)).isTrue();

            operator.addInput(singleNotDistinctPage);
            Block singleNotDistinctBlock = operator.getOutput().getBlock(maskChannel);
            assertThat(singleNotDistinctBlock instanceof RunLengthEncodedBlock)
                    .describedAs("single position inputs should not be RLE")
                    .isFalse();
            assertThat(BOOLEAN.getBoolean(singleNotDistinctBlock, 0)).isFalse();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testMemoryReservationYield()
    {
        testMemoryReservationYield(BIGINT);
        testMemoryReservationYield(VARCHAR);
    }

    private void testMemoryReservationYield(Type type)
    {
        List<Page> input = createPagesWithDistinctHashKeys(type, 6_000, 600);

        OperatorFactory operatorFactory = new MarkDistinctOperatorFactory(0, new PlanNodeId("test"), ImmutableList.of(type), ImmutableList.of(0), Optional.of(1), hashStrategyCompiler);

        // get result with yield; pick a relatively small buffer for partitionRowCount's memory usage
        GroupByHashYieldAssertion.GroupByHashYieldResult result = finishOperatorWithYieldingGroupByHash(input, type, operatorFactory, operator -> ((MarkDistinctOperator) operator).getCapacity(), 450_000);
        assertThat(result.getYieldCount()).isGreaterThanOrEqualTo(5);
        assertThat(result.getMaxReservedBytes()).isGreaterThanOrEqualTo(20L << 20);

        int count = 0;
        for (Page page : result.getOutput()) {
            assertThat(page.getChannelCount()).isEqualTo(3);
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertThat(BOOLEAN.getBoolean(page.getBlock(2), i)).isTrue();
                count++;
            }
        }
        assertThat(count).isEqualTo(6_000 * 600);
    }

    private DriverContext newDriverContext()
    {
        return createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
