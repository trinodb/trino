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
import io.trino.block.BlockAssertions;
import io.trino.metadata.Metadata;
import io.trino.operator.AggregationOperator.AggregationOperatorFactory;
import io.trino.operator.aggregation.AccumulatorFactory;
import io.trino.operator.aggregation.InternalAggregationFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.tree.QualifiedName;
import io.trino.testing.MaterializedResult;
import io.trino.type.BlockTypeOperators;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.Collections.emptyIterator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestAggregationOperator
{
    private static final Metadata metadata = createTestMetadataManager();

    private static final InternalAggregationFunction LONG_AVERAGE = metadata.getAggregateFunctionImplementation(
            metadata.resolveFunction(QualifiedName.of("avg"), fromTypes(BIGINT)));
    private static final InternalAggregationFunction DOUBLE_SUM = metadata.getAggregateFunctionImplementation(
            metadata.resolveFunction(QualifiedName.of("sum"), fromTypes(DOUBLE)));
    private static final InternalAggregationFunction LONG_SUM = metadata.getAggregateFunctionImplementation(
            metadata.resolveFunction(QualifiedName.of("sum"), fromTypes(BIGINT)));
    private static final InternalAggregationFunction REAL_SUM = metadata.getAggregateFunctionImplementation(
            metadata.resolveFunction(QualifiedName.of("sum"), fromTypes(REAL)));
    private static final InternalAggregationFunction COUNT = metadata.getAggregateFunctionImplementation(
            metadata.resolveFunction(QualifiedName.of("count"), ImmutableList.of()));

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testMaskWithDirtyNulls()
    {
        List<Page> input = ImmutableList.of(new Page(
                4,
                BlockAssertions.createLongsBlock(1, 2, 3, 4),
                new ByteArrayBlock(
                        4,
                        Optional.of(new boolean[] {true, true, false, false}),
                        new byte[] {0, 27 /* dirty null */, 0, 75 /* non-zero value is true */})));

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                Step.SINGLE,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.of(1))),
                false);

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(1L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testDistinctMaskWithNulls()
    {
        TypeOperators typeOperators = new TypeOperators();

        AccumulatorFactory distinctFactory = COUNT.bind(
                ImmutableList.of(0),
                Optional.of(1),
                ImmutableList.of(BIGINT, BOOLEAN),
                ImmutableList.of(),
                ImmutableList.of(),
                null,
                true, // distinct
                new JoinCompiler(typeOperators),
                new BlockTypeOperators(typeOperators),
                ImmutableList.of(),
                TEST_SESSION);

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                Step.SINGLE,
                ImmutableList.of(distinctFactory),
                false);

        ByteArrayBlock trueMaskAllNull = new ByteArrayBlock(
                4,
                Optional.of(new boolean[] {true, true, true, true}), /* all positions are null */
                new byte[] {1, 1, 1, 1}); /* non-zero value is true, all masks are true */

        Block trueNullRleMask = new RunLengthEncodedBlock(trueMaskAllNull.getSingleValueBlock(0), 4);

        List<Page> nullTrueMaskInput = ImmutableList.of(
                new Page(4, BlockAssertions.createLongsBlock(1, 2, 3, 4), trueMaskAllNull),
                new Page(4, BlockAssertions.createLongsBlock(5, 6, 7, 8), trueNullRleMask));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(0L) // all rows should be filtered by nulls
                .build();

        assertOperatorEquals(operatorFactory, driverContext, nullTrueMaskInput, expected);
    }

    @Test
    public void testAggregation()
    {
        InternalAggregationFunction countVarcharColumn = metadata.getAggregateFunctionImplementation(
                metadata.resolveFunction(QualifiedName.of("count"), fromTypes(VARCHAR)));
        InternalAggregationFunction maxVarcharColumn = metadata.getAggregateFunctionImplementation(
                metadata.resolveFunction(QualifiedName.of("max"), fromTypes(VARCHAR)));
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT, VARCHAR, BIGINT, REAL, DOUBLE, VARCHAR)
                .addSequencePage(100, 0, 0, 300, 500, 400, 500, 500)
                .build();

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                Step.SINGLE,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty()),
                        LONG_SUM.bind(ImmutableList.of(1), Optional.empty()),
                        LONG_AVERAGE.bind(ImmutableList.of(1), Optional.empty()),
                        maxVarcharColumn.bind(ImmutableList.of(2), Optional.empty()),
                        countVarcharColumn.bind(ImmutableList.of(0), Optional.empty()),
                        LONG_SUM.bind(ImmutableList.of(3), Optional.empty()),
                        REAL_SUM.bind(ImmutableList.of(4), Optional.empty()),
                        DOUBLE_SUM.bind(ImmutableList.of(5), Optional.empty()),
                        maxVarcharColumn.bind(ImmutableList.of(6), Optional.empty())),
                false);

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT, REAL, DOUBLE, VARCHAR)
                .row(100L, 4950L, 49.5, "399", 100L, 54950L, 44950.0f, 54950.0, "599")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
        assertEquals(driverContext.getSystemMemoryUsage(), 0);
        assertEquals(driverContext.getMemoryUsage(), 0);
    }

    @Test
    public void testMemoryTracking()
            throws Exception
    {
        testMemoryTracking(false);
        testMemoryTracking(true);
    }

    private void testMemoryTracking(boolean useSystemMemory)
            throws Exception
    {
        Page input = getOnlyElement(rowPagesBuilder(BIGINT).addSequencePage(100, 0).build());

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                Step.SINGLE,
                ImmutableList.of(LONG_SUM.bind(ImmutableList.of(0), Optional.empty())),
                useSystemMemory);

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            assertTrue(operator.needsInput());
            operator.addInput(input);

            if (useSystemMemory) {
                assertThat(driverContext.getSystemMemoryUsage()).isGreaterThan(0);
                assertEquals(driverContext.getMemoryUsage(), 0);
            }
            else {
                assertEquals(driverContext.getSystemMemoryUsage(), 0);
                assertThat(driverContext.getMemoryUsage()).isGreaterThan(0);
            }

            toPages(operator, emptyIterator());
        }

        assertEquals(driverContext.getSystemMemoryUsage(), 0);
        assertEquals(driverContext.getMemoryUsage(), 0);
    }
}
