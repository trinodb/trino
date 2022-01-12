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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.AggregationOperator.AggregationOperatorFactory;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.tree.QualifiedName;
import io.trino.testing.MaterializedResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
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
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    private static final TestingAggregationFunction LONG_AVERAGE = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("avg"), fromTypes(BIGINT));
    private static final TestingAggregationFunction DOUBLE_SUM = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("sum"), fromTypes(DOUBLE));
    private static final TestingAggregationFunction LONG_SUM = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("sum"), fromTypes(BIGINT));
    private static final TestingAggregationFunction REAL_SUM = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("sum"), fromTypes(REAL));
    private static final TestingAggregationFunction COUNT = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("count"), ImmutableList.of());

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
                createLongsBlock(1, 2, 3, 4),
                new ByteArrayBlock(
                        4,
                        Optional.of(new boolean[] {true, true, false, false}),
                        new byte[] {0, 27 /* dirty null */, 0, 75 /* non-zero value is true */})));

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.of(1))));

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
        AggregatorFactory distinctFactory = LONG_SUM.createDistinctAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.of(1));

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(distinctFactory));

        ByteArrayBlock trueMaskAllNull = new ByteArrayBlock(
                4,
                Optional.of(new boolean[] {true, true, true, true}), /* all positions are null */
                new byte[] {1, 1, 1, 1}); /* non-zero value is true, all masks are true */

        Block trueNullRleMask = new RunLengthEncodedBlock(trueMaskAllNull.getSingleValueBlock(0), 4);

        List<Page> nullTrueMaskInput = ImmutableList.of(
                new Page(4, createLongsBlock(1, 2, 3, 4), trueMaskAllNull),
                new Page(4, createLongsBlock(10, 11, 10, 11), createBooleansBlock(true, true, true, true)),
                new Page(4, createLongsBlock(5, 6, 7, 8), trueNullRleMask));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(21L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, nullTrueMaskInput, expected);
    }

    @Test
    public void testAggregation()
    {
        TestingAggregationFunction countVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("count"), fromTypes(VARCHAR));
        TestingAggregationFunction maxVarcharColumn = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("max"), fromTypes(VARCHAR));
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT, VARCHAR, BIGINT, REAL, DOUBLE, VARCHAR)
                .addSequencePage(100, 0, 0, 300, 500, 400, 500, 500)
                .build();

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(COUNT.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                        LONG_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(1), OptionalInt.empty()),
                        LONG_AVERAGE.createAggregatorFactory(SINGLE, ImmutableList.of(1), OptionalInt.empty()),
                        maxVarcharColumn.createAggregatorFactory(SINGLE, ImmutableList.of(2), OptionalInt.empty()),
                        countVarcharColumn.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty()),
                        LONG_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(3), OptionalInt.empty()),
                        REAL_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(4), OptionalInt.empty()),
                        DOUBLE_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(5), OptionalInt.empty()),
                        maxVarcharColumn.createAggregatorFactory(SINGLE, ImmutableList.of(6), OptionalInt.empty())));

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT, REAL, DOUBLE, VARCHAR)
                .row(100L, 4950L, 49.5, "399", 100L, 54950L, 44950.0f, 54950.0, "599")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
        assertEquals(driverContext.getMemoryUsage(), 0);
    }

    @Test
    public void testMemoryTracking()
            throws Exception
    {
        Page input = getOnlyElement(rowPagesBuilder(BIGINT).addSequencePage(100, 0).build());

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(LONG_SUM.createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())));

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            assertTrue(operator.needsInput());
            operator.addInput(input);

            assertThat(driverContext.getMemoryUsage()).isGreaterThan(0);

            toPages(operator, emptyIterator());
        }

        assertEquals(driverContext.getMemoryUsage(), 0);
    }
}
