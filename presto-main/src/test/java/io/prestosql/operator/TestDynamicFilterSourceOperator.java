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
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.DynamicFilterId;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.SequencePageBuilder.createSequencePage;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxPerDriverRowCount;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxPerDriverSize;
import static io.prestosql.block.BlockAssertions.createBooleansBlock;
import static io.prestosql.block.BlockAssertions.createDoublesBlock;
import static io.prestosql.block.BlockAssertions.createLongRepeatBlock;
import static io.prestosql.block.BlockAssertions.createLongsBlock;
import static io.prestosql.block.BlockAssertions.createStringsBlock;
import static io.prestosql.operator.OperatorAssertion.toMaterializedResult;
import static io.prestosql.operator.OperatorAssertion.toPages;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.lang.Float.floatToRawIntBits;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;

@Test(singleThreaded = true)
public class TestDynamicFilterSourceOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private PipelineContext pipelineContext;

    private ImmutableList.Builder<TupleDomain<DynamicFilterId>> partitions;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        pipelineContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false);

        partitions = ImmutableList.builder();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    private void verifyPassthrough(Operator operator, List<Type> types, Page... pages)
    {
        List<Page> inputPages = ImmutableList.copyOf(pages);
        List<Page> outputPages = toPages(operator, inputPages.iterator());
        MaterializedResult actual = toMaterializedResult(pipelineContext.getSession(), types, outputPages);
        MaterializedResult expected = toMaterializedResult(pipelineContext.getSession(), types, inputPages);
        assertEquals(actual, expected);
    }

    private OperatorFactory createOperatorFactory(DynamicFilterSourceOperator.Channel... buildChannels)
    {
        return new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                0,
                new PlanNodeId("PLAN_NODE_ID"),
                this::consumePredicate,
                Arrays.stream(buildChannels).collect(toList()),
                getDynamicFilteringMaxPerDriverRowCount(TEST_SESSION),
                getDynamicFilteringMaxPerDriverSize(TEST_SESSION));
    }

    private void consumePredicate(TupleDomain<DynamicFilterId> partitionPredicate)
    {
        partitions.add(partitionPredicate);
    }

    private Operator createOperator(OperatorFactory operatorFactory)
    {
        return operatorFactory.createOperator(pipelineContext.addDriverContext());
    }

    private static DynamicFilterSourceOperator.Channel channel(int index, Type type)
    {
        return new DynamicFilterSourceOperator.Channel(new DynamicFilterId(Integer.toString(index)), type, index);
    }

    @Test
    public void testCollectMultipleOperators()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BIGINT));

        Operator op1 = createOperator(operatorFactory); // will finish before noMoreOperators()
        verifyPassthrough(op1,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2)),
                new Page(createLongsBlock(3, 5)));

        Operator op2 = createOperator(operatorFactory); // will finish after noMoreOperators()
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 5L))))));

        verifyPassthrough(op2,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(2, 3)),
                new Page(createLongsBlock(1, 4)));

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 5L)))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 4L))))));
    }

    @Test
    public void testCollectMultipleColumns()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BOOLEAN), channel(1, DOUBLE));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), Domain.multipleValues(BOOLEAN, ImmutableList.of(true, false)),
                        new DynamicFilterId("1"), Domain.multipleValues(DOUBLE, ImmutableList.of(1.5, 3.0, 4.5))))));
    }

    @Test
    public void testCollectOnlyFirstColumn()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BOOLEAN));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), Domain.multipleValues(BOOLEAN, ImmutableList.of(true, false))))));
    }

    @Test
    public void testCollectOnlyLastColumn()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(1, DOUBLE));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("1"), Domain.multipleValues(DOUBLE, ImmutableList.of(1.5, 3.0, 4.5))))));
    }

    @Test
    public void testCollectWithNulls()
    {
        Block blockWithNulls = INTEGER
                .createFixedSizeBlockBuilder(0)
                .writeInt(3)
                .appendNull()
                .writeInt(4)
                .build();

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, INTEGER));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(INTEGER),
                new Page(createLongsBlock(1, 2, 3)),
                new Page(blockWithNulls),
                new Page(createLongsBlock(4, 5)));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), Domain.create(ValueSet.of(INTEGER, 1L, 2L, 3L, 4L, 5L), false)))));
    }

    @Test
    public void testCollectWithDoubleNaN()
    {
        BlockBuilder input = DOUBLE.createBlockBuilder(null, 10);
        DOUBLE.writeDouble(input, 42.0);
        DOUBLE.writeDouble(input, Double.NaN);

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, DOUBLE));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(DOUBLE),
                new Page(input.build()));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), Domain.multipleValues(DOUBLE, ImmutableList.of(42.0))))));
    }

    @Test
    public void testCollectWithRealNaN()
    {
        BlockBuilder input = REAL.createBlockBuilder(null, 10);
        REAL.writeLong(input, floatToRawIntBits(42.0f));
        REAL.writeLong(input, floatToRawIntBits(Float.NaN));

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, REAL));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(REAL),
                new Page(input.build()));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), Domain.multipleValues(REAL, ImmutableList.of((long) floatToRawIntBits(42.0f)))))));
    }

    @Test
    public void testCollectNoFilters()
    {
        OperatorFactory operatorFactory = createOperatorFactory();
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2, 3)));
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectEmptyBuildSide()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BIGINT));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(BIGINT));
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.none()));
    }

    @Test
    public void testCollectTooMuchRows()
    {
        int maxRowCount = getDynamicFilteringMaxPerDriverRowCount(pipelineContext.getSession());
        Page largePage = createSequencePage(ImmutableList.of(BIGINT), maxRowCount + 1);

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BIGINT));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(BIGINT),
                largePage);
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectTooMuchBytesSingleColumn()
    {
        long maxByteSize = getDynamicFilteringMaxPerDriverSize(pipelineContext.getSession()).toBytes();
        Page largePage = new Page(createStringsBlock("A".repeat((int) maxByteSize + 1)));

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, VARCHAR));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(VARCHAR),
                largePage);
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectTooMuchBytesMultipleColumns()
    {
        long maxByteSize = getDynamicFilteringMaxPerDriverSize(pipelineContext.getSession()).toBytes();
        Page largePage = new Page(createStringsBlock("A".repeat((int) (maxByteSize / 2) + 1)),
                createStringsBlock("B".repeat((int) (maxByteSize / 2) + 1)));

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, VARCHAR),
                channel(1, VARCHAR));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(VARCHAR, VARCHAR),
                largePage);
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectDeduplication()
    {
        int maxRowCount = getDynamicFilteringMaxPerDriverRowCount(pipelineContext.getSession());
        Page largePage = new Page(createLongRepeatBlock(7, maxRowCount * 10)); // lots of zeros
        Page nullsPage = new Page(createLongsBlock(Arrays.asList(new Long[maxRowCount * 10]))); // lots of nulls

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BIGINT));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(BIGINT),
                largePage, nullsPage);
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), Domain.create(ValueSet.of(BIGINT, 7L), false)))));
    }
}
