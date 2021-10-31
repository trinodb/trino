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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.type.BlockTypeOperators;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createColorRepeatBlock;
import static io.trino.block.BlockAssertions.createColorSequenceBlock;
import static io.trino.block.BlockAssertions.createDoubleRepeatBlock;
import static io.trino.block.BlockAssertions.createDoubleSequenceBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createLongRepeatBlock;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createSequenceBlockOfReal;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.block.BlockAssertions.createTypedLongsBlock;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.spi.predicate.Range.equal;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.type.ColorType.COLOR;
import static java.lang.Float.floatToRawIntBits;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@Test(singleThreaded = true)
public class TestDynamicFilterSourceOperator
{
    private BlockTypeOperators blockTypeOperators;
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private PipelineContext pipelineContext;

    private ImmutableList.Builder<TupleDomain<DynamicFilterId>> partitions;

    @BeforeMethod
    public void setUp()
    {
        blockTypeOperators = new BlockTypeOperators(new TypeOperators());
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
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
        verifyPassthrough(operator, types, Arrays.asList(pages));
    }

    private void verifyPassthrough(Operator operator, List<Type> types, List<Page> pages)
    {
        List<Page> inputPages = ImmutableList.copyOf(pages);
        List<Page> outputPages = toPages(operator, inputPages.iterator());
        MaterializedResult actual = toMaterializedResult(pipelineContext.getSession(), types, outputPages);
        MaterializedResult expected = toMaterializedResult(pipelineContext.getSession(), types, inputPages);
        assertEquals(actual, expected);
    }

    private OperatorFactory createOperatorFactory(DynamicFilterSourceOperator.Channel... buildChannels)
    {
        return createOperatorFactory(100, DataSize.of(10, KILOBYTE), 1_000_000, Arrays.asList(buildChannels));
    }

    private OperatorFactory createOperatorFactory(
            int maxFilterDistinctValues,
            DataSize maxFilterSize,
            int minMaxCollectionLimit,
            Iterable<DynamicFilterSourceOperator.Channel> buildChannels)
    {
        return new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                0,
                new PlanNodeId("PLAN_NODE_ID"),
                this::consumePredicate,
                ImmutableList.copyOf(buildChannels),
                maxFilterDistinctValues,
                maxFilterSize,
                minMaxCollectionLimit,
                blockTypeOperators);
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

    private void assertDynamicFilters(int maxFilterDistinctValues, List<Type> types, List<Page> pages, List<TupleDomain<DynamicFilterId>> expectedTupleDomains)
    {
        assertDynamicFilters(maxFilterDistinctValues, DataSize.of(10, KILOBYTE), 1_000_000, types, pages, expectedTupleDomains);
    }

    private void assertDynamicFilters(
            int maxFilterDistinctValues,
            DataSize maxFilterSize,
            int minMaxCollectionLimit,
            List<Type> types,
            List<Page> pages,
            List<TupleDomain<DynamicFilterId>> expectedTupleDomains)
    {
        List<DynamicFilterSourceOperator.Channel> buildChannels = IntStream.range(0, types.size())
                .mapToObj(i -> channel(i, types.get(i)))
                .collect(toImmutableList());
        OperatorFactory operatorFactory = createOperatorFactory(maxFilterDistinctValues, maxFilterSize, minMaxCollectionLimit, buildChannels);
        verifyPassthrough(createOperator(operatorFactory), types, pages);
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), expectedTupleDomains);
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
                        new DynamicFilterId("0"), Domain.create(
                                ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 3L, true), Range.equal(BIGINT, 5L)),
                                false)))));

        verifyPassthrough(op2,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(2, 3)),
                new Page(createLongsBlock(1, 4)));

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), Domain.create(
                                ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 3L, true), Range.equal(BIGINT, 5L)),
                                false))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), Domain.create(
                                ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 4L, true)),
                                false)))));
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
                        new DynamicFilterId("0"), Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 1L, true, 5L, true)), false)))));
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
    public void testCollectTooMuchRowsDouble()
    {
        int maxDistinctValues = 100;
        assertDynamicFilters(
                maxDistinctValues,
                ImmutableList.of(DOUBLE),
                ImmutableList.of(
                        new Page(createDoubleSequenceBlock(0, maxDistinctValues + 1)),
                        new Page(createDoubleRepeatBlock(Double.NaN, maxDistinctValues + 1))),
                ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectTooMuchRowsReal()
    {
        int maxDistinctValues = 100;
        assertDynamicFilters(
                maxDistinctValues,
                ImmutableList.of(REAL),
                ImmutableList.of(
                        new Page(createSequenceBlockOfReal(0, maxDistinctValues + 1)),
                        new Page(createBlockOfReals(Collections.nCopies(maxDistinctValues + 1, Float.NaN)))),
                ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectTooMuchRowsNonOrderable()
    {
        int maxDistinctValues = 100;
        assertDynamicFilters(
                maxDistinctValues,
                ImmutableList.of(COLOR),
                ImmutableList.of(new Page(createColorSequenceBlock(0, maxDistinctValues + 1))),
                ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectRowsNonOrderable()
    {
        int maxDistinctValues = 100;
        Block block = createColorSequenceBlock(0, maxDistinctValues / 2);
        ImmutableList.Builder<Object> values = ImmutableList.builder();
        for (int position = 0; position < block.getPositionCount(); ++position) {
            values.add(readNativeValue(COLOR, block, position));
        }

        assertDynamicFilters(
                maxDistinctValues,
                ImmutableList.of(COLOR),
                ImmutableList.of(new Page(block)),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"),
                        Domain.create(ValueSet.copyOf(COLOR, values.build()), false)))));
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
    public void testSingleColumnCollectMinMaxRangeWhenTooManyPositions()
    {
        int maxDistinctValues = 100;
        Page largePage = new Page(createLongSequenceBlock(0, maxDistinctValues + 1));

        assertDynamicFilters(
                maxDistinctValues,
                ImmutableList.of(BIGINT),
                ImmutableList.of(largePage),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"),
                        Domain.create(
                                ValueSet.ofRanges(range(BIGINT, 0L, true, (long) maxDistinctValues, true)),
                                false)))));
    }

    @Test
    public void testMultipleColumnsCollectBelowDistinctValuesLimit()
    {
        int maxDistinctValues = 101;
        Page largePage = new Page(
                createLongSequenceBlock(0, 101),
                createLongSequenceBlock(100, 201),
                createLongSequenceBlock(200, 301));

        List<TupleDomain<DynamicFilterId>> expectedTupleDomains = ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), Domain.create(
                                ValueSet.ofRanges(Range.range(BIGINT, 0L, true, 100L, true)),
                                false),
                        new DynamicFilterId("1"), Domain.create(
                                ValueSet.ofRanges(Range.range(BIGINT, 100L, true, 200L, true)),
                                false),
                        new DynamicFilterId("2"), Domain.create(
                                ValueSet.ofRanges(Range.range(BIGINT, 200L, true, 300L, true)),
                                false))));
        assertDynamicFilters(maxDistinctValues, ImmutableList.of(BIGINT, BIGINT, BIGINT), ImmutableList.of(largePage), expectedTupleDomains);
    }

    @Test
    public void testMultipleColumnsCollectMinMaxRangeWhenTooManyDistinctValues()
    {
        int maxDistinctValues = 100;
        Page largePage = new Page(
                createLongSequenceBlock(0, 101),
                createColorRepeatBlock(100, 101),
                createLongRepeatBlock(200, 101));

        List<TupleDomain<DynamicFilterId>> expectedTupleDomains = ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), Domain.create(ValueSet.ofRanges(
                                range(BIGINT, 0L, true, 100L, true)), false),
                        new DynamicFilterId("2"), Domain.create(ValueSet.ofRanges(
                                equal(BIGINT, 200L)), false))));
        assertDynamicFilters(maxDistinctValues, ImmutableList.of(BIGINT, COLOR, BIGINT), ImmutableList.of(largePage), expectedTupleDomains);
    }

    @Test
    public void testMultipleColumnsCollectMinMaxWithNulls()
    {
        int maxDistinctValues = 100;
        Page largePage = new Page(
                createLongsBlock(Collections.nCopies(100, null)),
                createLongSequenceBlock(200, 301));

        assertDynamicFilters(
                maxDistinctValues,
                ImmutableList.of(BIGINT, BIGINT),
                ImmutableList.of(largePage),
                ImmutableList.of(TupleDomain.none()));
    }

    @Test
    public void testSingleColumnCollectMinMaxRangeWhenTooManyBytes()
    {
        DataSize maxSize = DataSize.of(10, KILOBYTE);
        long maxByteSize = maxSize.toBytes();
        String largeText = "A".repeat((int) maxByteSize + 1);
        Page largePage = new Page(createStringsBlock(largeText));

        assertDynamicFilters(
                100,
                maxSize,
                100,
                ImmutableList.of(VARCHAR),
                ImmutableList.of(largePage),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"),
                        Domain.create(
                                ValueSet.ofRanges(range(VARCHAR, utf8Slice(largeText), true, utf8Slice(largeText), true)),
                                false)))));
    }

    @Test
    public void testMultipleColumnsCollectMinMaxRangeWhenTooManyBytes()
    {
        DataSize maxSize = DataSize.of(10, KILOBYTE);
        long maxByteSize = maxSize.toBytes();
        String largeTextA = "A".repeat((int) (maxByteSize / 2) + 1);
        String largeTextB = "B".repeat((int) (maxByteSize / 2) + 1);
        Page largePage = new Page(createStringsBlock(largeTextA), createStringsBlock(largeTextB));

        List<TupleDomain<DynamicFilterId>> expectedTupleDomains = ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), Domain.create(ValueSet.ofRanges(
                                range(VARCHAR, utf8Slice(largeTextA), true, utf8Slice(largeTextA), true)), false),
                        new DynamicFilterId("1"), Domain.create(ValueSet.ofRanges(
                                range(VARCHAR, utf8Slice(largeTextB), true, utf8Slice(largeTextB), true)), false))));
        assertDynamicFilters(
                100,
                maxSize,
                100,
                ImmutableList.of(VARCHAR, VARCHAR),
                ImmutableList.of(largePage),
                expectedTupleDomains);
    }

    @Test
    public void testCollectMultipleLargePages()
    {
        int maxDistinctValues = 100;
        Page page1 = new Page(createLongSequenceBlock(50, 151));
        Page page2 = new Page(createLongSequenceBlock(0, 101));
        Page page3 = new Page(createLongSequenceBlock(100, 201));

        assertDynamicFilters(
                maxDistinctValues,
                ImmutableList.of(BIGINT),
                ImmutableList.of(page1, page2, page3),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"),
                        Domain.create(
                                ValueSet.ofRanges(range(BIGINT, 0L, true, 200L, true)),
                                false)))));
    }

    @Test
    public void testCollectDeduplication()
    {
        int maxDistinctValues = 100;
        Page largePage = new Page(createLongRepeatBlock(7, maxDistinctValues * 10)); // lots of zeros
        Page nullsPage = new Page(createLongsBlock(Arrays.asList(new Long[maxDistinctValues * 10]))); // lots of nulls

        assertDynamicFilters(
                maxDistinctValues,
                ImmutableList.of(BIGINT),
                ImmutableList.of(largePage, nullsPage),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"),
                        Domain.create(ValueSet.of(BIGINT, 7L), false)))));
    }

    @Test
    public void testCollectMinMaxLimitSinglePage()
    {
        int maxDistinctValues = 100;
        assertDynamicFilters(
                maxDistinctValues,
                DataSize.of(10, KILOBYTE),
                2 * maxDistinctValues,
                ImmutableList.of(BIGINT),
                ImmutableList.of(new Page(createLongSequenceBlock(0, (2 * maxDistinctValues) + 1))),
                ImmutableList.of(TupleDomain.all()));
    }

    @DataProvider
    public Object[][] denseType()
    {
        return new Object[][] {{BIGINT}, {INTEGER}, {SMALLINT}, {TINYINT}, {createDecimalType(3)}};
    }

    @Test(dataProvider = "denseType")
    public void testCollectCompactedDomain(Type type)
    {
        int maxDistinctValues = 100;
        List<Long> values = LongStream.range(0, 10).boxed().collect(Collectors.toList());
        assertDynamicFilters(
                maxDistinctValues,
                DataSize.of(10, KILOBYTE),
                2 * maxDistinctValues,
                ImmutableList.of(type),
                ImmutableList.of(new Page(createTypedLongsBlock(type, values))),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"),
                        Domain.create(ValueSet.ofRanges(
                                Range.range(type, 0L, true, 9L, true)), false)))));
    }

    @Test
    public void testCollectUncompactedDomain()
    {
        Type type = REAL; // doesn't support consecutive values' compaction
        int maxDistinctValues = 100;
        List<Long> values = LongStream.range(0, 10).boxed().collect(Collectors.toList());
        assertDynamicFilters(
                maxDistinctValues,
                DataSize.of(10, KILOBYTE),
                2 * maxDistinctValues,
                ImmutableList.of(type),
                ImmutableList.of(new Page(createTypedLongsBlock(type, values))),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"),
                        Domain.multipleValues(type, values)))));
    }

    @Test
    public void testCollectMinMaxLimitMultiplePages()
    {
        int maxDistinctValues = 100;
        assertDynamicFilters(
                maxDistinctValues,
                DataSize.of(10, KILOBYTE),
                (2 * maxDistinctValues) + 1,
                ImmutableList.of(BIGINT),
                ImmutableList.of(
                        new Page(createLongSequenceBlock(0, maxDistinctValues + 1)),
                        new Page(createLongSequenceBlock(0, maxDistinctValues + 1))),
                ImmutableList.of(TupleDomain.all()));
    }
}
