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
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.BloomFilterWithRange;
import io.trino.sql.planner.DynamicFilterDomain;
import io.trino.sql.planner.DynamicFilterSourceConsumer;
import io.trino.sql.planner.DynamicFilterTupleDomain;
import io.trino.sql.planner.LongBloomFilter;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.util.DynamicFiltersTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createColorRepeatBlock;
import static io.trino.block.BlockAssertions.createColorSequenceBlock;
import static io.trino.block.BlockAssertions.createDoubleRepeatBlock;
import static io.trino.block.BlockAssertions.createDoubleSequenceBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createLongRepeatBlock;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createSequenceBlockOfReal;
import static io.trino.operator.OperatorAssertion.finishOperator;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.operator.OperatorAssertion.toPagesPartial;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static io.trino.type.ColorType.COLOR;
import static java.lang.Float.floatToRawIntBits;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestDynamicFilterSourceOperator
{
    private TypeOperators typeOperators;
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private PipelineContext pipelineContext;

    private ImmutableList.Builder<DynamicFilterTupleDomain<DynamicFilterId>> partitions;

    @BeforeEach
    public void setUp()
    {
        typeOperators = new TypeOperators();
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
        pipelineContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false);

        partitions = ImmutableList.builder();
    }

    @AfterEach
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
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    private OperatorFactory createOperatorFactory(DynamicFilterSourceOperator.Channel... buildChannels)
    {
        return createOperatorFactory(100, 100 * 2, DataSize.of(10, KILOBYTE), Arrays.asList(buildChannels));
    }

    private OperatorFactory createOperatorFactory(
            int maxFilterDistinctValues,
            int bloomFilterMaxDistinctValues,
            DataSize maxFilterSize,
            Iterable<DynamicFilterSourceOperator.Channel> buildChannels)
    {
        return new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                0,
                new PlanNodeId("PLAN_NODE_ID"),
                new DynamicFilterSourceConsumer()
                {
                    @Override
                    public void addPartition(DynamicFilterTupleDomain<DynamicFilterId> tupleDomain)
                    {
                        partitions.add(tupleDomain);
                    }

                    @Override
                    public void setPartitionCount(int partitionCount) {}

                    @Override
                    public boolean isDomainCollectionComplete()
                    {
                        return false;
                    }
                },
                ImmutableList.copyOf(buildChannels),
                maxFilterDistinctValues,
                bloomFilterMaxDistinctValues,
                maxFilterSize,
                typeOperators);
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
        assertDynamicFilters(
                maxFilterDistinctValues,
                maxFilterDistinctValues * 2,
                DataSize.of(10, KILOBYTE),
                types,
                pages,
                expectedTupleDomains.stream()
                        .map(DynamicFiltersTestUtil::toDynamicFilterTupleDomain)
                        .collect(toImmutableList()));
    }

    private void assertDynamicFilters(
            int maxFilterDistinctValues,
            int bloomFilterMaxDistinctValues,
            DataSize maxFilterSize,
            List<Type> types,
            List<Page> pages,
            List<DynamicFilterTupleDomain<DynamicFilterId>> expectedTupleDomains)
    {
        List<DynamicFilterSourceOperator.Channel> buildChannels = IntStream.range(0, types.size())
                .mapToObj(i -> channel(i, types.get(i)))
                .collect(toImmutableList());
        OperatorFactory operatorFactory = createOperatorFactory(maxFilterDistinctValues, bloomFilterMaxDistinctValues, maxFilterSize, buildChannels);
        Operator operator = createOperator(operatorFactory);
        verifyPassthrough(operator, types, pages);
        operatorFactory.noMoreOperators();
        assertThat(operator.getOperatorContext().getOperatorMemoryContext().getUserMemory()).isEqualTo(0);
        assertThat(partitions.build()).isEqualTo(expectedTupleDomains);
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
        assertThat(partitions.build()).isEqualTo(ImmutableList.of(
                DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), DynamicFilterDomain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 5L))))));

        verifyPassthrough(op2,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(2, 3)),
                new Page(createLongsBlock(1, 4)));

        assertThat(partitions.build()).isEqualTo(ImmutableList.of(
                DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), DynamicFilterDomain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 5L)))),
                DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), DynamicFilterDomain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 4L))))));
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

        assertThat(partitions.build()).isEqualTo(ImmutableList.of(
                DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), DynamicFilterDomain.multipleValues(BOOLEAN, ImmutableList.of(true, false)),
                        new DynamicFilterId("1"), DynamicFilterDomain.multipleValues(DOUBLE, ImmutableList.of(1.5, 3.0, 4.5))))));
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

        assertThat(partitions.build()).isEqualTo(ImmutableList.of(
                DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), DynamicFilterDomain.multipleValues(BOOLEAN, ImmutableList.of(true, false))))));
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

        assertThat(partitions.build()).isEqualTo(ImmutableList.of(
                DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("1"), DynamicFilterDomain.multipleValues(DOUBLE, ImmutableList.of(1.5, 3.0, 4.5))))));
    }

    @Test
    public void testCollectWithNulls()
    {
        BlockBuilder blockBuilder = INTEGER.createFixedSizeBlockBuilder(3);
        INTEGER.writeInt(blockBuilder, 3);
        blockBuilder.appendNull();
        INTEGER.writeInt(blockBuilder, 4);
        Block blockWithNulls = blockBuilder.build();

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, INTEGER));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(INTEGER),
                new Page(createIntsBlock(1, 2, 3)),
                new Page(blockWithNulls),
                new Page(createIntsBlock(4, 5)));
        operatorFactory.noMoreOperators();

        assertThat(partitions.build()).isEqualTo(ImmutableList.of(
                DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), DynamicFilterDomain.fromDomain(Domain.create(ValueSet.of(INTEGER, 1L, 2L, 3L, 4L, 5L), false))))));
    }

    @Test
    public void testCollectWithDoubleNaN()
    {
        BlockBuilder input = DOUBLE.createFixedSizeBlockBuilder(10);
        DOUBLE.writeDouble(input, 42.0);
        DOUBLE.writeDouble(input, Double.NaN);

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, DOUBLE));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(DOUBLE),
                new Page(input.build()));
        operatorFactory.noMoreOperators();

        assertThat(partitions.build()).isEqualTo(ImmutableList.of(
                DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), DynamicFilterDomain.multipleValues(DOUBLE, ImmutableList.of(42.0))))));
    }

    @Test
    public void testCollectWithRealNaN()
    {
        BlockBuilder input = REAL.createFixedSizeBlockBuilder(10);
        REAL.writeLong(input, floatToRawIntBits(42.0f));
        REAL.writeLong(input, floatToRawIntBits(Float.NaN));

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, REAL));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(REAL),
                new Page(input.build()));
        operatorFactory.noMoreOperators();

        assertThat(partitions.build()).isEqualTo(ImmutableList.of(
                DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), DynamicFilterDomain.multipleValues(REAL, ImmutableList.of((long) floatToRawIntBits(42.0f)))))));
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
        assertThat(partitions.build()).isEqualTo(ImmutableList.of(DynamicFilterTupleDomain.all()));
    }

    @Test
    public void testCollectEmptyBuildSide()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BIGINT));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(BIGINT));
        operatorFactory.noMoreOperators();
        assertThat(partitions.build()).isEqualTo(ImmutableList.of(DynamicFilterTupleDomain.none()));
    }

    @Test
    public void testSingleColumnCollectBloomFilterWhenTooManyPositions()
    {
        int maxDistinctValues = 100;
        Page largePage = new Page(createLongSequenceBlock(0, maxDistinctValues + 1));

        assertDynamicFilters(
                maxDistinctValues,
                2 * maxDistinctValues,
                DataSize.of(10, KILOBYTE),
                ImmutableList.of(BIGINT),
                ImmutableList.of(largePage),
                ImmutableList.of(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"),
                        DynamicFilterDomain.fromBloomFilter(new BloomFilterWithRange(
                                createBloomFilter(0, maxDistinctValues),
                                ValueSet.ofRanges(range(BIGINT, 0L, true, (long) maxDistinctValues, true)),
                                BIGINT,
                                false))))));
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
                                ValueSet.copyOf(BIGINT, LongStream.rangeClosed(0L, 100L).boxed().collect(toImmutableList())),
                                false),
                        new DynamicFilterId("1"), Domain.create(
                                ValueSet.copyOf(BIGINT, LongStream.rangeClosed(100L, 200L).boxed().collect(toImmutableList())),
                                false),
                        new DynamicFilterId("2"), Domain.create(
                                ValueSet.copyOf(BIGINT, LongStream.rangeClosed(200L, 300L).boxed().collect(toImmutableList())),
                                false))));
        assertDynamicFilters(maxDistinctValues, ImmutableList.of(BIGINT, BIGINT, BIGINT), ImmutableList.of(largePage), expectedTupleDomains);
    }

    @Test
    public void testMultipleColumnsBothSetAndRangeWhenTooManyDistinctValues()
    {
        int maxDistinctValues = 100;
        Page largePage = new Page(
                createLongSequenceBlock(0, 101),
                createColorRepeatBlock(100, 101),
                createLongRepeatBlock(200, 101));

        List<DynamicFilterTupleDomain<DynamicFilterId>> expectedTupleDomains = ImmutableList.of(
                DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), DynamicFilterDomain.fromBloomFilter(new BloomFilterWithRange(
                                createBloomFilter(0, 100),
                                ValueSet.ofRanges(range(BIGINT, 0L, true, 100L, true)),
                                BIGINT,
                                false)),
                        new DynamicFilterId("1"), DynamicFilterDomain.singleValue(COLOR, 100L),
                        new DynamicFilterId("2"), DynamicFilterDomain.singleValue(BIGINT, 200L))));
        assertDynamicFilters(
                maxDistinctValues,
                2 * maxDistinctValues,
                DataSize.of(10, KILOBYTE),
                ImmutableList.of(BIGINT, COLOR, BIGINT),
                ImmutableList.of(largePage),
                expectedTupleDomains);
    }

    @Test
    public void testMultipleColumnsSingleSetWithNoRangeWhenTooManyDistinctValues()
    {
        Page largePage = new Page(
                createLongSequenceBlock(0, 101),
                createColorRepeatBlock(100, 101),
                createLongRepeatBlock(200, 101));

        List<DynamicFilterTupleDomain<DynamicFilterId>> expectedTupleDomains = ImmutableList.of(
                DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("1"), DynamicFilterDomain.singleValue(COLOR, 100L),
                        new DynamicFilterId("2"), DynamicFilterDomain.singleValue(BIGINT, 200L))));
        assertDynamicFilters(
                80,
                100,
                DataSize.of(10, KILOBYTE),
                ImmutableList.of(BIGINT, COLOR, BIGINT),
                ImmutableList.of(largePage),
                expectedTupleDomains);
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
                2 * maxDistinctValues,
                DataSize.of(10, KILOBYTE),
                ImmutableList.of(BIGINT, BIGINT),
                ImmutableList.of(largePage),
                ImmutableList.of(DynamicFilterTupleDomain.none()));
    }

    @Test
    public void testSingleColumnCollectBloomFilterWhenTooManyBytes()
    {
        DataSize maxSize = DataSize.of(1, KILOBYTE);
        Page largePage = new Page(createLongSequenceBlock(0, 201));

        assertDynamicFilters(
                300,
                300,
                maxSize,
                ImmutableList.of(BIGINT),
                ImmutableList.of(largePage),
                ImmutableList.of(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"),
                        DynamicFilterDomain.fromBloomFilter(new BloomFilterWithRange(
                                createBloomFilter(0, 200),
                                ValueSet.ofRanges(range(BIGINT, 0L, true, 200L, true)),
                                BIGINT,
                                false))))));
    }

    @Test
    public void testMultipleColumnsCollectBloomFilterWhenTooManyBytes()
    {
        DataSize maxSize = DataSize.of(1, KILOBYTE);
        Page largePage = new Page(createLongSequenceBlock(0, 201), createLongSequenceBlock(100, 301));

        List<DynamicFilterTupleDomain<DynamicFilterId>> expectedTupleDomains = ImmutableList.of(
                DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"), DynamicFilterDomain.fromBloomFilter(new BloomFilterWithRange(
                                createBloomFilter(0, 200),
                                ValueSet.ofRanges(range(BIGINT, 0L, true, 200L, true)),
                                BIGINT,
                                false)),
                        new DynamicFilterId("1"), DynamicFilterDomain.fromBloomFilter(new BloomFilterWithRange(
                                createBloomFilter(100, 300),
                                ValueSet.ofRanges(range(BIGINT, 100L, true, 300L, true)),
                                BIGINT,
                                false)))));
        assertDynamicFilters(
                300,
                300,
                maxSize,
                ImmutableList.of(BIGINT, BIGINT),
                ImmutableList.of(largePage),
                expectedTupleDomains);
    }

    @Test
    public void testCollectMultipleLargePages()
    {
        Page page1 = new Page(createLongSequenceBlock(50, 151));
        Page page2 = new Page(createLongSequenceBlock(0, 101));
        Page page3 = new Page(createLongSequenceBlock(100, 201));

        assertDynamicFilters(
                120,
                250,
                DataSize.of(10, KILOBYTE),
                ImmutableList.of(BIGINT),
                ImmutableList.of(page1, page2, page3),
                ImmutableList.of(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("0"),
                        DynamicFilterDomain.fromBloomFilter(new BloomFilterWithRange(
                                createBloomFilter(0, 200),
                                ValueSet.ofRanges(range(BIGINT, 0L, true, 200L, true)),
                                BIGINT,
                                false))))));
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
                maxDistinctValues,
                DataSize.of(10, KILOBYTE),
                ImmutableList.of(BIGINT),
                ImmutableList.of(new Page(createLongSequenceBlock(0, (2 * maxDistinctValues) + 1))),
                ImmutableList.of(DynamicFilterTupleDomain.all()));
    }

    @Test
    public void testCollectMinMaxLimitMultiplePages()
    {
        int maxDistinctValues = 100;
        assertDynamicFilters(
                maxDistinctValues,
                maxDistinctValues,
                DataSize.of(10, KILOBYTE),
                ImmutableList.of(BIGINT),
                ImmutableList.of(
                        new Page(createLongSequenceBlock(0, maxDistinctValues + 1)),
                        new Page(createLongSequenceBlock(0, maxDistinctValues + 1))),
                ImmutableList.of(DynamicFilterTupleDomain.all()));
    }

    @Test
    public void testMemoryUsage()
    {
        OperatorFactory operatorFactory = createOperatorFactory(
                100,
                150,
                DataSize.of(10, KILOBYTE),
                ImmutableList.of(channel(0, BIGINT), channel(1, BIGINT)));
        Operator operator = createOperator(operatorFactory);
        final long initialMemoryUsage = operator.getOperatorContext().getOperatorMemoryContext().getUserMemory();

        List<Page> inputPages = ImmutableList.of(new Page(
                createLongSequenceBlock(51, 151),
                createLongRepeatBlock(200, 100)));
        toPagesPartial(operator, inputPages.iterator());
        long baseMemoryUsage = operator.getOperatorContext().getOperatorMemoryContext().getUserMemory();
        // Hashtable for the first channel has grown
        assertThat(baseMemoryUsage)
                .isGreaterThan(initialMemoryUsage);

        inputPages = ImmutableList.of(new Page(
                createLongSequenceBlock(0, 51),
                createLongSequenceBlock(51, 101)));
        toPagesPartial(operator, inputPages.iterator());
        long firstChannelStoppedMemoryUsage = operator.getOperatorContext().getOperatorMemoryContext().getUserMemory();
        // First channel stops collecting distinct values, so memory will decrease below the initial value since hashtable is freed
        assertThat(firstChannelStoppedMemoryUsage)
                .isGreaterThan(0)
                .isLessThan(initialMemoryUsage);

        toPagesPartial(operator, inputPages.iterator());
        // No change in distinct values
        assertThat(operator.getOperatorContext().getOperatorMemoryContext().getUserMemory()).isEqualTo(firstChannelStoppedMemoryUsage);

        inputPages = ImmutableList.of(new Page(
                createLongSequenceBlock(0, 51),
                createLongSequenceBlock(0, 51)));
        toPagesPartial(operator, inputPages.iterator());
        // Second channel stops collecting distinct values, falls back to bloom filter collection, so memory usage will increase
        assertThat(operator.getOperatorContext().getOperatorMemoryContext().getUserMemory())
                .isGreaterThan(firstChannelStoppedMemoryUsage);

        finishOperator(operator);
        operatorFactory.noMoreOperators();
        LongBloomFilter bloomFilter = createBloomFilter(0, 100);
        bloomFilter.insert(200);
        assertThat(partitions.build()).isEqualTo(ImmutableList.of(
                DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                        new DynamicFilterId("1"),
                        DynamicFilterDomain.fromBloomFilter(new BloomFilterWithRange(
                                bloomFilter,
                                ValueSet.ofRanges(range(BIGINT, 0L, true, 200L, true)),
                                BIGINT,
                                false))))));
    }

    private static LongBloomFilter createBloomFilter(long start, long end)
    {
        LongBloomFilter bloomFilter = new LongBloomFilter();
        for (long i = start; i <= end; i++) {
            bloomFilter.insert(i);
        }
        return bloomFilter;
    }
}
