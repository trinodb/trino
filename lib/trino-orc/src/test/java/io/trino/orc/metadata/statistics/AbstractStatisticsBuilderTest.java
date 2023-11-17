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
package io.trino.orc.metadata.statistics;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static io.trino.orc.metadata.statistics.ColumnStatistics.mergeColumnStatistics;
import static java.lang.Math.min;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractStatisticsBuilderTest<B extends StatisticsBuilder, T>
{
    public enum StatisticsType
    {
        NONE, BOOLEAN, INTEGER, DOUBLE, STRING, DATE, DECIMAL, TIMESTAMP
    }

    private final StatisticsType statisticsType;
    private final Supplier<B> statisticsBuilderSupplier;
    private final BiConsumer<B, T> adder;

    public AbstractStatisticsBuilderTest(StatisticsType statisticsType, Supplier<B> statisticsBuilderSupplier, BiConsumer<B, T> adder)
    {
        this.statisticsType = statisticsType;
        this.statisticsBuilderSupplier = statisticsBuilderSupplier;
        this.adder = adder;
    }

    @Test
    public void testNoValue()
    {
        B statisticsBuilder = statisticsBuilderSupplier.get();
        AggregateColumnStatistics aggregateColumnStatistics = new AggregateColumnStatistics();

        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 0);
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertNoColumnStatistics(aggregateColumnStatistics.getMergedColumnStatistics(Optional.empty()), 0);

        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 0);
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertNoColumnStatistics(aggregateColumnStatistics.getMergedColumnStatistics(Optional.empty()), 0);

        assertNoColumnStatistics(statisticsBuilder.buildColumnStatistics(), 0);
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertNoColumnStatistics(aggregateColumnStatistics.getMergedColumnStatistics(Optional.empty()), 0);
    }

    protected void assertMinAverageValueBytes(long expectedAverageValueBytes, List<T> values)
    {
        // test add value
        B statisticsBuilder = statisticsBuilderSupplier.get();
        for (T value : values) {
            adder.accept(statisticsBuilder, value);
        }
        assertThat(statisticsBuilder.buildColumnStatistics().getMinAverageValueSizeInBytes()).isEqualTo(expectedAverageValueBytes);

        // test merge
        statisticsBuilder = statisticsBuilderSupplier.get();
        for (int i = 0; i < values.size() / 2; i++) {
            adder.accept(statisticsBuilder, values.get(i));
        }
        ColumnStatistics firstStats = statisticsBuilder.buildColumnStatistics();

        statisticsBuilder = statisticsBuilderSupplier.get();
        for (int i = values.size() / 2; i < values.size(); i++) {
            adder.accept(statisticsBuilder, values.get(i));
        }
        ColumnStatistics secondStats = statisticsBuilder.buildColumnStatistics();
        assertThat(mergeColumnStatistics(ImmutableList.of(firstStats, secondStats)).getMinAverageValueSizeInBytes()).isEqualTo(expectedAverageValueBytes);
    }

    protected void assertMinMaxValues(T expectedMin, T expectedMax)
    {
        // just min
        assertValues(expectedMin, expectedMin, ImmutableList.of(expectedMin));

        // just max
        assertValues(expectedMax, expectedMax, ImmutableList.of(expectedMax));

        // both
        assertValues(expectedMin, expectedMax, ImmutableList.of(expectedMin, expectedMax));
    }

    protected void assertValues(T expectedMin, T expectedMax, List<T> values)
    {
        assertValuesInternal(expectedMin, expectedMax, values);
        assertValuesInternal(expectedMin, expectedMax, ImmutableList.copyOf(values).reverse());

        List<T> randomOrder = new ArrayList<>(values);
        Collections.shuffle(randomOrder, new Random(42));
        assertValuesInternal(expectedMin, expectedMax, randomOrder);
    }

    private void assertValuesInternal(T expectedMin, T expectedMax, List<T> values)
    {
        B statisticsBuilder = statisticsBuilderSupplier.get();
        AggregateColumnStatistics aggregateColumnStatistics = new AggregateColumnStatistics();
        aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
        assertColumnStatistics(statisticsBuilder.buildColumnStatistics(), 0, null, null, aggregateColumnStatistics);

        for (int loop = 0; loop < 4; loop++) {
            for (T value : values) {
                adder.accept(statisticsBuilder, value);
                aggregateColumnStatistics.add(statisticsBuilder.buildColumnStatistics());
            }
            assertColumnStatistics(statisticsBuilder.buildColumnStatistics(), values.size() * (loop + 1), expectedMin, expectedMax, aggregateColumnStatistics);
        }
    }

    protected static void assertNoColumnStatistics(ColumnStatistics columnStatistics, long expectedNumberOfValues)
    {
        assertThat(columnStatistics.getNumberOfValues()).isEqualTo(expectedNumberOfValues);
        assertThat(columnStatistics.getBooleanStatistics()).isNull();
        assertThat(columnStatistics.getIntegerStatistics()).isNull();
        assertThat(columnStatistics.getDoubleStatistics()).isNull();
        assertThat(columnStatistics.getStringStatistics()).isNull();
        assertThat(columnStatistics.getDateStatistics()).isNull();
        assertThat(columnStatistics.getDecimalStatistics()).isNull();
        assertThat(columnStatistics.getBloomFilter()).isNull();
    }

    protected static void assertNoColumnStatistics(ColumnStatistics columnStatistics, int expectedNumberOfValues, int expectedNumberOfNanValues)
    {
        assertNoColumnStatistics(columnStatistics, expectedNumberOfValues);
        assertThat(columnStatistics.getNumberOfNanValues()).isEqualTo(expectedNumberOfNanValues);
    }

    private void assertColumnStatistics(
            ColumnStatistics columnStatistics,
            int expectedNumberOfValues,
            T expectedMin,
            T expectedMax,
            AggregateColumnStatistics aggregateColumnStatistics)
    {
        assertColumnStatistics(columnStatistics, expectedNumberOfValues, expectedMin, expectedMax);

        // merge in forward order
        long totalCount = aggregateColumnStatistics.getTotalCount();
        assertColumnStatistics(aggregateColumnStatistics.getMergedColumnStatistics(Optional.empty()), totalCount, expectedMin, expectedMax);
        assertColumnStatistics(aggregateColumnStatistics.getMergedColumnStatisticsPairwise(Optional.empty()), totalCount, expectedMin, expectedMax);

        // merge in a random order
        for (int i = 0; i < 10; i++) {
            assertColumnStatistics(aggregateColumnStatistics.getMergedColumnStatistics(Optional.of(ThreadLocalRandom.current())), totalCount, expectedMin, expectedMax);
            assertColumnStatistics(aggregateColumnStatistics.getMergedColumnStatisticsPairwise(Optional.of(ThreadLocalRandom.current())), totalCount, expectedMin, expectedMax);
        }

        List<ColumnStatistics> statisticsList = aggregateColumnStatistics.getStatisticsList();
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, 0, 10)), totalCount + 10);
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, statisticsList.size(), 10)), totalCount + 10);
        assertNoColumnStatistics(mergeColumnStatistics(insertEmptyColumnStatisticsAt(statisticsList, statisticsList.size() / 2, 10)), totalCount + 10);
    }

    static List<ColumnStatistics> insertEmptyColumnStatisticsAt(List<ColumnStatistics> statisticsList, int index, long numberOfValues)
    {
        List<ColumnStatistics> newStatisticsList = new ArrayList<>(statisticsList);
        newStatisticsList.add(index, new ColumnStatistics(numberOfValues, 0, null, null, null, null, null, null, null, null, null, null));
        return newStatisticsList;
    }

    protected void assertColumnStatistics(ColumnStatistics columnStatistics, long expectedNumberOfValues, T expectedMin, T expectedMax)
    {
        assertThat(columnStatistics.getNumberOfValues()).isEqualTo(expectedNumberOfValues);

        if (statisticsType == StatisticsType.BOOLEAN && expectedNumberOfValues > 0) {
            assertThat(columnStatistics.getBooleanStatistics()).isNotNull();
        }
        else {
            assertThat(columnStatistics.getBooleanStatistics()).isNull();
        }

        if (statisticsType == StatisticsType.INTEGER && expectedNumberOfValues > 0) {
            assertRangeStatistics(columnStatistics.getIntegerStatistics(), expectedMin, expectedMax);
        }
        else {
            assertThat(columnStatistics.getIntegerStatistics()).isNull();
        }

        if (statisticsType == StatisticsType.DOUBLE && expectedNumberOfValues > 0) {
            assertRangeStatistics(columnStatistics.getDoubleStatistics(), expectedMin, expectedMax);
        }
        else {
            assertThat(columnStatistics.getDoubleStatistics()).isNull();
        }

        if (statisticsType == StatisticsType.STRING && expectedNumberOfValues > 0) {
            assertRangeStatistics(columnStatistics.getStringStatistics(), expectedMin, expectedMax);
        }
        else {
            assertThat(columnStatistics.getStringStatistics()).isNull();
        }

        if (statisticsType == StatisticsType.DATE && expectedNumberOfValues > 0) {
            assertRangeStatistics(columnStatistics.getDateStatistics(), expectedMin, expectedMax);
        }
        else {
            assertThat(columnStatistics.getDateStatistics()).isNull();
        }

        if (statisticsType == StatisticsType.DECIMAL && expectedNumberOfValues > 0) {
            assertRangeStatistics(columnStatistics.getDecimalStatistics(), expectedMin, expectedMax);
        }
        else {
            assertThat(columnStatistics.getDecimalStatistics()).isNull();
        }

        assertThat(columnStatistics.getBloomFilter()).isNull();
    }

    void assertRangeStatistics(RangeStatistics<?> rangeStatistics, T expectedMin, T expectedMax)
    {
        assertThat(rangeStatistics).isNotNull();
        assertThat(rangeStatistics.getMin()).isEqualTo(expectedMin);
        assertThat(rangeStatistics.getMax()).isEqualTo(expectedMax);
    }

    public static class AggregateColumnStatistics
    {
        private long totalCount;
        private final ImmutableList.Builder<ColumnStatistics> statisticsList = ImmutableList.builder();

        public void add(ColumnStatistics columnStatistics)
        {
            totalCount += columnStatistics.getNumberOfValues();
            statisticsList.add(columnStatistics);
        }

        public long getTotalCount()
        {
            return totalCount;
        }

        public List<ColumnStatistics> getStatisticsList()
        {
            return statisticsList.build();
        }

        public ColumnStatistics getMergedColumnStatistics(Optional<Random> random)
        {
            List<ColumnStatistics> statistics = new ArrayList<>(statisticsList.build());
            random.ifPresent(rand -> Collections.shuffle(statistics, rand));
            return mergeColumnStatistics(ImmutableList.copyOf(statistics));
        }

        public ColumnStatistics getMergedColumnStatisticsPairwise(Optional<Random> random)
        {
            List<ColumnStatistics> statistics = new ArrayList<>(statisticsList.build());
            random.ifPresent(rand -> Collections.shuffle(statistics, rand));
            return getMergedColumnStatisticsPairwise(ImmutableList.copyOf(statistics));
        }

        private static ColumnStatistics getMergedColumnStatisticsPairwise(List<ColumnStatistics> statistics)
        {
            while (statistics.size() > 1) {
                ImmutableList.Builder<ColumnStatistics> mergedStatistics = ImmutableList.builder();
                for (int i = 0; i < statistics.size(); i += 2) {
                    mergedStatistics.add(mergeColumnStatistics(statistics.subList(i, min(i + 2, statistics.size()))));
                }
                statistics = mergedStatistics.build();
            }
            return statistics.get(0);
        }
    }
}
