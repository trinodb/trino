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
package io.trino.cost;

import io.trino.sql.planner.Symbol;
import org.assertj.core.api.AbstractDoubleAssert;
import org.junit.jupiter.api.Test;

import static io.trino.cost.PlanNodeStatsEstimateMath.addStatsAndMaxDistinctValues;
import static io.trino.cost.PlanNodeStatsEstimateMath.addStatsAndSumDistinctValues;
import static io.trino.cost.PlanNodeStatsEstimateMath.capStats;
import static io.trino.cost.PlanNodeStatsEstimateMath.subtractSubsetStats;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPlanNodeStatsEstimateMath
{
    private static final Symbol SYMBOL = new Symbol(UNKNOWN, "symbol");
    private static final StatisticRange NON_EMPTY_RANGE = openRange(1);

    @Test
    public void testAddRowCount()
    {
        PlanNodeStatsEstimate unknownStats = statistics(NaN, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate first = statistics(10, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate second = statistics(20, NaN, NaN, StatisticRange.empty());

        assertThat(addStatsAndSumDistinctValues(unknownStats, unknownStats)).isEqualTo(PlanNodeStatsEstimate.unknown());
        assertThat(addStatsAndSumDistinctValues(first, unknownStats)).isEqualTo(PlanNodeStatsEstimate.unknown());
        assertThat(addStatsAndSumDistinctValues(unknownStats, second)).isEqualTo(PlanNodeStatsEstimate.unknown());
        assertThat(addStatsAndSumDistinctValues(first, second).getOutputRowCount()).isEqualTo(30.0);
    }

    @Test
    public void testAddNullsFraction()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate unknownNullsFraction = statistics(10, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate first = statistics(10, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate second = statistics(20, 0.2, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate fractionalRowCountFirst = statistics(0.1, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate fractionalRowCountSecond = statistics(0.2, 0.3, NaN, NON_EMPTY_RANGE);

        assertThatAddNullsFraction(unknownRowCount, unknownRowCount).isNaN();
        assertThatAddNullsFraction(unknownNullsFraction, unknownNullsFraction).isNaN();
        assertThatAddNullsFraction(unknownRowCount, unknownNullsFraction).isNaN();
        assertThatAddNullsFraction(first, unknownNullsFraction).isNaN();
        assertThatAddNullsFraction(unknownRowCount, second).isNaN();
        assertThatAddNullsFraction(first, second).isEqualTo(0.16666666666666666);
        assertThatAddNullsFraction(fractionalRowCountFirst, fractionalRowCountSecond).isEqualTo(0.2333333333333333);
    }

    private static AbstractDoubleAssert<?> assertThatAddNullsFraction(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second)
    {
        return assertThat(addStatsAndSumDistinctValues(first, second).getSymbolStatistics(SYMBOL).getNullsFraction());
    }

    @Test
    public void testAddAverageRowSize()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, 0.1, 10, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate unknownNullsFraction = statistics(10, NaN, 10, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate unknownAverageRowSize = statistics(10, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate first = statistics(10, 0.1, 15, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate second = statistics(20, 0.2, 20, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate fractionalRowCountFirst = statistics(0.1, 0.1, 0.3, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate fractionalRowCountSecond = statistics(0.2, 0.3, 0.4, NON_EMPTY_RANGE);

        assertThatAddAverageRowSize(unknownRowCount, unknownRowCount).isNaN();
        assertThatAddAverageRowSize(unknownNullsFraction, unknownNullsFraction).isNaN();
        assertThatAddAverageRowSize(unknownAverageRowSize, unknownAverageRowSize).isNaN();
        assertThatAddAverageRowSize(first, unknownRowCount).isNaN();
        assertThatAddAverageRowSize(unknownNullsFraction, second).isNaN();
        assertThatAddAverageRowSize(first, unknownAverageRowSize).isNaN();
        assertThatAddAverageRowSize(first, second).isEqualTo(18.2);
        assertThatAddAverageRowSize(fractionalRowCountFirst, fractionalRowCountSecond).isEqualTo(0.3608695652173913);
    }

    private static AbstractDoubleAssert<?> assertThatAddAverageRowSize(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second)
    {
        return assertThat(addStatsAndSumDistinctValues(first, second).getSymbolStatistics(SYMBOL).getAverageRowSize());
    }

    @Test
    public void testSumNumberOfDistinctValues()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate emptyRange = statistics(10, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate unknownRange = statistics(10, NaN, NaN, openRange(NaN));
        PlanNodeStatsEstimate first = statistics(10, NaN, NaN, openRange(2));
        PlanNodeStatsEstimate second = statistics(10, NaN, NaN, openRange(3));

        assertThatSumNumberOfDistinctValues(unknownRowCount, unknownRowCount).isNaN();
        assertThatSumNumberOfDistinctValues(unknownRowCount, second).isNaN();
        assertThatSumNumberOfDistinctValues(first, emptyRange).isEqualTo(2);
        assertThatSumNumberOfDistinctValues(first, unknownRange).isNaN();
        assertThatSumNumberOfDistinctValues(first, second).isEqualTo(5);
    }

    private static AbstractDoubleAssert<?> assertThatSumNumberOfDistinctValues(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second)
    {
        return assertThat(addStatsAndSumDistinctValues(first, second).getSymbolStatistics(SYMBOL).getDistinctValuesCount());
    }

    @Test
    public void testMaxNumberOfDistinctValues()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate emptyRange = statistics(10, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate unknownRange = statistics(10, NaN, NaN, openRange(NaN));
        PlanNodeStatsEstimate first = statistics(10, NaN, NaN, openRange(2));
        PlanNodeStatsEstimate second = statistics(10, NaN, NaN, openRange(3));

        assertThatMaxNumberOfDistinctValues(unknownRowCount, unknownRowCount).isNaN();
        assertThatMaxNumberOfDistinctValues(unknownRowCount, second).isNaN();
        assertThatMaxNumberOfDistinctValues(first, emptyRange).isEqualTo(2);
        assertThatMaxNumberOfDistinctValues(first, unknownRange).isNaN();
        assertThatMaxNumberOfDistinctValues(first, second).isEqualTo(3);
    }

    private static AbstractDoubleAssert<?> assertThatMaxNumberOfDistinctValues(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second)
    {
        return assertThat(addStatsAndMaxDistinctValues(first, second).getSymbolStatistics(SYMBOL).getDistinctValuesCount());
    }

    @Test
    public void testAddRange()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate emptyRange = statistics(10, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate unknownRange = statistics(10, NaN, NaN, openRange(NaN));
        PlanNodeStatsEstimate first = statistics(10, NaN, NaN, new StatisticRange(12, 100, 2));
        PlanNodeStatsEstimate second = statistics(10, NaN, NaN, new StatisticRange(101, 200, 3));

        assertAddRange(unknownRange, unknownRange, NEGATIVE_INFINITY, POSITIVE_INFINITY);
        assertAddRange(unknownRowCount, second, NEGATIVE_INFINITY, POSITIVE_INFINITY);
        assertAddRange(unknownRange, second, NEGATIVE_INFINITY, POSITIVE_INFINITY);
        assertAddRange(emptyRange, second, 101, 200);
        assertAddRange(first, second, 12, 200);
    }

    private static void assertAddRange(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second, double expectedLow, double expectedHigh)
    {
        SymbolStatsEstimate statistics = addStatsAndMaxDistinctValues(first, second).getSymbolStatistics(SYMBOL);
        assertThat(statistics.getLowValue()).isEqualTo(expectedLow);
        assertThat(statistics.getHighValue()).isEqualTo(expectedHigh);
    }

    @Test
    public void testSubtractRowCount()
    {
        PlanNodeStatsEstimate unknownStats = statistics(NaN, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate first = statistics(40, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate second = statistics(10, NaN, NaN, StatisticRange.empty());

        assertThat(subtractSubsetStats(unknownStats, unknownStats)).isEqualTo(PlanNodeStatsEstimate.unknown());
        assertThat(subtractSubsetStats(first, unknownStats)).isEqualTo(PlanNodeStatsEstimate.unknown());
        assertThat(subtractSubsetStats(unknownStats, second)).isEqualTo(PlanNodeStatsEstimate.unknown());
        assertThat(subtractSubsetStats(first, second).getOutputRowCount()).isEqualTo(30.0);
    }

    @Test
    public void testSubtractNullsFraction()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate unknownNullsFraction = statistics(10, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate first = statistics(50, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate second = statistics(20, 0.2, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate fractionalRowCountFirst = statistics(0.7, 0.1, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate fractionalRowCountSecond = statistics(0.2, 0.3, NaN, NON_EMPTY_RANGE);

        assertThatSubtractNullsFraction(unknownRowCount, unknownRowCount).isNaN();
        assertThatSubtractNullsFraction(unknownRowCount, unknownNullsFraction).isNaN();
        assertThatSubtractNullsFraction(first, unknownNullsFraction).isNaN();
        assertThatSubtractNullsFraction(unknownRowCount, second).isNaN();
        assertThatSubtractNullsFraction(first, second).isEqualTo(0.03333333333333333);
        assertThatSubtractNullsFraction(fractionalRowCountFirst, fractionalRowCountSecond).isEqualTo(0.019999999999999993);
    }

    private static AbstractDoubleAssert<?> assertThatSubtractNullsFraction(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second)
    {
        return assertThat(subtractSubsetStats(first, second).getSymbolStatistics(SYMBOL).getNullsFraction());
    }

    @Test
    public void testSubtractNumberOfDistinctValues()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate unknownDistinctValues = statistics(100, 0.1, NaN, openRange(NaN));
        PlanNodeStatsEstimate zero = statistics(0, 0.1, NaN, openRange(0));
        PlanNodeStatsEstimate first = statistics(30, 0.1, NaN, openRange(10));
        PlanNodeStatsEstimate second = statistics(20, 0.1, NaN, openRange(5));
        PlanNodeStatsEstimate third = statistics(10, 0.1, NaN, openRange(3));

        assertThatSubtractNumberOfDistinctValues(unknownRowCount, unknownRowCount).isNaN();
        assertThatSubtractNumberOfDistinctValues(unknownRowCount, second).isNaN();
        assertThatSubtractNumberOfDistinctValues(unknownDistinctValues, second).isNaN();
        assertThatSubtractNumberOfDistinctValues(first, zero).isEqualTo(10);
        assertThatSubtractNumberOfDistinctValues(zero, zero).isEqualTo(0);
        assertThatSubtractNumberOfDistinctValues(first, second).isEqualTo(5);
        assertThatSubtractNumberOfDistinctValues(second, third).isEqualTo(5);
    }

    private static AbstractDoubleAssert<?> assertThatSubtractNumberOfDistinctValues(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second)
    {
        return assertThat(subtractSubsetStats(first, second).getSymbolStatistics(SYMBOL).getDistinctValuesCount());
    }

    @Test
    public void testSubtractRange()
    {
        assertSubtractRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, NEGATIVE_INFINITY, POSITIVE_INFINITY, NEGATIVE_INFINITY, POSITIVE_INFINITY);
        assertSubtractRange(0, 1, NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1);
        assertSubtractRange(NaN, NaN, 0, 1, NaN, NaN);
        assertSubtractRange(0, 1, NaN, NaN, 0, 1);
        assertSubtractRange(0, 2, 0, 1, 0, 2);
        assertSubtractRange(0, 2, 1, 2, 0, 2);
        assertSubtractRange(0, 2, 0.5, 1, 0, 2);
    }

    private static void assertSubtractRange(double supersetLow, double supersetHigh, double subsetLow, double subsetHigh, double expectedLow, double expectedHigh)
    {
        PlanNodeStatsEstimate first = statistics(30, NaN, NaN, new StatisticRange(supersetLow, supersetHigh, 10));
        PlanNodeStatsEstimate second = statistics(20, NaN, NaN, new StatisticRange(subsetLow, subsetHigh, 5));
        SymbolStatsEstimate statistics = subtractSubsetStats(first, second).getSymbolStatistics(SYMBOL);
        assertThat(statistics.getLowValue()).isEqualByComparingTo(expectedLow);
        assertThat(statistics.getHighValue()).isEqualByComparingTo(expectedHigh);
    }

    @Test
    public void testCapRowCount()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate first = statistics(20, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate second = statistics(10, NaN, NaN, NON_EMPTY_RANGE);

        assertThat(capStats(unknownRowCount, unknownRowCount).getOutputRowCount()).isNaN();
        assertThat(capStats(first, unknownRowCount).getOutputRowCount()).isNaN();
        assertThat(capStats(unknownRowCount, second).getOutputRowCount()).isNaN();
        assertThat(capStats(first, second).getOutputRowCount()).isEqualTo(10.0);
        assertThat(capStats(second, first).getOutputRowCount()).isEqualTo(10.0);
    }

    @Test
    public void testCapAverageRowSize()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate unknownAverageRowSize = statistics(20, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate first = statistics(20, NaN, 10, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate second = statistics(10, NaN, 5, NON_EMPTY_RANGE);

        assertThatcapAverageRowSize(unknownRowCount, unknownRowCount).isNaN();
        assertThatcapAverageRowSize(unknownAverageRowSize, unknownAverageRowSize).isNaN();
        // average row size should be preserved
        assertThatcapAverageRowSize(first, unknownAverageRowSize).isEqualTo(10);
        assertThatcapAverageRowSize(unknownAverageRowSize, second).isNaN();
        // average row size should be preserved
        assertThatcapAverageRowSize(first, second).isEqualTo(10);
    }

    private static AbstractDoubleAssert<?> assertThatcapAverageRowSize(PlanNodeStatsEstimate stats, PlanNodeStatsEstimate cap)
    {
        return assertThat(capStats(stats, cap).getSymbolStatistics(SYMBOL).getAverageRowSize());
    }

    @Test
    public void testCapNumberOfDistinctValues()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate unknownNumberOfDistinctValues = statistics(20, NaN, NaN, openRange(NaN));
        PlanNodeStatsEstimate first = statistics(20, NaN, NaN, openRange(10));
        PlanNodeStatsEstimate second = statistics(10, NaN, NaN, openRange(5));

        assertThatcapNumberOfDistinctValues(unknownRowCount, unknownRowCount).isNaN();
        assertThatcapNumberOfDistinctValues(unknownNumberOfDistinctValues, unknownNumberOfDistinctValues).isNaN();
        assertThatcapNumberOfDistinctValues(first, unknownRowCount).isNaN();
        assertThatcapNumberOfDistinctValues(unknownNumberOfDistinctValues, second).isNaN();
        assertThatcapNumberOfDistinctValues(first, second).isEqualTo(5);
    }

    private static AbstractDoubleAssert<?> assertThatcapNumberOfDistinctValues(PlanNodeStatsEstimate stats, PlanNodeStatsEstimate cap)
    {
        return assertThat(capStats(stats, cap).getSymbolStatistics(SYMBOL).getDistinctValuesCount());
    }

    @Test
    public void testCapRange()
    {
        PlanNodeStatsEstimate emptyRange = statistics(10, NaN, NaN, StatisticRange.empty());
        PlanNodeStatsEstimate openRange = statistics(10, NaN, NaN, openRange(NaN));
        PlanNodeStatsEstimate first = statistics(10, NaN, NaN, new StatisticRange(12, 100, NaN));
        PlanNodeStatsEstimate second = statistics(10, NaN, NaN, new StatisticRange(13, 99, NaN));

        assertCapRange(emptyRange, emptyRange, NaN, NaN);
        assertCapRange(emptyRange, openRange, NaN, NaN);
        assertCapRange(openRange, emptyRange, NaN, NaN);
        assertCapRange(first, openRange, 12, 100);
        assertCapRange(openRange, second, 13, 99);
        assertCapRange(first, second, 13, 99);
    }

    private static void assertCapRange(PlanNodeStatsEstimate stats, PlanNodeStatsEstimate cap, double expectedLow, double expectedHigh)
    {
        SymbolStatsEstimate symbolStats = capStats(stats, cap).getSymbolStatistics(SYMBOL);
        assertThat(symbolStats.getLowValue()).isEqualByComparingTo(expectedLow);
        assertThat(symbolStats.getHighValue()).isEqualByComparingTo(expectedHigh);
    }

    @Test
    public void testCapNullsFraction()
    {
        PlanNodeStatsEstimate unknownRowCount = statistics(NaN, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate unknownNullsFraction = statistics(10, NaN, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate first = statistics(20, 0.25, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate second = statistics(10, 0.6, NaN, NON_EMPTY_RANGE);
        PlanNodeStatsEstimate third = statistics(0, 0.6, NaN, NON_EMPTY_RANGE);

        assertThatCapNullsFraction(unknownRowCount, unknownRowCount).isNaN();
        assertThatCapNullsFraction(unknownNullsFraction, unknownNullsFraction).isNaN();
        assertThatCapNullsFraction(first, unknownNullsFraction).isNaN();
        assertThatCapNullsFraction(unknownNullsFraction, second).isNaN();
        assertThatCapNullsFraction(first, second).isEqualTo(0.5);
        assertThatCapNullsFraction(first, third).isEqualTo(1);
    }

    private static AbstractDoubleAssert<?> assertThatCapNullsFraction(PlanNodeStatsEstimate stats, PlanNodeStatsEstimate cap)
    {
        return assertThat(capStats(stats, cap).getSymbolStatistics(SYMBOL).getNullsFraction());
    }

    private static PlanNodeStatsEstimate statistics(double rowCount, double nullsFraction, double averageRowSize, StatisticRange range)
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rowCount)
                .addSymbolStatistics(SYMBOL, SymbolStatsEstimate.builder()
                        .setNullsFraction(nullsFraction)
                        .setAverageRowSize(averageRowSize)
                        .setStatisticsRange(range)
                        .build())
                .build();
    }

    private static StatisticRange openRange(double distinctValues)
    {
        return new StatisticRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, distinctValues);
    }
}
