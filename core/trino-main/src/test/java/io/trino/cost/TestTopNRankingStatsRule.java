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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.TopNRankingNode.RankingType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.cost.SymbolStatsEstimate.unknown;
import static io.trino.cost.SymbolStatsEstimate.zero;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.DENSE_RANK;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.RANK;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;
import static java.lang.Double.NaN;

public class TestTopNRankingStatsRule
        extends BaseStatsCalculatorTest
{
    private final SymbolStatsEstimate xStats = SymbolStatsEstimate.builder()
            .setDistinctValuesCount(5.0)
            .setNullsFraction(0)
            .build();

    private final SymbolStatsEstimate yStats = SymbolStatsEstimate.builder()
            .setDistinctValuesCount(100.0)
            .setNullsFraction(0.5)
            .build();

    private final SymbolStatsEstimate uStats = SymbolStatsEstimate.builder()
            .setDistinctValuesCount(100.0)
            .setNullsFraction(0.5)
            .build();

    private final SymbolStatsEstimate vStats = SymbolStatsEstimate.builder()
            .setDistinctValuesCount(5.0)
            .setNullsFraction(0)
            .build();

    @Test
    public void testRowNumber()
    {
        tester().assertStatsFor(pb -> pb
                        .topNRanking(
                                new DataOrganizationSpecification(
                                        ImmutableList.of(pb.symbol("x", DOUBLE)),
                                        Optional.of(new OrderingScheme(
                                                ImmutableList.of(pb.symbol("y", DOUBLE)),
                                                ImmutableMap.of(pb.symbol("y", DOUBLE), ASC_NULLS_FIRST)))),
                                ROW_NUMBER,
                                10,
                                pb.symbol("z", DOUBLE),
                                pb.values(pb.symbol("x", DOUBLE), pb.symbol("y", DOUBLE))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(new Symbol(DOUBLE, "x"), xStats)
                        .addSymbolStatistics(new Symbol(DOUBLE, "y"), yStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(50)
                        .symbolStats("x", assertion -> assertion.isEqualTo(xStats))
                        .symbolStats("y", assertion -> assertion.distinctValuesCount(5.0))
                        .symbolStats("z", assertion -> assertion
                                .lowValue(1)
                                .highValue(10)
                                .distinctValuesCount(10)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));

        tester().assertStatsFor(pb -> pb
                        .topNRanking(
                                new DataOrganizationSpecification(
                                        ImmutableList.of(pb.symbol("x", DOUBLE), pb.symbol("y", DOUBLE)),
                                        Optional.of(new OrderingScheme(
                                                ImmutableList.of(pb.symbol("u", DOUBLE), pb.symbol("v", DOUBLE)),
                                                ImmutableMap.of(
                                                        pb.symbol("u", DOUBLE), ASC_NULLS_FIRST,
                                                        pb.symbol("v", DOUBLE), ASC_NULLS_FIRST)))),
                                ROW_NUMBER,
                                20,
                                pb.symbol("z", DOUBLE),
                                pb.values(
                                        pb.symbol("x", DOUBLE),
                                        pb.symbol("y", DOUBLE),
                                        pb.symbol("u", DOUBLE),
                                        pb.symbol("v", DOUBLE))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(new Symbol(DOUBLE, "x"), xStats)
                        .addSymbolStatistics(new Symbol(DOUBLE, "y"), yStats)
                        .addSymbolStatistics(new Symbol(DOUBLE, "u"), uStats)
                        .addSymbolStatistics(new Symbol(DOUBLE, "v"), vStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(6366.331316)
                        .symbolStats("x", assertion -> assertion.isEqualTo(xStats))
                        .symbolStats("y", assertion -> assertion.isEqualTo(yStats))
                        .symbolStats("u", assertion -> assertion.isEqualTo(uStats))
                        .symbolStats("v", assertion -> assertion.isEqualTo(vStats))
                        .symbolStats("z", assertion -> assertion
                                .lowValue(1)
                                .highValue(20)
                                .distinctValuesCount(20)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));
    }

    @Test
    public void testRank()
    {
        tester().assertStatsFor(pb -> pb
                        .topNRanking(
                                new DataOrganizationSpecification(
                                        ImmutableList.of(pb.symbol("x", DOUBLE)),
                                        Optional.of(new OrderingScheme(
                                                ImmutableList.of(pb.symbol("y", DOUBLE)),
                                                ImmutableMap.of(pb.symbol("y", DOUBLE), ASC_NULLS_FIRST)))),
                                RANK,
                                10,
                                pb.symbol("z", DOUBLE),
                                pb.values(pb.symbol("x", DOUBLE), pb.symbol("y", DOUBLE))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(new Symbol(DOUBLE, "x"), xStats)
                        .addSymbolStatistics(new Symbol(DOUBLE, "y"), yStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(99.009901)
                        .symbolStats("x", assertion -> assertion.isEqualTo(xStats))
                        .symbolStats("y", assertion -> assertion.distinctValuesCount(5))
                        .symbolStats("z", assertion -> assertion
                                .lowValue(1)
                                .highValue(10)
                                .distinctValuesCount(1)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));

        tester().assertStatsFor(pb -> pb
                        .topNRanking(
                                new DataOrganizationSpecification(
                                        ImmutableList.of(pb.symbol("x", DOUBLE), pb.symbol("y", DOUBLE)),
                                        Optional.of(new OrderingScheme(
                                                ImmutableList.of(pb.symbol("u", DOUBLE), pb.symbol("v", DOUBLE)),
                                                ImmutableMap.of(
                                                        pb.symbol("u", DOUBLE), ASC_NULLS_FIRST,
                                                        pb.symbol("v", DOUBLE), ASC_NULLS_FIRST)))),
                                RANK,
                                20,
                                pb.symbol("z", DOUBLE),
                                pb.values(
                                        pb.symbol("x", DOUBLE),
                                        pb.symbol("y", DOUBLE),
                                        pb.symbol("u", DOUBLE),
                                        pb.symbol("v", DOUBLE))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(new Symbol(DOUBLE, "x"), xStats)
                        .addSymbolStatistics(new Symbol(DOUBLE, "y"), yStats)
                        .addSymbolStatistics(new Symbol(DOUBLE, "u"), uStats)
                        .addSymbolStatistics(new Symbol(DOUBLE, "v"), vStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(6366.331316)
                        .symbolStats("x", assertion -> assertion.isEqualTo(xStats))
                        .symbolStats("y", assertion -> assertion.isEqualTo(yStats))
                        .symbolStats("u", assertion -> assertion.isEqualTo(uStats))
                        .symbolStats("v", assertion -> assertion.isEqualTo(vStats))
                        .symbolStats("z", assertion -> assertion
                                .lowValue(1)
                                .highValue(20)
                                .distinctValuesCount(20)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));
    }

    @Test
    public void testDenseRank()
    {
        tester().assertStatsFor(pb -> pb
                        .topNRanking(
                                new DataOrganizationSpecification(
                                        ImmutableList.of(pb.symbol("x", DOUBLE)),
                                        Optional.of(new OrderingScheme(
                                                ImmutableList.of(pb.symbol("y", DOUBLE)),
                                                ImmutableMap.of(pb.symbol("y", DOUBLE), ASC_NULLS_FIRST)))),
                                DENSE_RANK,
                                3,
                                pb.symbol("z", DOUBLE),
                                pb.values(pb.symbol("x", DOUBLE), pb.symbol("y", DOUBLE))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(new Symbol(DOUBLE, "x"), xStats)
                        .addSymbolStatistics(new Symbol(DOUBLE, "y"), yStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(297.029703)
                        .symbolStats("x", assertion -> assertion.isEqualTo(xStats))
                        .symbolStats("y", assertion -> assertion.distinctValuesCount(15))
                        .symbolStats("z", assertion -> assertion
                                .lowValue(1)
                                .highValue(3)
                                .distinctValuesCount(3)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));

        tester().assertStatsFor(pb -> pb
                        .topNRanking(
                                new DataOrganizationSpecification(
                                        ImmutableList.of(pb.symbol("x", DOUBLE), pb.symbol("y", DOUBLE)),
                                        Optional.of(new OrderingScheme(
                                                ImmutableList.of(pb.symbol("u", DOUBLE), pb.symbol("v", DOUBLE)),
                                                ImmutableMap.of(
                                                        pb.symbol("u", DOUBLE), ASC_NULLS_FIRST,
                                                        pb.symbol("v", DOUBLE), ASC_NULLS_FIRST)))),
                                DENSE_RANK,
                                10,
                                pb.symbol("z", DOUBLE),
                                pb.values(
                                        pb.symbol("x", DOUBLE),
                                        pb.symbol("y", DOUBLE),
                                        pb.symbol("u", DOUBLE),
                                        pb.symbol("v", DOUBLE))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(new Symbol(DOUBLE, "x"), xStats)
                        .addSymbolStatistics(new Symbol(DOUBLE, "y"), yStats)
                        .addSymbolStatistics(new Symbol(DOUBLE, "u"), uStats)
                        .addSymbolStatistics(new Symbol(DOUBLE, "v"), vStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(3183.165658)
                        .symbolStats("x", assertion -> assertion.isEqualTo(xStats))
                        .symbolStats("y", assertion -> assertion.isEqualTo(yStats))
                        .symbolStats("u", assertion -> assertion.isEqualTo(uStats))
                        .symbolStats("v", assertion -> assertion.isEqualTo(vStats))
                        .symbolStats("z", assertion -> assertion
                                .lowValue(1)
                                .highValue(10)
                                .distinctValuesCount(10)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));
    }

    @ParameterizedTest
    @MethodSource("rankTypeWithOutputAndRankSymbolDistinctCount")
    public void testRowNumberWhenOrderByDistinctCountIsNan(RankingType rankingType, double outputRowCount, double rankSymbolDistinctCount)
    {
        tester().assertStatsFor(pb -> pb
                        .topNRanking(
                                new DataOrganizationSpecification(
                                        ImmutableList.of(pb.symbol("x", DOUBLE)),
                                        Optional.of(new OrderingScheme(
                                                ImmutableList.of(pb.symbol("y", DOUBLE)),
                                                ImmutableMap.of(pb.symbol("y", DOUBLE), ASC_NULLS_FIRST)))),
                                rankingType,
                                10,
                                pb.symbol("z", DOUBLE),
                                pb.values(pb.symbol("x", DOUBLE), pb.symbol("y", DOUBLE))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(new Symbol(DOUBLE, "x"), xStats)
                        .addSymbolStatistics(new Symbol(DOUBLE, "y"), unknown())
                        .build())
                .check(check -> check
                        .outputRowsCount(outputRowCount)
                        .symbolStats("x", assertion -> assertion.isEqualTo(xStats))
                        .symbolStats("y", assertion -> assertion.isEqualTo(unknown()))
                        .symbolStats("z", assertion -> assertion
                                .lowValue(1)
                                .highValue(10)
                                .distinctValuesCount(rankSymbolDistinctCount)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));
    }

    public static Stream<Arguments> rankTypeWithOutputAndRankSymbolDistinctCount()
    {
        return Stream.of(
                Arguments.of(ROW_NUMBER, 50, 10),
                Arguments.of(RANK, 10000, 2000),
                Arguments.of(DENSE_RANK, 10000, 2000));
    }

    @ParameterizedTest
    @MethodSource("rankTypes")
    public void testWhenInputRowCountIsNan(RankingType rankingType)
    {
        tester().assertStatsFor(pb -> pb
                        .topNRanking(
                                new DataOrganizationSpecification(
                                        ImmutableList.of(pb.symbol("x", DOUBLE)),
                                        Optional.of(new OrderingScheme(
                                                ImmutableList.of(pb.symbol("y", DOUBLE)),
                                                ImmutableMap.of(pb.symbol("y", DOUBLE), ASC_NULLS_FIRST)))),
                                rankingType,
                                10,
                                pb.symbol("z", DOUBLE),
                                pb.values(pb.symbol("x", DOUBLE), pb.symbol("y", DOUBLE))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(NaN)
                        .addSymbolStatistics(new Symbol(DOUBLE, "x"), unknown())
                        .addSymbolStatistics(new Symbol(DOUBLE, "y"), unknown())
                        .build())
                .check(check -> check
                        .outputRowsCount(NaN)
                        .symbolStats("x", assertion -> assertion.isEqualTo(unknown()))
                        .symbolStats("y", assertion -> assertion.isEqualTo(unknown()))
                        .symbolStats("z", assertion -> assertion.isEqualTo(unknown())));
    }

    @ParameterizedTest
    @MethodSource("rankTypes")
    public void testWhenPartitionByDistinctCountIsNan(RankingType rankingType)
    {
        tester().assertStatsFor(pb -> pb
                        .topNRanking(
                                new DataOrganizationSpecification(
                                        ImmutableList.of(pb.symbol("x", DOUBLE)),
                                        Optional.of(new OrderingScheme(
                                                ImmutableList.of(pb.symbol("y", DOUBLE)),
                                                ImmutableMap.of(pb.symbol("y", DOUBLE), ASC_NULLS_FIRST)))),
                                rankingType,
                                10,
                                pb.symbol("z", DOUBLE),
                                pb.values(pb.symbol("x", DOUBLE), pb.symbol("y", DOUBLE))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(new Symbol(DOUBLE, "x"), unknown())
                        .addSymbolStatistics(new Symbol(DOUBLE, "y"), unknown())
                        .build())
                .check(check -> check
                        .outputRowsCount(10000)
                        .symbolStats("x", assertion -> assertion.isEqualTo(unknown()))
                        .symbolStats("y", assertion -> assertion.isEqualTo(unknown()))
                        .symbolStats("z", assertion -> assertion.isEqualTo(unknown())));
    }

    @ParameterizedTest
    @MethodSource("rankTypes")
    public void testWhenSourceRowCountIsZero(RankingType rankingType)
    {
        tester().assertStatsFor(pb -> pb
                        .topNRanking(
                                new DataOrganizationSpecification(
                                        ImmutableList.of(pb.symbol("x", DOUBLE)),
                                        Optional.of(new OrderingScheme(
                                                ImmutableList.of(pb.symbol("y", DOUBLE)),
                                                ImmutableMap.of(pb.symbol("y", DOUBLE), ASC_NULLS_FIRST)))),
                                rankingType,
                                10,
                                pb.symbol("z", DOUBLE),
                                pb.values(pb.symbol("x", DOUBLE), pb.symbol("y", DOUBLE))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(0)
                        .addSymbolStatistics(new Symbol(DOUBLE, "x"), zero())
                        .addSymbolStatistics(new Symbol(DOUBLE, "y"), zero())
                        .build())
                .check(check -> check
                        .outputRowsCount(0)
                        .symbolStats("x", assertion -> assertion.isEqualTo(zero()))
                        .symbolStats("y", assertion -> assertion.isEqualTo(zero()))
                        .symbolStats("z", assertion -> assertion.isEqualTo(zero())));
    }

    public static Stream<Arguments> rankTypes()
    {
        return Stream.of(
                Arguments.of(ROW_NUMBER),
                Arguments.of(RANK),
                Arguments.of(DENSE_RANK));
    }
}
