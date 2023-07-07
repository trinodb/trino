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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.RowNumberSymbolMatcher;
import io.trino.sql.planner.assertions.TopNRankingSymbolMatcher;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.TopNRankingNode.RankingType;
import io.trino.sql.planner.plan.WindowNode;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.OPTIMIZE_TOP_N_RANKING;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyNot;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topNRanking;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.RANK;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;
import static java.lang.String.format;

public class TestWindowFilterPushDown
        extends BasePlanTest
{
    private static final String ROW_NUMBER_FUNCTION_NAME = "row_number";
    private static final String RANK_FUNCTION_NAME = "rank";

    @Test
    public void testLimitAbovePartitionedWindow()
    {
        assertLimitAbovePartitionedWindow(ROW_NUMBER_FUNCTION_NAME, ROW_NUMBER);
        assertLimitAbovePartitionedWindow(RANK_FUNCTION_NAME, RANK);
    }

    private void assertLimitAbovePartitionedWindow(String rankingFunction, RankingType rankingType)
    {
        @Language("SQL") String sql = format(
                "SELECT %s() OVER (PARTITION BY suppkey ORDER BY orderkey) partition_row_number FROM lineitem LIMIT 10",
                rankingFunction);

        assertPlanWithSession(
                sql,
                optimizeTopNRanking(true),
                true,
                anyTree(
                        limit(10, anyTree(
                                topNRanking(
                                        pattern -> pattern
                                                .rankingType(rankingType)
                                                .maxRankingPerPartition(10),
                                        anyTree(
                                                tableScan("lineitem")))))));

        assertPlanWithSession(
                sql,
                optimizeTopNRanking(false),
                true,
                anyTree(
                        limit(10, anyTree(
                                node(WindowNode.class,
                                        anyTree(
                                                tableScan("lineitem")))))));
    }

    @Test
    public void testLimitAboveUnpartitionedWindow()
    {
        // Unpartitioned row number guarantees an output row count, so final limit should be eliminated
        assertPlanWithSession(
                "SELECT row_number() OVER (ORDER BY orderkey) partition_row_number FROM lineitem LIMIT 10",
                optimizeTopNRanking(true),
                true,
                output(
                        project(
                                topNRanking(
                                        pattern -> pattern
                                                .rankingType(RankingType.ROW_NUMBER)
                                                .maxRankingPerPartition(10),
                                        anyTree(
                                                tableScan("lineitem"))))));

        // Rank can produce more rows than limit, and thus needs a final limit
        assertPlanWithSession(
                "SELECT rank() OVER (ORDER BY orderkey) partition_row_number FROM lineitem LIMIT 10",
                optimizeTopNRanking(true),
                true,
                anyTree(
                        limit(10,
                                anyTree(
                                        topNRanking(
                                                pattern -> pattern
                                                        .rankingType(RankingType.RANK)
                                                        .maxRankingPerPartition(10),
                                                anyTree(
                                                        tableScan("lineitem")))))));
    }

    @Test
    public void testFilterAboveWindow()
    {
        assertFilterAboveWindow(ROW_NUMBER_FUNCTION_NAME, RankingType.ROW_NUMBER);
        assertFilterAboveWindow(RANK_FUNCTION_NAME, RankingType.RANK);
    }

    private void assertFilterAboveWindow(String rankingFunction, RankingType rankingType)
    {
        @Language("SQL") String sql = format(
                "SELECT * FROM " +
                        "(SELECT %s() OVER (PARTITION BY suppkey ORDER BY orderkey) partition_ranking FROM lineitem) " +
                        "WHERE partition_ranking < 10",
                rankingFunction);

        assertPlanWithSession(
                sql,
                optimizeTopNRanking(true),
                true,
                anyTree(
                        anyNot(FilterNode.class,
                                topNRanking(pattern -> pattern
                                                .rankingType(rankingType)
                                                .maxRankingPerPartition(9),
                                        anyTree(
                                                tableScan("lineitem"))))));

        assertPlanWithSession(
                sql,
                optimizeTopNRanking(false),
                true,
                anyTree(
                        node(FilterNode.class,
                                anyTree(
                                        node(WindowNode.class,
                                                anyTree(
                                                        tableScan("lineitem")))))));

        // remove subplan if predicate on row number symbol can't be satisfied
        assertPlanWithSession(
                format("SELECT * FROM (SELECT name, %s() OVER(ORDER BY name) FROM nation) t(name, ranking) WHERE ranking < 0", rankingFunction),
                optimizeTopNRanking(true),
                true,
                output(
                        ImmutableList.of("name", "ranking"),
                        values("name", "ranking")));

        // optimize to TopNRanking on the basis of predicate; remove filter because predicate is satisfied
        assertPlanWithSession(
                format(
                        "SELECT * FROM (SELECT name, %s() OVER(ORDER BY name) FROM nation) t(name, ranking) WHERE ranking > 0 AND ranking < 2", rankingFunction),
                optimizeTopNRanking(true),
                true,
                output(
                        ImmutableList.of("name", "ranking"),
                        topNRanking(
                                pattern -> pattern
                                        .rankingType(rankingType)
                                        .maxRankingPerPartition(1)
                                        .specification(ImmutableList.of(), ImmutableList.of("name"), ImmutableMap.of("name", ASC_NULLS_LAST)),
                                any(
                                        tableScan("nation", ImmutableMap.of("name", "name"))))
                                .withAlias("ranking", new TopNRankingSymbolMatcher())));

        // optimize to TopNRanking on the basis of predicate; remove filter because predicate is satisfied
        assertPlanWithSession(
                format(
                        "SELECT * FROM (SELECT name, %s() OVER(ORDER BY name) FROM nation) t(name, ranking) WHERE ranking <= 1", rankingFunction),
                optimizeTopNRanking(true),
                true,
                output(
                        ImmutableList.of("name", "ranking"),
                        topNRanking(
                                pattern -> pattern
                                        .rankingType(rankingType)
                                        .maxRankingPerPartition(1)
                                        .specification(ImmutableList.of(), ImmutableList.of("name"), ImmutableMap.of("name", ASC_NULLS_LAST)),
                                any(
                                        tableScan("nation", ImmutableMap.of("name", "name"))))
                                .withAlias("ranking", new TopNRankingSymbolMatcher())));

        // optimize to TopNRanking on the basis of predicate; remove filter because predicate is satisfied
        assertPlanWithSession(
                format(
                        "SELECT * FROM (SELECT name, %s() OVER(ORDER BY name) FROM nation) t(name, ranking) WHERE ranking <= 1 AND ranking > -10", rankingFunction),
                optimizeTopNRanking(true),
                true,
                output(
                        ImmutableList.of("name", "ranking"),
                        topNRanking(
                                pattern -> pattern
                                        .rankingType(rankingType)
                                        .maxRankingPerPartition(1)
                                        .specification(ImmutableList.of(), ImmutableList.of("name"), ImmutableMap.of("name", ASC_NULLS_LAST)),
                                any(
                                        tableScan("nation", ImmutableMap.of("name", "name"))))
                                .withAlias("ranking", new TopNRankingSymbolMatcher())));

        // optimize to TopNRanking on the basis of predicate; cannot remove filter because predicate is not satisfied
        assertPlanWithSession(
                format("SELECT * FROM (SELECT name, %s() OVER(ORDER BY name) FROM nation) t(name, ranking) WHERE ranking > 1 AND ranking < 3", rankingFunction),
                optimizeTopNRanking(true),
                true,
                output(
                        ImmutableList.of("name", "ranking"),
                        filter(
                                "(ranking > BIGINT '1') AND (ranking < BIGINT '3')",
                                topNRanking(
                                        pattern -> pattern
                                                .rankingType(rankingType)
                                                .maxRankingPerPartition(2)
                                                .specification(ImmutableList.of(), ImmutableList.of("name"), ImmutableMap.of("name", ASC_NULLS_LAST)),
                                        any(
                                                tableScan("nation", ImmutableMap.of("name", "name"))))
                                        .withAlias("ranking", new TopNRankingSymbolMatcher()))));
    }

    @Test
    public void testFilterAboveRowNumber()
    {
        // remove subplan if predicate on row number symbol can't be satisfied
        assertPlan(
                "SELECT * FROM (SELECT name, row_number() OVER() FROM nation) t(name, row_number) WHERE row_number < 0",
                output(
                        ImmutableList.of("name", "row_number"),
                        values("name", "row_number")));

        // include limit into RowNUmberNode on the basis of predicate; remove filter because predicate is satisfied
        assertPlan(
                "SELECT * FROM (SELECT name, row_number() OVER() FROM nation) t(name, row_number) WHERE row_number < 2",
                output(
                        ImmutableList.of("name", "row_number"),
                        rowNumber(
                                pattern -> pattern
                                        .maxRowCountPerPartition(Optional.of(1)),
                                any(
                                        tableScan("nation", ImmutableMap.of("name", "name"))))
                                .withAlias("row_number", new RowNumberSymbolMatcher())));

        // include limit into RowNUmberNode on the basis of predicate; remove filter because predicate is satisfied
        assertPlan(
                "SELECT * FROM (SELECT name, row_number() OVER() FROM nation) t(name, row_number) WHERE row_number <= 1",
                output(
                        ImmutableList.of("name", "row_number"),
                        rowNumber(
                                pattern -> pattern
                                        .maxRowCountPerPartition(Optional.of(1)),
                                any(
                                        tableScan("nation", ImmutableMap.of("name", "name"))))
                                .withAlias("row_number", new RowNumberSymbolMatcher())));

        // include limit into RowNUmberNode on the basis of predicate; remove filter because predicate is satisfied
        assertPlan(
                "SELECT * FROM (SELECT name, row_number() OVER() FROM nation) t(name, row_number) WHERE row_number <= 1 AND row_number > -10",
                output(
                        ImmutableList.of("name", "row_number"),
                        rowNumber(
                                pattern -> pattern
                                        .maxRowCountPerPartition(Optional.of(1)),
                                any(
                                        tableScan("nation", ImmutableMap.of("name", "name"))))
                                .withAlias("row_number", new RowNumberSymbolMatcher())));

        // include limit into RowNUmberNode on the basis of predicate; cannot remove filter because predicate is not satisfied
        assertPlan(
                "SELECT * FROM (SELECT name, row_number() OVER() FROM nation) t(name, row_number) WHERE row_number > 1 AND row_number < 3",
                output(
                        ImmutableList.of("name", "row_number"),
                        filter(
                                "(row_number > BIGINT '1') AND (row_number < BIGINT '3')",
                                rowNumber(
                                        pattern -> pattern
                                                .maxRowCountPerPartition(Optional.of(2)),
                                        any(
                                                tableScan("nation", ImmutableMap.of("name", "name"))))
                                        .withAlias("row_number", new RowNumberSymbolMatcher()))));
    }

    private Session optimizeTopNRanking(boolean enabled)
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZE_TOP_N_RANKING, Boolean.toString(enabled))
                .build();
    }
}
