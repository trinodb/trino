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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.assertions.RowNumberSymbolMatcher;
import io.prestosql.sql.planner.assertions.TopNRowNumberSymbolMatcher;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.TopNRowNumberNode;
import io.prestosql.sql.planner.plan.WindowNode;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.SystemSessionProperties.OPTIMIZE_TOP_N_ROW_NUMBER;
import static io.prestosql.spi.block.SortOrder.ASC_NULLS_LAST;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.any;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyNot;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.limit;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.output;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.topNRowNumber;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;

public class TestWindowFilterPushDown
        extends BasePlanTest
{
    @Test
    public void testLimitAboveWindow()
    {
        @Language("SQL") String sql = "SELECT " +
                "row_number() OVER (PARTITION BY suppkey ORDER BY orderkey) partition_row_number FROM lineitem LIMIT 10";

        assertPlanWithSession(
                sql,
                optimizeTopNRowNumber(true),
                true,
                anyTree(
                        limit(10, anyTree(
                                node(TopNRowNumberNode.class,
                                        anyTree(
                                                tableScan("lineitem")))))));

        assertPlanWithSession(
                sql,
                optimizeTopNRowNumber(false),
                true,
                anyTree(
                        limit(10, anyTree(
                                node(WindowNode.class,
                                        anyTree(
                                                tableScan("lineitem")))))));
    }

    @Test
    public void testFilterAboveWindow()
    {
        @Language("SQL") String sql = "SELECT * FROM " +
                "(SELECT row_number() OVER (PARTITION BY suppkey ORDER BY orderkey) partition_row_number FROM lineitem) " +
                "WHERE partition_row_number < 10";

        assertPlanWithSession(
                sql,
                optimizeTopNRowNumber(true),
                true,
                anyTree(
                        anyNot(FilterNode.class,
                                node(TopNRowNumberNode.class,
                                        anyTree(
                                                tableScan("lineitem"))))));

        assertPlanWithSession(
                sql,
                optimizeTopNRowNumber(false),
                true,
                anyTree(
                        node(FilterNode.class,
                                anyTree(
                                        node(WindowNode.class,
                                                anyTree(
                                                        tableScan("lineitem")))))));

        // remove subplan if predicate on row number symbol can't be satisfied
        assertPlanWithSession(
                "SELECT * FROM (SELECT name, row_number() OVER(ORDER BY name) FROM nation) t(name, row_number) WHERE row_number < 0",
                optimizeTopNRowNumber(true),
                true,
                output(
                        ImmutableList.of("name", "row_number"),
                        values("name", "row_number")));

        // optimize to TopNRowNumber on the basis of predicate; remove filter because predicate is satisfied
        assertPlanWithSession(
                "SELECT * FROM (SELECT name, row_number() OVER(ORDER BY name) FROM nation) t(name, row_number) WHERE row_number < 2",
                optimizeTopNRowNumber(true),
                true,
                output(
                        ImmutableList.of("name", "row_number"),
                        topNRowNumber(
                                pattern -> pattern
                                        .maxRowCountPerPartition(1)
                                        .specification(ImmutableList.of(), ImmutableList.of("name"), ImmutableMap.of("name", ASC_NULLS_LAST)),
                                any(
                                        tableScan("nation", ImmutableMap.of("NAME", "name"))))
                                .withAlias("row_number", new TopNRowNumberSymbolMatcher())));

        // optimize to TopNRowNumber on the basis of predicate; remove filter because predicate is satisfied
        assertPlanWithSession(
                "SELECT * FROM (SELECT name, row_number() OVER(ORDER BY name) FROM nation) t(name, row_number) WHERE row_number <= 1",
                optimizeTopNRowNumber(true),
                true,
                output(
                        ImmutableList.of("name", "row_number"),
                        topNRowNumber(
                                pattern -> pattern
                                        .maxRowCountPerPartition(1)
                                        .specification(ImmutableList.of(), ImmutableList.of("name"), ImmutableMap.of("name", ASC_NULLS_LAST)),
                                any(
                                        tableScan("nation", ImmutableMap.of("NAME", "name"))))
                                .withAlias("row_number", new TopNRowNumberSymbolMatcher())));

        // optimize to TopNRowNumber on the basis of predicate; remove filter because predicate is satisfied
        assertPlanWithSession(
                "SELECT * FROM (SELECT name, row_number() OVER(ORDER BY name) FROM nation) t(name, row_number) WHERE row_number <= 1 AND row_number > -10",
                optimizeTopNRowNumber(true),
                true,
                output(
                        ImmutableList.of("name", "row_number"),
                        topNRowNumber(
                                pattern -> pattern
                                        .maxRowCountPerPartition(1)
                                        .specification(ImmutableList.of(), ImmutableList.of("name"), ImmutableMap.of("name", ASC_NULLS_LAST)),
                                any(
                                        tableScan("nation", ImmutableMap.of("NAME", "name"))))
                                .withAlias("row_number", new TopNRowNumberSymbolMatcher())));

        // optimize to TopNRowNumber on the basis of predicate; cannot remove filter because predicate is not satisfied
        assertPlanWithSession(
                "SELECT * FROM (SELECT name, row_number() OVER(ORDER BY name) FROM nation) t(name, row_number) WHERE row_number > 1 AND row_number < 3",
                optimizeTopNRowNumber(true),
                true,
                output(
                        ImmutableList.of("name", "row_number"),
                        filter(
                                "(row_number > BIGINT '1') AND (row_number < BIGINT '3')",
                                topNRowNumber(
                                        pattern -> pattern
                                                .maxRowCountPerPartition(2)
                                                .specification(ImmutableList.of(), ImmutableList.of("name"), ImmutableMap.of("name", ASC_NULLS_LAST)),
                                        any(
                                                tableScan("nation", ImmutableMap.of("NAME", "name"))))
                                        .withAlias("row_number", new TopNRowNumberSymbolMatcher()))));
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
                                        tableScan("nation", ImmutableMap.of("NAME", "name"))))
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
                                        tableScan("nation", ImmutableMap.of("NAME", "name"))))
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
                                        tableScan("nation", ImmutableMap.of("NAME", "name"))))
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
                                                tableScan("nation", ImmutableMap.of("NAME", "name"))))
                                        .withAlias("row_number", new RowNumberSymbolMatcher()))));
    }

    private Session optimizeTopNRowNumber(boolean enabled)
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZE_TOP_N_ROW_NUMBER, Boolean.toString(enabled))
                .build();
    }
}
