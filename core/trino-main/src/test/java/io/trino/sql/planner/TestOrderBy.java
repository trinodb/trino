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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.tree.SortItem.NullOrdering.LAST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestOrderBy
        extends BasePlanTest
{
    @Test
    public void testRedundantOrderByInSubquery()
    {
        assertPlan("SELECT * FROM (SELECT * FROM (VALUES 1, 2, 3) t(x) ORDER BY x)",
                output(
                        node(ValuesNode.class)));
    }

    @Test
    public void testRequiredOrderByInSubquery()
    {
        assertPlan("SELECT * FROM (SELECT * FROM (VALUES 1, 2, 3) t(x) ORDER BY x LIMIT 1)",
                output(
                        node(TopNNode.class,
                                anyTree(
                                        node(ValuesNode.class)))));
    }

    @Test
    public void testRedundantOrderByInScalarSubquery()
    {
        assertPlan("SELECT (SELECT * FROM (VALUES 1, 2, 3) t(x) ORDER BY x) FROM (VALUES 10)",
                output(
                        node(EnforceSingleRowNode.class,
                                node(ValuesNode.class))));
    }

    @Test
    public void testRequiredOrderByInScalarSubquery()
    {
        assertPlan("SELECT (SELECT * FROM (VALUES 1, 2, 3) t(x) ORDER BY x LIMIT 1) FROM (VALUES 10)",
                output(
                        anyTree(
                                node(TopNNode.class,
                                        node(ValuesNode.class)))));
    }

    @Test
    public void testRequiredOrderByInUnion()
    {
        assertPlan("VALUES 1 " +
                        "UNION ALL " +
                        "VALUES 2 " +
                        "ORDER BY 1 ",
                output(
                        anyTree(
                                node(SortNode.class,
                                        node(ExchangeNode.class,
                                                node(ValuesNode.class),
                                                node(ValuesNode.class))))));
    }

    @Test
    public void testRedundantOrderByInUnion()
    {
        assertPlan("SELECT * FROM (" +
                        "   VALUES 1 " +
                        "   UNION ALL " +
                        "   VALUES 2 " +
                        "   ORDER BY 1 " +
                        ")",
                output(
                        node(ExchangeNode.class,
                                node(ValuesNode.class),
                                node(ValuesNode.class))));
    }

    @Test
    public void testRedundantOrderByInWith()
    {
        assertPlan("""
                        WITH t(a) AS (
                            SELECT * FROM (VALUES 2, 1) t(a)
                            ORDER BY a)
                        SELECT * FROM t
                        """,
                output(node(ValuesNode.class)));
    }

    @Test
    public void testOrderByInWithLimit()
    {
        assertPlan("""
                        WITH t(a) AS (
                            SELECT * FROM (VALUES 2, 1) t(a)
                            ORDER BY a
                            LIMIT 1)
                        SELECT * FROM t
                        """,
                output(
                        topN(1, ImmutableList.of(sort("c", ASCENDING, LAST)), TopNNode.Step.FINAL,
                                topN(1, ImmutableList.of(sort("c", ASCENDING, LAST)), TopNNode.Step.PARTIAL,
                                        values("c")))));
    }

    @Test
    public void testOrderByInWithOffset()
    {
        assertPlan("""
                        WITH t(a) AS (
                            SELECT * FROM (VALUES (2),(1)) t(a)
                            ORDER BY a
                            OFFSET 1)
                        SELECT * FROM t
                        """,
                output(
                        node(ProjectNode.class,
                                node(FilterNode.class,
                                        node(RowNumberNode.class,
                                                exchange(LOCAL,
                                                        sort(exchange(LOCAL,
                                                                values("c")))))))));
    }
}
