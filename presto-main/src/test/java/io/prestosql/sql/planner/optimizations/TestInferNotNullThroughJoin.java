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
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import org.testng.annotations.Test;

import static io.prestosql.SystemSessionProperties.OPTIMIZED_NULLS_IN_JOIN;
import static io.prestosql.SystemSessionProperties.OPTIMIZED_NULLS_IN_JOIN_THRESHOLD;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;

public class TestInferNotNullThroughJoin
        extends BasePlanTest
{
    private static final PlanMatchPattern ORDERS_TABLESCAN = tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"));
    private static final PlanMatchPattern LINEITEM_TABLESCAN = tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey"));

    public TestInferNotNullThroughJoin()
    {
        super(ImmutableMap.of(OPTIMIZED_NULLS_IN_JOIN, "true", OPTIMIZED_NULLS_IN_JOIN_THRESHOLD, "0"));
    }

    @Test
    public void testInnerJoinWithOptimize()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(join(INNER, ImmutableList.of(equiJoinClause("O_ORDERKEY", "L_ORDERKEY")),
                        anyTree(filter("NOT (O_ORDERKEY IS NULL)", ORDERS_TABLESCAN)),
                        anyTree(filter("NOT (L_ORDERKEY IS NULL)", LINEITEM_TABLESCAN)))));
    }

    @Test
    public void testLeftJoinWithOptimize()
    {
        assertPlan("SELECT o.orderkey FROM orders o LEFT JOIN lineitem l on l.orderkey = o.orderkey",
                anyTree(join(LEFT, ImmutableList.of(equiJoinClause("O_ORDERKEY", "L_ORDERKEY")),
                        // NO filter here
                        project(ORDERS_TABLESCAN),
                        anyTree(filter("NOT (L_ORDERKEY IS NULL)", LINEITEM_TABLESCAN)))));
    }

    @Test
    public void testRightJoinWithOptimize()
    {
        assertPlan("SELECT o.orderkey FROM orders o RIGHT JOIN lineitem l on l.orderkey = o.orderkey",
                anyTree(join(RIGHT, ImmutableList.of(equiJoinClause("O_ORDERKEY", "L_ORDERKEY")),
                        anyTree(filter("NOT (O_ORDERKEY IS NULL)", ORDERS_TABLESCAN)),
                        // NO filter here
                        anyTree(project(LINEITEM_TABLESCAN)))));
    }

    @Test
    public void testFullJoinWithOptimize()
    {
        assertPlan("SELECT o.orderkey FROM orders o FULL JOIN lineitem l on l.orderkey = o.orderkey",
                anyTree(join(FULL, ImmutableList.of(equiJoinClause("O_ORDERKEY", "L_ORDERKEY")),
                        project(ORDERS_TABLESCAN),
                        anyTree(project(LINEITEM_TABLESCAN)))));
    }
}
