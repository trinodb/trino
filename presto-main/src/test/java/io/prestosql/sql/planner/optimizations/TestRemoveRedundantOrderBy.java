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

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.sort;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.topN;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.tree.SortItem.NullOrdering.LAST;
import static io.prestosql.sql.tree.SortItem.Ordering.ASCENDING;

public class TestRemoveRedundantOrderBy
        extends BasePlanTest
{
    private static final PlanMatchPattern ORDERS_TABLESCAN = tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"));
    private static final PlanMatchPattern LINEITEM_TABLESCAN = tableScan(
            "lineitem",
            ImmutableMap.of(
                    "L_PARTKEY", "partkey",
                    "L_ORDERKEY", "orderkey"));

    @Test
    public void testRemoveRedundantOrderBy()
    {
        String sql1 = "select * from orders a join (select * from lineitem order by orderkey) b on a.orderkey = b.orderkey";
        assertPlan(sql1,
                anyTree(join(INNER, ImmutableList.of(equiJoinClause("O_ORDERKEY", "L_ORDERKEY")),
                        project(tableScan("orders")),
                        exchange(project(tableScan("lineitem"))))));
    }

    @Test
    public void testOrderByWithLimitNotRemoved()
    {
        String sql1 = "select * from orders a join (select * from lineitem order by orderkey limit 10) b on a.orderkey = b.orderkey";
        assertPlan(sql1,
                anyTree(join(INNER, ImmutableList.of(equiJoinClause("O_ORDERKEY", "L_ORDERKEY")),
                        project(tableScan("orders")),
                        topN(10, ImmutableList.of(sort("l_orderkey", ASCENDING, LAST)), exchange(project(anyTree(LINEITEM_TABLESCAN)))))));
    }
}
