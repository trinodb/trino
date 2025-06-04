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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.INNER;

public class TestQuantifiedComparison
        extends BasePlanTest
{
    @Test
    public void testQuantifiedComparisonEqualsAny()
    {
        String query = "SELECT orderkey, custkey FROM orders WHERE orderkey = ANY (VALUES ROW(CAST(5 as BIGINT)), ROW(CAST(3 as BIGINT)))";
        assertPlan(query, anyTree(
                join(INNER, builder -> builder
                        .equiCriteria("X", "Y")
                        .left(anyTree(tableScan("orders", ImmutableMap.of("X", "orderkey"))))
                        .right(anyTree(values(ImmutableMap.of("Y", 0)))))));
    }

    @Test
    public void testQuantifiedComparisonNotEqualsAll()
    {
        String query = "SELECT orderkey, custkey FROM orders WHERE orderkey <> ALL (VALUES ROW(CAST(5 as BIGINT)), ROW(CAST(3 as BIGINT)))";
        assertPlan(query, anyTree(
                filter(
                        not(getPlanTester().getPlannerContext().getMetadata(), new Reference(BOOLEAN, "S")),
                        semiJoin("X", "Y", "S",
                                tableScan("orders", ImmutableMap.of("X", "orderkey")),
                                values(ImmutableMap.of("Y", 0))))));
    }

    @Test
    public void testQuantifiedComparisonLessAll()
    {
        assertQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey < ALL (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))");
    }

    @Test
    public void testQuantifiedComparisonGreaterEqualAll()
    {
        assertQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey >= ALL (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))");
    }

    @Test
    public void testQuantifiedComparisonLessSome()
    {
        assertQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey < SOME (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))");
    }

    @Test
    public void testQuantifiedComparisonGreaterEqualAny()
    {
        assertQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey >= ANY (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))");
    }

    @Test
    public void testQuantifiedComparisonEqualAll()
    {
        assertQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey = ALL (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))");
    }

    @Test
    public void testQuantifiedComparisonNotEqualAny()
    {
        assertQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey <> SOME (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))");
    }

    private void assertQuantifiedComparison(String query)
    {
        assertPlan(query, anyTree(
                node(JoinNode.class,
                        tableScan("orders"),
                        node(AggregationNode.class,
                                node(ValuesNode.class)))));
    }
}
