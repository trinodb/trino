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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.ArithmeticUnaryExpression;
import org.testng.annotations.Test;

import static io.prestosql.cost.PlanNodeStatsEstimate.unknown;
import static io.prestosql.cost.StatsAndCosts.empty;
import static io.prestosql.metadata.AbstractMockMetadata.dummyMetadata;
import static io.prestosql.sql.planner.assertions.PlanAssert.assertPlan;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.iterative.Lookup.noLookup;
import static io.prestosql.sql.planner.iterative.rule.PushProjectionThroughJoin.pushProjectionThroughJoin;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static io.prestosql.sql.tree.ArithmeticUnaryExpression.Sign.PLUS;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertNotEquals;

public class TestPushProjectionThroughJoin
{
    @Test
    public void testPushesProjectionThroughJoin()
    {
        PlanBuilder p = new PlanBuilder(new PlanNodeIdAllocator(), dummyMetadata());
        Symbol a1 = p.symbol("a1");
        Symbol a2 = p.symbol("a2");
        Symbol b0 = p.symbol("b0");
        Symbol b1 = p.symbol("b1");
        Symbol b2 = p.symbol("b2");

        ProjectNode planNode = p.project(
                Assignments.of(
                        a2, new ArithmeticUnaryExpression(MINUS, a1.toSymbolReference()),
                        b2, new ArithmeticUnaryExpression(PLUS, b1.toSymbolReference())),
                p.join(
                        INNER,
                        p.values(a1),
                        p.values(b0, b1),
                        new JoinNode.EquiJoinClause(a1, b1)));

        PlanNode rewritten = pushProjectionThroughJoin(planNode, noLookup(), new PlanNodeIdAllocator());
        assertNotEquals(planNode, rewritten);
        assertPlan(
                testSessionBuilder().build(),
                dummyMetadata(),
                node -> unknown(),
                new Plan(rewritten, p.getTypes(), empty()), noLookup(),
                join(
                        INNER,
                        ImmutableList.of(aliases -> new JoinNode.EquiJoinClause(new Symbol("a1"), new Symbol("b1"))),
                        strictProject(
                                ImmutableMap.of(
                                        "a2", expression("-a1"),
                                        "a1", expression("a1")),
                                PlanMatchPattern.values("a1")),
                        strictProject(
                                ImmutableMap.of(
                                        "b2", expression("+b1"),
                                        "b1", expression("b1")),
                                PlanMatchPattern.values("b0", "b1")))
                        .withExactOutputs("a2", "b2"));
    }
}
