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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.cost.PlanNodeStatsEstimate.unknown;
import static io.trino.cost.StatsAndCosts.empty;
import static io.trino.metadata.AbstractMockMetadata.dummyMetadata;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.sql.planner.assertions.PlanAssert.assertPlan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static io.trino.sql.planner.iterative.rule.PushProjectionThroughJoin.pushProjectionThroughJoin;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.sql.tree.ArithmeticUnaryExpression.Sign.PLUS;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public class TestPushProjectionThroughJoin
{
    @Test
    public void testPushesProjectionThroughJoin()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder p = new PlanBuilder(idAllocator, dummyMetadata(), TEST_SESSION);
        Symbol a0 = p.symbol("a0");
        Symbol a1 = p.symbol("a1");
        Symbol a2 = p.symbol("a2");
        Symbol a3 = p.symbol("a3");
        Symbol b0 = p.symbol("b0");
        Symbol b1 = p.symbol("b1");
        Symbol b2 = p.symbol("b2");

        ProjectNode planNode = p.project(
                Assignments.of(
                        a3, new ArithmeticUnaryExpression(MINUS, a2.toSymbolReference()),
                        b2, new ArithmeticUnaryExpression(PLUS, b1.toSymbolReference())),
                p.join(
                        INNER,
                        // intermediate non-identity projections should be fully inlined
                        p.project(
                                Assignments.of(
                                        a2, new ArithmeticUnaryExpression(PLUS, a0.toSymbolReference()),
                                        a1, a1.toSymbolReference()),
                                p.project(
                                        Assignments.builder()
                                                .putIdentity(a0)
                                                .putIdentity(a1)
                                                .build(),
                                        p.values(a0, a1))),
                        p.values(b0, b1),
                        new JoinNode.EquiJoinClause(a1, b1)));

        Session session = testSessionBuilder().build();
        Optional<PlanNode> rewritten = pushProjectionThroughJoin(PLANNER_CONTEXT, planNode, noLookup(), idAllocator, session, createTestingTypeAnalyzer(
                PLANNER_CONTEXT), p.getTypes());
        assertTrue(rewritten.isPresent());
        assertPlan(
                session,
                dummyMetadata(),
                createTestingFunctionManager(),
                node -> unknown(),
                new Plan(rewritten.get(), p.getTypes(), empty()), noLookup(),
                join(
                        INNER,
                        ImmutableList.of(aliases -> new JoinNode.EquiJoinClause(new Symbol("a1"), new Symbol("b1"))),
                        strictProject(ImmutableMap.of(
                                "a3", expression("-(+a0)"),
                                "a1", expression("a1")),
                                strictProject(ImmutableMap.of(
                                        "a0", expression("a0"),
                                        "a1", expression("a1")),
                                        PlanMatchPattern.values("a0", "a1"))),
                        strictProject(ImmutableMap.of(
                                "b2", expression("+b1"),
                                "b1", expression("b1")),
                                PlanMatchPattern.values("b0", "b1")))
                        .withExactOutputs("a3", "b2"));
    }

    @Test
    public void testDoesNotPushStraddlingProjection()
    {
        PlanBuilder p = new PlanBuilder(new PlanNodeIdAllocator(), dummyMetadata(), TEST_SESSION);
        Symbol a = p.symbol("a");
        Symbol b = p.symbol("b");
        Symbol c = p.symbol("c");

        ProjectNode planNode = p.project(
                Assignments.of(
                        c, new ArithmeticBinaryExpression(ADD, a.toSymbolReference(), b.toSymbolReference())),
                p.join(
                        INNER,
                        p.values(a),
                        p.values(b)));
        Optional<PlanNode> rewritten = pushProjectionThroughJoin(PLANNER_CONTEXT, planNode, noLookup(), new PlanNodeIdAllocator(), testSessionBuilder().build(), createTestingTypeAnalyzer(
                PLANNER_CONTEXT), p.getTypes());
        assertThat(rewritten).isEmpty();
    }

    @Test
    public void testDoesNotPushProjectionThroughOuterJoin()
    {
        PlanBuilder p = new PlanBuilder(new PlanNodeIdAllocator(), dummyMetadata(), TEST_SESSION);
        Symbol a = p.symbol("a");
        Symbol b = p.symbol("b");
        Symbol c = p.symbol("c");

        ProjectNode planNode = p.project(
                Assignments.of(
                        c, new ArithmeticUnaryExpression(MINUS, a.toSymbolReference())),
                p.join(
                        LEFT,
                        p.values(a),
                        p.values(b)));
        Optional<PlanNode> rewritten = pushProjectionThroughJoin(PLANNER_CONTEXT, planNode, noLookup(), new PlanNodeIdAllocator(), testSessionBuilder().build(), createTestingTypeAnalyzer(
                PLANNER_CONTEXT), p.getTypes());
        assertThat(rewritten).isEmpty();
    }
}
