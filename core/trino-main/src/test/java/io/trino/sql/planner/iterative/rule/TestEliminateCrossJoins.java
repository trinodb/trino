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
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.optimizations.joins.JoinGraph;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static io.trino.sql.planner.iterative.rule.EliminateCrossJoins.getJoinOrder;
import static io.trino.sql.planner.iterative.rule.EliminateCrossJoins.isOriginalOrder;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestEliminateCrossJoins
        extends BaseRuleTest
{
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    @Test
    public void testEliminateCrossJoin()
    {
        tester().assertThat(new EliminateCrossJoins(tester().getMetadata(), tester().getTypeAnalyzer()))
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "ELIMINATE_CROSS_JOINS")
                .on(crossJoinAndJoin(INNER))
                .matches(
                        join(INNER,
                                ImmutableList.of(aliases -> new EquiJoinClause(new Symbol("cySymbol"), new Symbol("bySymbol"))),
                                join(INNER,
                                        ImmutableList.of(aliases -> new EquiJoinClause(new Symbol("axSymbol"), new Symbol("cxSymbol"))),
                                        any(),
                                        any()),
                                any()));
    }

    @Test
    public void testRetainOutgoingGroupReferences()
    {
        tester().assertThat(new EliminateCrossJoins(tester().getMetadata(), tester().getTypeAnalyzer()))
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "ELIMINATE_CROSS_JOINS")
                .on(crossJoinAndJoin(INNER))
                .matches(
                        node(JoinNode.class,
                                node(JoinNode.class,
                                        node(GroupReference.class),
                                        node(GroupReference.class)),
                                node(GroupReference.class)));
    }

    @Test
    public void testDoNotReorderOuterJoin()
    {
        tester().assertThat(new EliminateCrossJoins(tester().getMetadata(), tester().getTypeAnalyzer()))
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "ELIMINATE_CROSS_JOINS")
                .on(crossJoinAndJoin(JoinNode.Type.LEFT))
                .doesNotFire();
    }

    @Test
    public void testIsOriginalOrder()
    {
        assertTrue(isOriginalOrder(ImmutableList.of(0, 1, 2, 3, 4)));
        assertFalse(isOriginalOrder(ImmutableList.of(0, 2, 1, 3, 4)));
    }

    @Test
    public void testJoinOrder()
    {
        Session session = testSessionBuilder().build();

        PlanNode plan =
                joinNode(
                        joinNode(
                                values("a"),
                                values("b")),
                        values("c"),
                        "a", "c",
                        "b", "c");

        JoinGraph joinGraph = JoinGraph.buildFrom(tester().getMetadata(), plan, noLookup(), new PlanNodeIdAllocator(), session, new TypeAnalyzer(new SqlParser(), tester().getMetadata()), TypeProvider.empty());

        assertEquals(
                getJoinOrder(joinGraph),
                ImmutableList.of(0, 2, 1));
    }

    @Test
    public void testJoinOrderWithRealCrossJoin()
    {
        Session session = testSessionBuilder().build();

        PlanNode leftPlan =
                joinNode(
                        joinNode(
                                values("a"),
                                values("b")),
                        values("c"),
                        "a", "c",
                        "b", "c");

        PlanNode rightPlan =
                joinNode(
                        joinNode(
                                values("x"),
                                values("y")),
                        values("z"),
                        "x", "z",
                        "y", "z");

        PlanNode plan = joinNode(leftPlan, rightPlan);

        JoinGraph joinGraph = JoinGraph.buildFrom(tester().getMetadata(), plan, noLookup(), new PlanNodeIdAllocator(), session, new TypeAnalyzer(new SqlParser(), tester().getMetadata()), TypeProvider.empty());

        assertEquals(
                getJoinOrder(joinGraph),
                ImmutableList.of(0, 2, 1, 3, 5, 4));
    }

    @Test
    public void testJoinOrderWithMultipleEdgesBetweenNodes()
    {
        Session session = testSessionBuilder().build();

        PlanNode plan =
                joinNode(
                        joinNode(
                                values("a"),
                                values("b1", "b2")),
                        values("c1", "c2"),
                        "a", "c1",
                        "b1", "c1",
                        "b2", "c2");

        JoinGraph joinGraph = JoinGraph.buildFrom(tester().getMetadata(), plan, noLookup(), new PlanNodeIdAllocator(), session, new TypeAnalyzer(new SqlParser(), tester().getMetadata()), TypeProvider.empty());

        assertEquals(
                getJoinOrder(joinGraph),
                ImmutableList.of(0, 2, 1));
    }

    @Test
    public void testDoesNotChangeOrderWithoutCrossJoin()
    {
        Session session = testSessionBuilder().build();

        PlanNode plan =
                joinNode(
                        joinNode(
                                values("a"),
                                values("b"),
                                "a", "b"),
                        values("c"),
                        "b", "c");

        JoinGraph joinGraph = JoinGraph.buildFrom(tester().getMetadata(), plan, noLookup(), new PlanNodeIdAllocator(), session, new TypeAnalyzer(new SqlParser(), tester().getMetadata()), TypeProvider.empty());

        assertEquals(
                getJoinOrder(joinGraph),
                ImmutableList.of(0, 1, 2));
    }

    @Test
    public void testDoNotReorderCrossJoins()
    {
        Session session = testSessionBuilder().build();

        PlanNode plan =
                joinNode(
                        joinNode(
                                values("a"),
                                values("b")),
                        values("c"),
                        "b", "c");

        JoinGraph joinGraph = JoinGraph.buildFrom(tester().getMetadata(), plan, noLookup(), new PlanNodeIdAllocator(), session, new TypeAnalyzer(new SqlParser(), tester().getMetadata()), TypeProvider.empty());

        assertEquals(
                getJoinOrder(joinGraph),
                ImmutableList.of(0, 1, 2));
    }

    @Test
    public void testEliminateCrossJoinWithNonIdentityProjections()
    {
        tester().assertThat(new EliminateCrossJoins(tester().getMetadata(), tester().getTypeAnalyzer()))
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "ELIMINATE_CROSS_JOINS")
                .on(p -> {
                    Symbol a1 = p.symbol("a1");
                    Symbol a2 = p.symbol("a2");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    Symbol e = p.symbol("e");
                    Symbol f = p.symbol("f");

                    return p.join(
                            INNER,
                            p.project(
                                    Assignments.of(
                                            a2, new ArithmeticUnaryExpression(MINUS, new SymbolReference("a1")),
                                            f, new SymbolReference("f")),
                                    p.join(
                                            INNER,
                                            p.project(
                                                    Assignments.of(
                                                            a1, new SymbolReference("a1"),
                                                            f, new ArithmeticUnaryExpression(MINUS, new SymbolReference("b"))),
                                                    p.join(
                                                            INNER,
                                                            p.values(a1),
                                                            p.values(b))),
                                            p.values(e),
                                            new EquiJoinClause(a1, e))),
                            p.values(c, d),
                            new EquiJoinClause(a2, c),
                            new EquiJoinClause(f, d));
                })
                .matches(
                        node(ProjectNode.class,
                                join(
                                        INNER,
                                        ImmutableList.of(aliases -> new EquiJoinClause(new Symbol("d"), new Symbol("f"))),
                                        join(
                                                INNER,
                                                ImmutableList.of(aliases -> new EquiJoinClause(new Symbol("a2"), new Symbol("c"))),
                                                join(INNER,
                                                        ImmutableList.of(aliases -> new EquiJoinClause(new Symbol("a1"), new Symbol("e"))),
                                                        strictProject(
                                                                ImmutableMap.of(
                                                                        "a2", expression("-a1"),
                                                                        "a1", expression("a1")),
                                                                PlanMatchPattern.values("a1")),
                                                        strictProject(
                                                                ImmutableMap.of(
                                                                        "e", expression("e")),
                                                                PlanMatchPattern.values("e"))),
                                                any()),
                                        strictProject(
                                                ImmutableMap.of("f", expression("-b")),
                                                PlanMatchPattern.values("b")))));
    }

    @Test
    public void testGiveUpOnComplexProjections()
    {
        Session session = testSessionBuilder().build();

        PlanNode plan =
                joinNode(
                        projectNode(
                                joinNode(
                                        values("a1"),
                                        values("b")),
                                "a2",
                                new ArithmeticBinaryExpression(ADD, new SymbolReference("a1"), new SymbolReference("b")),
                                "b",
                                new SymbolReference("b")),
                        values("c"),
                        "a2", "c",
                        "b", "c");

        assertEquals(JoinGraph.buildFrom(tester().getMetadata(), plan, noLookup(), new PlanNodeIdAllocator(), session, new TypeAnalyzer(new SqlParser(), tester().getMetadata()), TypeProvider.empty()).size(), 2);
    }

    private Function<PlanBuilder, PlanNode> crossJoinAndJoin(JoinNode.Type secondJoinType)
    {
        return p -> {
            Symbol axSymbol = p.symbol("axSymbol");
            Symbol bySymbol = p.symbol("bySymbol");
            Symbol cxSymbol = p.symbol("cxSymbol");
            Symbol cySymbol = p.symbol("cySymbol");

            // (a inner join b) inner join c on c.x = a.x and c.y = b.y
            return p.join(INNER,
                    p.join(secondJoinType,
                            p.values(axSymbol),
                            p.values(bySymbol)),
                    p.values(cxSymbol, cySymbol),
                    new EquiJoinClause(axSymbol, cxSymbol),
                    new EquiJoinClause(bySymbol, cySymbol));
        };
    }

    private PlanNode projectNode(PlanNode source, String symbol1, Expression expression1, String symbol2, Expression expression2)
    {
        return new ProjectNode(
                idAllocator.getNextId(),
                source,
                Assignments.of(
                        new Symbol(symbol1), expression1,
                        new Symbol(symbol2), expression2));
    }

    private JoinNode joinNode(PlanNode left, PlanNode right, String... symbols)
    {
        checkArgument(symbols.length % 2 == 0);
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteria = ImmutableList.builder();

        for (int i = 0; i < symbols.length; i += 2) {
            criteria.add(new JoinNode.EquiJoinClause(new Symbol(symbols[i]), new Symbol(symbols[i + 1])));
        }

        return new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.INNER,
                left,
                right,
                criteria.build(),
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }

    private ValuesNode values(String... symbols)
    {
        return new ValuesNode(
                idAllocator.getNextId(),
                Arrays.stream(symbols).map(Symbol::new).collect(toImmutableList()),
                ImmutableList.of());
    }
}
