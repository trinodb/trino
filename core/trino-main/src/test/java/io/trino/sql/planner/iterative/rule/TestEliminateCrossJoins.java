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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.optimizations.joins.JoinGraph;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static io.trino.sql.planner.iterative.rule.EliminateCrossJoins.getJoinOrder;
import static io.trino.sql.planner.iterative.rule.EliminateCrossJoins.isOriginalOrder;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEliminateCrossJoins
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction NEGATION_BIGINT = FUNCTIONS.resolveOperator(OperatorType.NEGATION, ImmutableList.of(BIGINT));

    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    @Test
    public void testEliminateCrossJoin()
    {
        tester().assertThat(new EliminateCrossJoins())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "ELIMINATE_CROSS_JOINS")
                .on(crossJoinAndJoin(INNER))
                .matches(
                        join(INNER, builder -> builder
                                .equiCriteria(ImmutableList.of(aliases -> new EquiJoinClause(new Symbol(BIGINT, "cySymbol"), new Symbol(BIGINT, "bySymbol"))))
                                .left(
                                        join(INNER, leftJoinBuilder -> leftJoinBuilder
                                                .equiCriteria(ImmutableList.of(aliases -> new EquiJoinClause(new Symbol(BIGINT, "axSymbol"), new Symbol(BIGINT, "cxSymbol"))))
                                                .left(any())
                                                .right(any())))
                                .right(any())));
    }

    @Test
    public void testRetainOutgoingGroupReferences()
    {
        tester().assertThat(new EliminateCrossJoins())
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
        tester().assertThat(new EliminateCrossJoins())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "ELIMINATE_CROSS_JOINS")
                .on(crossJoinAndJoin(JoinType.LEFT))
                .doesNotFire();
    }

    @Test
    public void testIsOriginalOrder()
    {
        assertThat(isOriginalOrder(ImmutableList.of(0, 1, 2, 3, 4))).isTrue();
        assertThat(isOriginalOrder(ImmutableList.of(0, 2, 1, 3, 4))).isFalse();
    }

    @Test
    public void testJoinOrder()
    {
        PlanNode plan =
                joinNode(
                        joinNode(
                                values("a"),
                                values("b")),
                        values("c"),
                        "a", "c",
                        "b", "c");

        JoinGraph joinGraph = JoinGraph.buildFrom(plan, noLookup(), new PlanNodeIdAllocator());

        assertThat(getJoinOrder(joinGraph)).isEqualTo(ImmutableList.of(0, 2, 1));
    }

    @Test
    public void testJoinOrderWithRealCrossJoin()
    {
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

        JoinGraph joinGraph = JoinGraph.buildFrom(plan, noLookup(), new PlanNodeIdAllocator());

        assertThat(getJoinOrder(joinGraph)).isEqualTo(ImmutableList.of(0, 2, 1, 3, 5, 4));
    }

    @Test
    public void testJoinOrderWithMultipleEdgesBetweenNodes()
    {
        PlanNode plan =
                joinNode(
                        joinNode(
                                values("a"),
                                values("b1", "b2")),
                        values("c1", "c2"),
                        "a", "c1",
                        "b1", "c1",
                        "b2", "c2");

        JoinGraph joinGraph = JoinGraph.buildFrom(plan, noLookup(), new PlanNodeIdAllocator());

        assertThat(getJoinOrder(joinGraph)).isEqualTo(ImmutableList.of(0, 2, 1));
    }

    @Test
    public void testDoesNotChangeOrderWithoutCrossJoin()
    {
        PlanNode plan =
                joinNode(
                        joinNode(
                                values("a"),
                                values("b"),
                                "a", "b"),
                        values("c"),
                        "b", "c");

        JoinGraph joinGraph = JoinGraph.buildFrom(plan, noLookup(), new PlanNodeIdAllocator());

        assertThat(getJoinOrder(joinGraph)).isEqualTo(ImmutableList.of(0, 1, 2));
    }

    @Test
    public void testDoNotReorderCrossJoins()
    {
        PlanNode plan =
                joinNode(
                        joinNode(
                                values("a"),
                                values("b")),
                        values("c"),
                        "b", "c");

        JoinGraph joinGraph = JoinGraph.buildFrom(plan, noLookup(), new PlanNodeIdAllocator());

        assertThat(getJoinOrder(joinGraph)).isEqualTo(ImmutableList.of(0, 1, 2));
    }

    @Test
    public void testEliminateCrossJoinWithNonIdentityProjections()
    {
        tester().assertThat(new EliminateCrossJoins())
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
                                            a2, new Call(NEGATION_BIGINT, ImmutableList.of(new Reference(BIGINT, "a1"))),
                                            f, new Reference(BIGINT, "f")),
                                    p.join(
                                            INNER,
                                            p.project(
                                                    Assignments.of(
                                                            a1, new Reference(BIGINT, "a1"),
                                                            f, new Call(NEGATION_BIGINT, ImmutableList.of(new Reference(BIGINT, "b")))),
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
                                join(INNER, builder -> builder
                                        .equiCriteria(ImmutableList.of(aliases -> new EquiJoinClause(new Symbol(BIGINT, "d"), new Symbol(BIGINT, "f"))))
                                        .left(join(INNER, leftJoinBuilder -> leftJoinBuilder
                                                .equiCriteria(ImmutableList.of(aliases -> new EquiJoinClause(new Symbol(BIGINT, "a2"), new Symbol(BIGINT, "c"))))
                                                .left(
                                                        join(INNER, leftInnerJoinBuilder -> leftInnerJoinBuilder
                                                                .equiCriteria(ImmutableList.of(aliases -> new EquiJoinClause(new Symbol(BIGINT, "a1"), new Symbol(BIGINT, "e"))))
                                                                .left(
                                                                        strictProject(
                                                                                ImmutableMap.of(
                                                                                        "a2", expression(new Call(NEGATION_BIGINT, ImmutableList.of(new Reference(BIGINT, "a1")))),
                                                                                        "a1", expression(new Reference(BIGINT, "a1"))),
                                                                                PlanMatchPattern.values("a1")))
                                                                .right(
                                                                        strictProject(
                                                                                ImmutableMap.of(
                                                                                        "e", expression(new Reference(BIGINT, "e"))),
                                                                                PlanMatchPattern.values("e")))))
                                                .right(any())))
                                        .right(
                                                strictProject(
                                                        ImmutableMap.of("f", expression(new Call(NEGATION_BIGINT, ImmutableList.of(new Reference(BIGINT, "b"))))),
                                                        PlanMatchPattern.values("b"))))));
    }

    @Test
    public void testGiveUpOnComplexProjections()
    {
        PlanNode plan =
                joinNode(
                        projectNode(
                                joinNode(
                                        values("a1"),
                                        values("b")),
                                "a2",
                                new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "a1"), new Reference(BIGINT, "b"))),
                                "b",
                                new Reference(BIGINT, "b")),
                        values("c"),
                        "a2", "c",
                        "b", "c");

        assertThat(JoinGraph.buildFrom(plan, noLookup(), new PlanNodeIdAllocator()).size()).isEqualTo(2);
    }

    private Function<PlanBuilder, PlanNode> crossJoinAndJoin(JoinType secondJoinType)
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
                        new Symbol(BIGINT, symbol1), expression1,
                        new Symbol(BIGINT, symbol2), expression2));
    }

    private JoinNode joinNode(PlanNode left, PlanNode right, String... symbols)
    {
        checkArgument(symbols.length % 2 == 0);
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteria = ImmutableList.builder();

        for (int i = 0; i < symbols.length; i += 2) {
            criteria.add(new JoinNode.EquiJoinClause(new Symbol(BIGINT, symbols[i]), new Symbol(BIGINT, symbols[i + 1])));
        }

        return new JoinNode(
                idAllocator.getNextId(),
                JoinType.INNER,
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
                Arrays.stream(symbols).map(name -> new Symbol(BIGINT, name)).collect(toImmutableList()),
                ImmutableList.of());
    }
}
