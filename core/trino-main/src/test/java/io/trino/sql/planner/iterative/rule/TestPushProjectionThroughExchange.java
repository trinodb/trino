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
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestPushProjectionThroughExchange
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireNoExchange()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x"), new LongLiteral("3")),
                                p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireNarrowingProjection()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");

                    return p.project(
                            Assignments.builder()
                                    .putIdentity(a)
                                    .putIdentity(b)
                                    .build(),
                            p.exchange(e -> e
                                    .addSource(p.values(a, b, c))
                                    .addInputsSet(a, b, c)
                                    .singleDistributionPartitioningScheme(a, b, c)));
                })
                .doesNotFire();
    }

    @Test
    public void testSimpleMultipleInputs()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol c2 = p.symbol("c2");
                    Symbol x = p.symbol("x");
                    return p.project(
                            Assignments.of(
                                    x, new LongLiteral("3"),
                                    c2, new SymbolReference("c")),
                            p.exchange(e -> e
                                    .addSource(
                                            p.values(a))
                                    .addSource(
                                            p.values(b))
                                    .addInputsSet(a)
                                    .addInputsSet(b)
                                    .singleDistributionPartitioningScheme(c)));
                })
                .matches(
                        exchange(
                                project(
                                        values(ImmutableList.of("a")))
                                        .withAlias("x1", expression("3")),
                                project(
                                        values(ImmutableList.of("b")))
                                        .withAlias("x2", expression("3")))
                                // verify that data originally on symbols aliased as x1 and x2 is part of exchange output
                                .withAlias("x1")
                                .withAlias("x2"));
    }

    @Test
    public void testHashMapping()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol h1 = p.symbol("h_1");
                    Symbol c = p.symbol("c");
                    Symbol h = p.symbol("h");
                    Symbol cTimes5 = p.symbol("c_times_5");
                    return p.project(
                            Assignments.of(
                                    cTimes5, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, new SymbolReference("c"), new LongLiteral("5"))),
                            p.exchange(e -> e
                                    .addSource(
                                            p.values(a, h1))
                                    .addInputsSet(a, h1)
                                    .fixedHashDistributionPartitioningScheme(
                                            ImmutableList.of(c, h),
                                            ImmutableList.of(c),
                                            h)));
                })
                .matches(
                        project(
                                exchange(
                                        strictProject(
                                                ImmutableMap.of("a", expression("a"), "h_1", expression("h_1"), "a_times_5", expression("a * 5")),
                                                values(ImmutableList.of("a", "h_1"))))));
    }

    @Test
    public void testSkipIdentityProjectionIfOutputPresent()
    {
        // In the following example, the Projection over Exchange has got an identity assignment (a -> a).
        // The Projection is pushed down to Exchange's source, and the identity assignment is translated into
        // a0 -> a. The assignment is added to the pushed-down Projection because the input symbol 'a' is
        // required by the Exchange as a partitioning symbol.
        // When all the assignments from the parent Projection are added to the pushed-down Projection,
        // this assignment is omitted. Otherwise the doubled assignment would cause an error.
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol aTimes5 = p.symbol("a_times_5");
                    return p.project(
                            Assignments.of(
                                    aTimes5, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, new SymbolReference("a"), new LongLiteral("5")),
                                    a, a.toSymbolReference()),
                            p.exchange(e -> e
                                    .addSource(p.values(a))
                                    .addInputsSet(a)
                                    .fixedHashDistributionPartitioningScheme(ImmutableList.of(a), ImmutableList.of(a))));
                })
                .matches(
                        exchange(
                                strictProject(
                                        ImmutableMap.of("a_0", expression("a"), "a_times_5", expression("a * 5")),
                                        values(ImmutableList.of("a")))));

        // In the following example, the Projection over Exchange has got an identity assignment (b -> b).
        // The Projection is pushed down to Exchange's source, and the identity assignment is translated into
        // a0 -> a. The assignment is added to the pushed-down Projection because the input symbol 'a' is
        // required by the Exchange as a partitioning symbol.
        // When all the assignments from the parent Projection are added to the pushed-down Projection,
        // this assignment is omitted. Otherwise the doubled assignment would cause an error.
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol bTimes5 = p.symbol("b_times_5");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.of(
                                    bTimes5, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, new SymbolReference("b"), new LongLiteral("5")),
                                    b, b.toSymbolReference()),
                            p.exchange(e -> e
                                    .addSource(p.values(a))
                                    .addInputsSet(a)
                                    .fixedHashDistributionPartitioningScheme(ImmutableList.of(b), ImmutableList.of(b))));
                })
                .matches(
                        exchange(
                                strictProject(
                                        ImmutableMap.of("a_0", expression("a"), "a_times_5", expression("a * 5")),
                                        values(ImmutableList.of("a")))));
    }

    @Test
    public void testDoNotSkipIdentityProjectionIfOutputAbsent()
    {
        // In the following example, the Projection over Exchange has got an identity assignment (a -> a).
        // The Projection is pushed down to Exchange's source, and the identity assignment is translated into
        // a0 -> a. Input symbol 'a' is not used in the Exchange for partitioning, ordering or as a hash symbol.
        // It is just passed to output.
        // When all the assignments from the parent Projection are added to the pushed-down Projection,
        // the translated assignment is added too, so that the input symbol 'a' can be passed to the Exchange's output.
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol aTimes5 = p.symbol("a_times_5");
                    return p.project(
                            Assignments.of(
                                    aTimes5, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, new SymbolReference("a"), new LongLiteral("5")),
                                    a, a.toSymbolReference()),
                            p.exchange(e -> e
                                    .addSource(p.values(a))
                                    .addInputsSet(a)
                                    .singleDistributionPartitioningScheme(a)));
                })
                .matches(
                        exchange(
                                strictProject(
                                        ImmutableMap.of("a_0", expression("a"), "a_times_5", expression("a * 5")),
                                        values(ImmutableList.of("a")))));

        // In the following example, the Projection over Exchange has got an identity assignment (b -> b).
        // The Projection is pushed down to Exchange's source, and the identity assignment is translated into
        // a0 -> a. Input symbol 'a' is not used in the Exchange for partitioning, ordering or as a hash symbol.
        // It is just passed to output.
        // When all the assignments from the parent Projection are added to the pushed-down Projection,
        // the translated assignment is added too, so that the input symbol 'a' can be passed to the Exchange's output.
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol bTimes5 = p.symbol("b_times_5");
                    Symbol b = p.symbol("b");
                    return p.project(
                            Assignments.of(
                                    bTimes5, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, new SymbolReference("b"), new LongLiteral("5")),
                                    b, b.toSymbolReference()),
                            p.exchange(e -> e
                                    .addSource(p.values(a))
                                    .addInputsSet(a)
                                    .singleDistributionPartitioningScheme(b)));
                })
                .matches(
                        exchange(
                                strictProject(
                                        ImmutableMap.of("a_0", expression("a"), "a_times_5", expression("a * 5")),
                                        values(ImmutableList.of("a")))));
    }

    @Test
    public void testPartitioningColumnAndHashWithoutIdentityMappingInProjection()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol h = p.symbol("h");
                    Symbol aTimes5 = p.symbol("a_times_5");
                    Symbol bTimes5 = p.symbol("b_times_5");
                    Symbol hTimes5 = p.symbol("h_times_5");
                    return p.project(
                            Assignments.builder()
                                    .put(aTimes5, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, new SymbolReference("a"), new LongLiteral("5")))
                                    .put(bTimes5, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, new SymbolReference("b"), new LongLiteral("5")))
                                    .put(hTimes5, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, new SymbolReference("h"), new LongLiteral("5")))
                                    .build(),
                            p.exchange(e -> e
                                    .addSource(
                                            p.values(a, b, h))
                                    .addInputsSet(a, b, h)
                                    .fixedHashDistributionPartitioningScheme(
                                            ImmutableList.of(a, b, h),
                                            ImmutableList.of(b),
                                            h)));
                })
                .matches(
                        project(
                                exchange(
                                        project(
                                                values(
                                                        ImmutableList.of("a", "b", "h"))
                                        ).withNumberOfOutputColumns(5)
                                                .withAlias("b", expression("b"))
                                                .withAlias("h", expression("h"))
                                                .withAlias("a_times_5", expression("a * 5"))
                                                .withAlias("b_times_5", expression("b * 5"))
                                                .withAlias("h_times_5", expression("h * 5")))
                        ).withNumberOfOutputColumns(3)
                                .withExactOutputs("a_times_5", "b_times_5", "h_times_5"));
    }

    @Test
    public void testOrderingColumnsArePreserved()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol h = p.symbol("h");
                    Symbol aTimes5 = p.symbol("a_times_5");
                    Symbol bTimes5 = p.symbol("b_times_5");
                    Symbol hTimes5 = p.symbol("h_times_5");
                    Symbol sortSymbol = p.symbol("sortSymbol");
                    OrderingScheme orderingScheme = new OrderingScheme(ImmutableList.of(sortSymbol), ImmutableMap.of(sortSymbol, SortOrder.ASC_NULLS_FIRST));
                    return p.project(
                            Assignments.builder()
                                    .put(aTimes5, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, new SymbolReference("a"), new LongLiteral("5")))
                                    .put(bTimes5, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, new SymbolReference("b"), new LongLiteral("5")))
                                    .put(hTimes5, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, new SymbolReference("h"), new LongLiteral("5")))
                                    .build(),
                            p.exchange(e -> e
                                    .addSource(
                                            p.values(a, b, h, sortSymbol))
                                    .addInputsSet(a, b, h, sortSymbol)
                                    .singleDistributionPartitioningScheme(
                                            ImmutableList.of(a, b, h, sortSymbol))
                                    .orderingScheme(orderingScheme)));
                })
                .matches(
                        project(
                                exchange(REMOTE, GATHER, ImmutableList.of(sort("sortSymbol", ASCENDING, FIRST)),
                                        project(
                                                values(
                                                        ImmutableList.of("a", "b", "h", "sortSymbol")))
                                                .withNumberOfOutputColumns(4)
                                                .withAlias("a_times_5", expression("a * 5"))
                                                .withAlias("b_times_5", expression("b * 5"))
                                                .withAlias("h_times_5", expression("h * 5"))
                                                .withAlias("sortSymbol", expression("sortSymbol")))
                        ).withNumberOfOutputColumns(3)
                                .withExactOutputs("a_times_5", "b_times_5", "h_times_5"));
    }
}
