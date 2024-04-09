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
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
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
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction MULTIPLY_INTEGER = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));

    @Test
    public void testDoesNotFireNoExchange()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x", INTEGER), new Constant(INTEGER, 3L)),
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
                    Symbol x = p.symbol("x", INTEGER);
                    return p.project(
                            Assignments.of(
                                    x, new Constant(INTEGER, 3L),
                                    c2, new Reference(BIGINT, "c")),
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
                                        .withAlias("x1", expression(new Constant(INTEGER, 3L))),
                                project(
                                        values(ImmutableList.of("b")))
                                        .withAlias("x2", expression(new Constant(INTEGER, 3L))))
                                // verify that data originally on symbols aliased as x1 and x2 is part of exchange output
                                .withAlias("x1")
                                .withAlias("x2"));
    }

    @Test
    public void testHashMapping()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a", INTEGER);
                    Symbol h1 = p.symbol("h_1");
                    Symbol c = p.symbol("c", INTEGER);
                    Symbol h = p.symbol("h");
                    Symbol cTimes5 = p.symbol("c_times_5", INTEGER);
                    return p.project(
                            Assignments.of(
                                    cTimes5, new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "c"), new Constant(INTEGER, 5L)))),
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
                                                ImmutableMap.of(
                                                        "a", expression(new Reference(INTEGER, "a")),
                                                        "h_1", expression(new Reference(BIGINT, "h_1")),
                                                        "a_times_5", expression(new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 5L))))),
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
                    Symbol a = p.symbol("a", INTEGER);
                    Symbol aTimes5 = p.symbol("a_times_5", INTEGER);
                    return p.project(
                            Assignments.of(
                                    aTimes5, new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 5L))),
                                    a, a.toSymbolReference()),
                            p.exchange(e -> e
                                    .addSource(p.values(a))
                                    .addInputsSet(a)
                                    .fixedHashDistributionPartitioningScheme(ImmutableList.of(a), ImmutableList.of(a))));
                })
                .matches(
                        exchange(
                                strictProject(
                                        ImmutableMap.of("a_0", expression(new Reference(INTEGER, "a")), "a_times_5", expression(new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 5L))))),
                                        values(ImmutableList.of("a")))));

        // In the following example, the Projection over Exchange has got an identity assignment (b -> b).
        // The Projection is pushed down to Exchange's source, and the identity assignment is translated into
        // a0 -> a. The assignment is added to the pushed-down Projection because the input symbol 'a' is
        // required by the Exchange as a partitioning symbol.
        // When all the assignments from the parent Projection are added to the pushed-down Projection,
        // this assignment is omitted. Otherwise the doubled assignment would cause an error.
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol bTimes5 = p.symbol("b_times_5", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.project(
                            Assignments.of(
                                    bTimes5, new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "b"), new Constant(BIGINT, 5L))),
                                    b, b.toSymbolReference()),
                            p.exchange(e -> e
                                    .addSource(p.values(a))
                                    .addInputsSet(a)
                                    .fixedHashDistributionPartitioningScheme(ImmutableList.of(b), ImmutableList.of(b))));
                })
                .matches(
                        exchange(
                                strictProject(
                                        ImmutableMap.of("a_0", expression(new Reference(BIGINT, "a")), "a_times_5", expression(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Constant(BIGINT, 5L))))),
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
                    Symbol a = p.symbol("a", INTEGER);
                    Symbol aTimes5 = p.symbol("a_times_5", INTEGER);
                    return p.project(
                            Assignments.of(
                                    aTimes5, new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 5L))),
                                    a, a.toSymbolReference()),
                            p.exchange(e -> e
                                    .addSource(p.values(a))
                                    .addInputsSet(a)
                                    .singleDistributionPartitioningScheme(a)));
                })
                .matches(
                        exchange(
                                strictProject(
                                        ImmutableMap.of("a_0", expression(new Reference(INTEGER, "a")), "a_times_5", expression(new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 5L))))),
                                        values(ImmutableList.of("a")))));

        // In the following example, the Projection over Exchange has got an identity assignment (b -> b).
        // The Projection is pushed down to Exchange's source, and the identity assignment is translated into
        // a0 -> a. Input symbol 'a' is not used in the Exchange for partitioning, ordering or as a hash symbol.
        // It is just passed to output.
        // When all the assignments from the parent Projection are added to the pushed-down Projection,
        // the translated assignment is added too, so that the input symbol 'a' can be passed to the Exchange's output.
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol bTimes5 = p.symbol("b_times_5", BIGINT);
                    Symbol b = p.symbol("b", BIGINT);
                    return p.project(
                            Assignments.of(
                                    bTimes5, new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "b"), new Constant(BIGINT, 5L))),
                                    b, b.toSymbolReference()),
                            p.exchange(e -> e
                                    .addSource(p.values(a))
                                    .addInputsSet(a)
                                    .singleDistributionPartitioningScheme(b)));
                })
                .matches(
                        exchange(
                                strictProject(
                                        ImmutableMap.of(
                                                "a_0", expression(new Reference(BIGINT, "a")),
                                                "a_times_5", expression(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Constant(BIGINT, 5L))))),
                                        values(ImmutableList.of("a")))));
    }

    @Test
    public void testPartitioningColumnAndHashWithoutIdentityMappingInProjection()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a", INTEGER);
                    Symbol b = p.symbol("b", INTEGER);
                    Symbol h = p.symbol("h", INTEGER);
                    Symbol aTimes5 = p.symbol("a_times_5", INTEGER);
                    Symbol bTimes5 = p.symbol("b_times_5", INTEGER);
                    Symbol hTimes5 = p.symbol("h_times_5", INTEGER);
                    return p.project(
                            Assignments.builder()
                                    .put(aTimes5, new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 5L))))
                                    .put(bTimes5, new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 5L))))
                                    .put(hTimes5, new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "h"), new Constant(INTEGER, 5L))))
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
                                                .withAlias("b", expression(new Reference(INTEGER, "b")))
                                                .withAlias("h", expression(new Reference(INTEGER, "h")))
                                                .withAlias("a_times_5", expression(new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 5L)))))
                                                .withAlias("b_times_5", expression(new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 5L)))))
                                                .withAlias("h_times_5", expression(new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "h"), new Constant(INTEGER, 5L))))))
                        ).withNumberOfOutputColumns(3)
                                .withExactOutputs("a_times_5", "b_times_5", "h_times_5"));
    }

    @Test
    public void testOrderingColumnsArePreserved()
    {
        tester().assertThat(new PushProjectionThroughExchange())
                .on(p -> {
                    Symbol a = p.symbol("a", INTEGER);
                    Symbol b = p.symbol("b", INTEGER);
                    Symbol h = p.symbol("h", INTEGER);
                    Symbol aTimes5 = p.symbol("a_times_5", INTEGER);
                    Symbol bTimes5 = p.symbol("b_times_5", INTEGER);
                    Symbol hTimes5 = p.symbol("h_times_5", INTEGER);
                    Symbol sortSymbol = p.symbol("sortSymbol");
                    OrderingScheme orderingScheme = new OrderingScheme(ImmutableList.of(sortSymbol), ImmutableMap.of(sortSymbol, SortOrder.ASC_NULLS_FIRST));
                    return p.project(
                            Assignments.builder()
                                    .put(aTimes5, new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 5L))))
                                    .put(bTimes5, new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 5L))))
                                    .put(hTimes5, new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "h"), new Constant(INTEGER, 5L))))
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
                                                .withAlias("a_times_5", expression(new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "a"), new Constant(INTEGER, 5L)))))
                                                .withAlias("b_times_5", expression(new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 5L)))))
                                                .withAlias("h_times_5", expression(new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "h"), new Constant(INTEGER, 5L)))))
                                                .withAlias("sortSymbol", expression(new Reference(INTEGER, "sortSymbol"))))
                        ).withNumberOfOutputColumns(3)
                                .withExactOutputs("a_times_5", "b_times_5", "h_times_5"));
    }
}
