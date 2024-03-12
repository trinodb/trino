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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.union;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.Assignments.identity;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static java.util.function.Predicate.not;

public class TestOptimizeDuplicateInsensitiveJoins
        extends BaseRuleTest
{
    @Test
    public void testNoAggregation()
    {
        tester().assertThat(new OptimizeDuplicateInsensitiveJoins(tester().getMetadata()))
                .on(p -> p.join(
                        INNER,
                        p.values(p.symbol("a")),
                        p.values(p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testAggregation()
    {
        tester().assertThat(new OptimizeDuplicateInsensitiveJoins(tester().getMetadata()))
                .on(p -> {
                    Symbol symbolA = p.symbol("a");
                    Symbol symbolB = p.symbol("b");
                    Symbol output = p.symbol("out");
                    return p.aggregation(a -> a
                            .singleGroupingSet(symbolA)
                            .addAggregation(output, PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of())
                            .source(p.join(
                                    INNER,
                                    p.values(symbolA),
                                    p.values(symbolB))));
                })
                .doesNotFire();
    }

    @Test
    public void testEmptyAggregation()
    {
        tester().assertThat(new OptimizeDuplicateInsensitiveJoins(tester().getMetadata()))
                .on(p -> {
                    Symbol symbolA = p.symbol("a");
                    Symbol symbolB = p.symbol("b");
                    return p.aggregation(a -> a
                            .singleGroupingSet(symbolA)
                            .source(p.join(
                                    INNER,
                                    p.values(symbolA),
                                    p.values(symbolB))));
                })
                .matches(
                        aggregation(ImmutableMap.of(),
                                join(INNER, builder -> builder
                                        .left(values("A"))
                                        .right(values("B")))
                                        .with(JoinNode.class, JoinNode::isMaySkipOutputDuplicates)));
    }

    @Test
    public void testNestedJoins()
    {
        tester().assertThat(new OptimizeDuplicateInsensitiveJoins(tester().getMetadata()))
                .on(p -> {
                    Symbol symbolA = p.symbol("a");
                    Symbol symbolB = p.symbol("b");
                    Symbol symbolC = p.symbol("c");
                    return p.aggregation(a -> a
                            .singleGroupingSet(symbolA)
                            .source(p.join(
                                    INNER,
                                    p.values(symbolA),
                                    p.project(identity(symbolB),
                                            p.filter(
                                                    new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("10")),
                                                    p.join(
                                                            INNER,
                                                            p.values(symbolB),
                                                            p.values(symbolC)))))));
                })
                .matches(
                        aggregation(ImmutableMap.of(),
                                join(INNER, builder -> builder
                                        .left(values("A"))
                                        .right(project(
                                                filter(
                                                        new ComparisonExpression(GREATER_THAN, new SymbolReference("B"), new LongLiteral("10")),
                                                        join(INNER, rightJoinBuilder -> rightJoinBuilder
                                                                .left(values("B"))
                                                                .right(values("C")))
                                                                .with(JoinNode.class, JoinNode::isMaySkipOutputDuplicates)))))
                                        .with(JoinNode.class, JoinNode::isMaySkipOutputDuplicates)));
    }

    @Test
    public void testNondeterministicJoins()
    {
        FunctionCall randomFunction = new FunctionCall(
                tester().getMetadata().resolveBuiltinFunction("random", ImmutableList.of()).toQualifiedName(),
                ImmutableList.of());

        tester().assertThat(new OptimizeDuplicateInsensitiveJoins(tester().getMetadata()))
                .on(p -> {
                    Symbol symbolA = p.symbol("a");
                    Symbol symbolB = p.symbol("b");
                    Symbol symbolC = p.symbol("c");
                    return p.aggregation(a -> a
                            .singleGroupingSet(symbolA)
                            .source(p.join(
                                    INNER,
                                    p.values(symbolA),
                                    p.join(
                                            INNER,
                                            p.values(symbolB),
                                            p.values(symbolC)),
                                    new ComparisonExpression(GREATER_THAN, symbolB.toSymbolReference(), randomFunction))));
                })
                .matches(
                        aggregation(ImmutableMap.of(),
                                join(INNER, builder -> builder
                                        .filter(new ComparisonExpression(GREATER_THAN, new SymbolReference("B"), new FunctionCall(QualifiedName.of("random"), ImmutableList.of())))
                                        .left(values("A"))
                                        .right(
                                                join(INNER, rightJoinBuilder -> rightJoinBuilder
                                                        .left(values("B"))
                                                        .right(values("C")))
                                                        .with(JoinNode.class, not(JoinNode::isMaySkipOutputDuplicates))))
                                        .with(JoinNode.class, JoinNode::isMaySkipOutputDuplicates)));
    }

    @Test
    public void testNondeterministicFilter()
    {
        FunctionCall randomFunction = new FunctionCall(
                tester().getMetadata().resolveBuiltinFunction("random", ImmutableList.of()).toQualifiedName(),
                ImmutableList.of());

        tester().assertThat(new OptimizeDuplicateInsensitiveJoins(tester().getMetadata()))
                .on(p -> {
                    Symbol symbolA = p.symbol("a");
                    Symbol symbolB = p.symbol("b");
                    return p.aggregation(a -> a
                            .singleGroupingSet(symbolA)
                            .source(p.filter(new ComparisonExpression(GREATER_THAN, symbolB.toSymbolReference(), randomFunction),
                                    p.join(
                                            INNER,
                                            p.values(symbolA),
                                            p.values(symbolB)))));
                })
                .doesNotFire();
    }

    @Test
    public void testNondeterministicProjection()
    {
        FunctionCall randomFunction = new FunctionCall(
                tester().getMetadata().resolveBuiltinFunction("random", ImmutableList.of()).toQualifiedName(),
                ImmutableList.of());

        tester().assertThat(new OptimizeDuplicateInsensitiveJoins(tester().getMetadata()))
                .on(p -> {
                    Symbol symbolA = p.symbol("a");
                    Symbol symbolB = p.symbol("b");
                    Symbol symbolC = p.symbol("c");
                    return p.aggregation(a -> a
                            .singleGroupingSet(symbolA)
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(symbolA)
                                            .put(symbolC, randomFunction)
                                            .build(),
                                    p.join(
                                            INNER,
                                            p.values(symbolA),
                                            p.values(symbolB)))));
                })
                .doesNotFire();
    }

    @Test
    public void testUnion()
    {
        tester().assertThat(new OptimizeDuplicateInsensitiveJoins(tester().getMetadata()))
                .on(p -> {
                    Symbol symbolA = p.symbol("a");
                    Symbol symbolB = p.symbol("b");
                    Symbol symbolC = p.symbol("c");
                    Symbol symbolD = p.symbol("d");
                    Symbol symbolE = p.symbol("e");
                    return p.aggregation(a -> a
                            .singleGroupingSet(symbolE)
                            .source(p.union(
                                    ImmutableListMultimap.<Symbol, Symbol>builder()
                                            .put(symbolE, symbolA)
                                            .put(symbolE, symbolC)
                                            .build(),
                                    ImmutableList.of(
                                            p.join(
                                                    INNER,
                                                    p.values(symbolA),
                                                    p.values(symbolB)),
                                            p.join(
                                                    INNER,
                                                    p.values(symbolC),
                                                    p.values(symbolD))))));
                })
                .matches(
                        aggregation(ImmutableMap.of(), union(
                                join(INNER, builder -> builder
                                        .left(values("A"))
                                        .right(values("B")))
                                        .with(JoinNode.class, JoinNode::isMaySkipOutputDuplicates),
                                join(INNER, builder -> builder
                                        .left(values("C"))
                                        .right(values("D")))
                                        .with(JoinNode.class, JoinNode::isMaySkipOutputDuplicates))));
    }
}
