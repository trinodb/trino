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
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.TopNRankingSymbolMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.TopNRankingNode.RankingType;
import io.trino.sql.planner.plan.WindowNode.Function;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topNRanking;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.RANK;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;

public class TestPushPredicateThroughProjectIntoWindow
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction MODULUS_INTEGER = FUNCTIONS.resolveOperator(OperatorType.MODULUS, ImmutableList.of(INTEGER, INTEGER));

    @Test
    public void testRankingSymbolPruned()
    {
        assertRankingSymbolPruned(rowNumberFunction());
        assertRankingSymbolPruned(rankFunction());
    }

    private void assertRankingSymbolPruned(Function rankingFunction)
    {
        tester().assertThat(new PushPredicateThroughProjectIntoWindow(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.filter(
                            new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)),
                            p.project(
                                    Assignments.identity(a),
                                    p.window(
                                            new DataOrganizationSpecification(
                                                    ImmutableList.of(),
                                                    Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))),
                                            ImmutableMap.of(ranking, rankingFunction),
                                            p.values(a))));
                })
                .doesNotFire();
    }

    @Test
    public void testNoUpperBoundForRankingSymbol()
    {
        assertNoUpperBoundForRankingSymbol(rowNumberFunction());
        assertNoUpperBoundForRankingSymbol(rankFunction());
    }

    private void assertNoUpperBoundForRankingSymbol(Function rankingFunction)
    {
        tester().assertThat(new PushPredicateThroughProjectIntoWindow(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.filter(
                            new Comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)),
                            p.project(
                                    Assignments.identity(a, ranking),
                                    p.window(
                                            new DataOrganizationSpecification(
                                                    ImmutableList.of(),
                                                    Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))),
                                            ImmutableMap.of(ranking, rankingFunction),
                                            p.values(a))));
                })
                .doesNotFire();
    }

    @Test
    public void testNonPositiveUpperBoundForRankingSymbol()
    {
        assertNonPositiveUpperBoundForRankingSymbol(rowNumberFunction());
        assertNonPositiveUpperBoundForRankingSymbol(rankFunction());
    }

    private void assertNonPositiveUpperBoundForRankingSymbol(Function rankingFunction)
    {
        tester().assertThat(new PushPredicateThroughProjectIntoWindow(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.filter(
                            new Logical(AND, ImmutableList.of(new Comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)), new Comparison(LESS_THAN, new Reference(BIGINT, "ranking"), new Constant(BIGINT, -10L)))),
                            p.project(
                                    Assignments.identity(a, ranking),
                                    p.window(
                                            new DataOrganizationSpecification(
                                                    ImmutableList.of(),
                                                    Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))),
                                            ImmutableMap.of(ranking, rankingFunction),
                                            p.values(a))));
                })
                .matches(values("a", "ranking"));
    }

    @Test
    public void testPredicateNotSatisfied()
    {
        assertPredicateNotSatisfied(rowNumberFunction(), ROW_NUMBER);
        assertPredicateNotSatisfied(rankFunction(), RANK);
    }

    private void assertPredicateNotSatisfied(Function rankingFunction, RankingType rankingType)
    {
        tester().assertThat(new PushPredicateThroughProjectIntoWindow(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.filter(
                            new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "ranking"), new Constant(BIGINT, 2L)), new Comparison(LESS_THAN, new Reference(BIGINT, "ranking"), new Constant(BIGINT, 5L)))),
                            p.project(
                                    Assignments.identity(ranking),
                                    p.window(
                                            new DataOrganizationSpecification(
                                                    ImmutableList.of(),
                                                    Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))),
                                            ImmutableMap.of(ranking, rankingFunction),
                                            p.values(a))));
                })
                .matches(filter(
                        new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "ranking"), new Constant(BIGINT, 2L)), new Comparison(LESS_THAN, new Reference(BIGINT, "ranking"), new Constant(BIGINT, 5L)))),
                        project(
                                ImmutableMap.of("ranking", expression(new Reference(BIGINT, "ranking"))),
                                topNRanking(
                                        pattern -> pattern
                                                .specification(
                                                        ImmutableList.of(),
                                                        ImmutableList.of("a"),
                                                        ImmutableMap.of("a", ASC_NULLS_FIRST))
                                                .rankingType(rankingType)
                                                .maxRankingPerPartition(4)
                                                .partial(false),
                                        values(ImmutableList.of("a")))
                                        .withAlias("ranking", new TopNRankingSymbolMatcher()))));
    }

    @Test
    public void testPredicateSatisfied()
    {
        assertPredicateSatisfied(rowNumberFunction(), RankingType.ROW_NUMBER);
        assertPredicateSatisfied(rankFunction(), RankingType.RANK);
    }

    private void assertPredicateSatisfied(Function rankingFunction, RankingType rankingType)
    {
        tester().assertThat(new PushPredicateThroughProjectIntoWindow(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.filter(
                            new Comparison(LESS_THAN, new Reference(BIGINT, "ranking"), new Constant(BIGINT, 5L)),
                            p.project(
                                    Assignments.identity(ranking),
                                    p.window(
                                            new DataOrganizationSpecification(
                                                    ImmutableList.of(),
                                                    Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))),
                                            ImmutableMap.of(ranking, rankingFunction),
                                            p.values(a))));
                })
                .matches(project(
                        ImmutableMap.of("ranking", expression(new Reference(BIGINT, "ranking"))),
                        topNRanking(
                                pattern -> pattern
                                        .specification(
                                                ImmutableList.of(),
                                                ImmutableList.of("a"),
                                                ImmutableMap.of("a", ASC_NULLS_FIRST))
                                        .rankingType(rankingType)
                                        .maxRankingPerPartition(4)
                                        .partial(false),
                                values(ImmutableList.of("a")))
                                .withAlias("ranking", new TopNRankingSymbolMatcher())));
    }

    @Test
    public void testPredicatePartiallySatisfied()
    {
        assertPredicatePartiallySatisfied(rowNumberFunction(), RankingType.ROW_NUMBER);
        assertPredicatePartiallySatisfied(rankFunction(), RankingType.RANK);
    }

    private void assertPredicatePartiallySatisfied(Function rankingFunction, RankingType rankingType)
    {
        tester().assertThat(new PushPredicateThroughProjectIntoWindow(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.filter(
                            new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(BIGINT, "ranking"), new Constant(BIGINT, 5L)), new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 0L)))),
                            p.project(
                                    Assignments.identity(ranking, a),
                                    p.window(
                                            new DataOrganizationSpecification(
                                                    ImmutableList.of(),
                                                    Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))),
                                            ImmutableMap.of(ranking, rankingFunction),
                                            p.values(a))));
                })
                .matches(filter(
                        new Comparison(GREATER_THAN, new Reference(BIGINT, "a"), new Constant(BIGINT, 0L)),
                        project(
                                ImmutableMap.of("ranking", expression(new Reference(BIGINT, "ranking")), "a", expression(new Reference(BIGINT, "a"))),
                                topNRanking(
                                        pattern -> pattern
                                                .specification(
                                                        ImmutableList.of(),
                                                        ImmutableList.of("a"),
                                                        ImmutableMap.of("a", ASC_NULLS_FIRST))
                                                .rankingType(rankingType)
                                                .maxRankingPerPartition(4)
                                                .partial(false),
                                        values(ImmutableList.of("a")))
                                        .withAlias("ranking", new TopNRankingSymbolMatcher()))));

        tester().assertThat(new PushPredicateThroughProjectIntoWindow(tester().getPlannerContext()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.filter(
                            new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(BIGINT, "ranking"), new Constant(BIGINT, 5L)), new Comparison(EQUAL, new Call(MODULUS_INTEGER, ImmutableList.of(new Reference(INTEGER, "ranking"), new Constant(INTEGER, 2L))), new Constant(INTEGER, 0L)))),
                            p.project(
                                    Assignments.identity(ranking),
                                    p.window(
                                            new DataOrganizationSpecification(
                                                    ImmutableList.of(),
                                                    Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))),
                                            ImmutableMap.of(ranking, rankingFunction),
                                            p.values(a))));
                })
                .matches(filter(
                        new Comparison(EQUAL, new Call(MODULUS_INTEGER, ImmutableList.of(new Reference(INTEGER, "ranking"), new Constant(INTEGER, 2L))), new Constant(INTEGER, 0L)),
                        project(
                                ImmutableMap.of("ranking", expression(new Reference(BIGINT, "ranking"))),
                                topNRanking(
                                        pattern -> pattern
                                                .specification(
                                                        ImmutableList.of(),
                                                        ImmutableList.of("a"),
                                                        ImmutableMap.of("a", ASC_NULLS_FIRST))
                                                .rankingType(rankingType)
                                                .maxRankingPerPartition(4)
                                                .partial(false),
                                        values(ImmutableList.of("a")))
                                        .withAlias("ranking", new TopNRankingSymbolMatcher()))));
    }

    private Function rowNumberFunction()
    {
        return new Function(
                tester().getMetadata().resolveBuiltinFunction("row_number", fromTypes()),
                ImmutableList.of(),
                DEFAULT_FRAME,
                false);
    }

    private Function rankFunction()
    {
        return new Function(
                tester().getMetadata().resolveBuiltinFunction("rank", fromTypes()),
                ImmutableList.of(),
                DEFAULT_FRAME,
                false);
    }
}
