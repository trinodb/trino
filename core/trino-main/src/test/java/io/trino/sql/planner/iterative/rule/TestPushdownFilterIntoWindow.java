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
import io.trino.spi.connector.SortOrder;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.TopNRankingSymbolMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.WindowNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topNRanking;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;

public class TestPushdownFilterIntoWindow
        extends BaseRuleTest
{
    @Test
    public void testEliminateFilter()
    {
        assertEliminateFilter("row_number");
        assertEliminateFilter("rank");
    }

    private void assertEliminateFilter(String rankingFunctionName)
    {
        ResolvedFunction ranking = tester().getMetadata().resolveBuiltinFunction(rankingFunctionName, fromTypes());
        tester().assertThat(new PushdownFilterIntoWindow(tester().getPlannerContext()))
                .on(p -> {
                    Symbol rankSymbol = p.symbol("rank_1");
                    Symbol a = p.symbol("a", BIGINT);
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.filter(
                            new Comparison(LESS_THAN, new Reference(BIGINT, "rank_1"), new Constant(BIGINT, 100L)),
                            p.window(
                                    new DataOrganizationSpecification(ImmutableList.of(a), Optional.of(orderingScheme)),
                                    ImmutableMap.of(rankSymbol, newWindowNodeFunction(ranking, a)),
                                    p.values(p.symbol("a"))));
                })
                .matches(topNRanking(pattern -> pattern
                                .maxRankingPerPartition(99)
                                .partial(false),
                        values("a")));
    }

    @Test
    public void testKeepFilter()
    {
        assertKeepFilter("row_number");
        assertKeepFilter("rank");
    }

    private void assertKeepFilter(String rankingFunctionName)
    {
        ResolvedFunction ranking = tester().getMetadata().resolveBuiltinFunction(rankingFunctionName, fromTypes());
        tester().assertThat(new PushdownFilterIntoWindow(tester().getPlannerContext()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    Symbol a = p.symbol("a", BIGINT);
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.filter(
                            new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Constant(BIGINT, 3L), new Reference(BIGINT, "row_number_1")), new Comparison(LESS_THAN, new Reference(BIGINT, "row_number_1"), new Constant(BIGINT, 100L)))),
                            p.window(
                                    new DataOrganizationSpecification(ImmutableList.of(a), Optional.of(orderingScheme)),
                                    ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(ranking, a)),
                                    p.values(p.symbol("a"))));
                })
                .matches(filter(
                        new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Constant(BIGINT, 3L), new Reference(BIGINT, "row_number_1")), new Comparison(LESS_THAN, new Reference(BIGINT, "row_number_1"), new Constant(BIGINT, 100L)))),
                        topNRanking(pattern -> pattern
                                        .partial(false)
                                        .maxRankingPerPartition(99)
                                        .specification(
                                                ImmutableList.of("a"),
                                                ImmutableList.of("a"),
                                                ImmutableMap.of("a", SortOrder.ASC_NULLS_FIRST)),
                                values("a")).withAlias("row_number_1", new TopNRankingSymbolMatcher())));

        tester().assertThat(new PushdownFilterIntoWindow(tester().getPlannerContext()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    Symbol a = p.symbol("a", BIGINT);
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.filter(
                            new Logical(AND, ImmutableList.of(
                                    new Comparison(LESS_THAN, new Reference(BIGINT, "row_number_1"), new Constant(BIGINT, 100L)),
                                    new Comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)))),
                            p.window(
                                    new DataOrganizationSpecification(ImmutableList.of(a), Optional.of(orderingScheme)),
                                    ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(ranking, a)),
                                    p.values(p.symbol("a"))));
                })
                .matches(filter(
                        new Comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)),
                        topNRanking(pattern -> pattern
                                        .partial(false)
                                        .maxRankingPerPartition(99)
                                        .specification(
                                                ImmutableList.of("a"),
                                                ImmutableList.of("a"),
                                                ImmutableMap.of("a", SortOrder.ASC_NULLS_FIRST)),
                                values("a")).withAlias("row_number_1", new TopNRankingSymbolMatcher())));
    }

    @Test
    public void testNoUpperBound()
    {
        assertNoUpperBound("row_number");
        assertNoUpperBound("rank");
    }

    private void assertNoUpperBound(String rankingFunctionName)
    {
        ResolvedFunction ranking = tester().getMetadata().resolveBuiltinFunction(rankingFunctionName, fromTypes());
        tester().assertThat(new PushdownFilterIntoWindow(tester().getPlannerContext()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    Symbol a = p.symbol("a");
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.filter(
                            new Comparison(LESS_THAN, new Cast(new Constant(INTEGER, 3L), BIGINT), new Reference(BIGINT, "row_number_1")),
                            p.window(
                                    new DataOrganizationSpecification(ImmutableList.of(a), Optional.of(orderingScheme)),
                                    ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(ranking, a)),
                                    p.values(a)));
                })
                .doesNotFire();
    }

    private static WindowNode.Function newWindowNodeFunction(ResolvedFunction resolvedFunction, Symbol symbol)
    {
        return new WindowNode.Function(
                resolvedFunction,
                ImmutableList.of(symbol.toSymbolReference()),
                DEFAULT_FRAME,
                false);
    }
}
