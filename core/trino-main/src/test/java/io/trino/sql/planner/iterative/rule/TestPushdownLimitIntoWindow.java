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
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topNRanking;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;

public class TestPushdownLimitIntoWindow
        extends BaseRuleTest
{
    @Test
    public void testLimitAboveWindow()
    {
        assertLimitAboveWindow("row_number");
        assertLimitAboveWindow("rank");
    }

    private void assertLimitAboveWindow(String rankingFunctionName)
    {
        ResolvedFunction ranking = tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of(rankingFunctionName), fromTypes());
        tester().assertThat(new PushdownLimitIntoWindow())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.limit(
                            3,
                            p.window(
                                    new DataOrganizationSpecification(ImmutableList.of(a), Optional.of(orderingScheme)),
                                    ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(ranking, a)),
                                    p.values(a)));
                })
                .matches(
                        limit(3, topNRanking(
                                pattern -> pattern
                                        .specification(
                                                ImmutableList.of("a"),
                                                ImmutableList.of("a"),
                                                ImmutableMap.of("a", SortOrder.ASC_NULLS_FIRST))
                                        .maxRankingPerPartition(3)
                                        .partial(false), values("a"))));
    }

    @Test
    public void testConvertToTopNRowNumber()
    {
        ResolvedFunction ranking = tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of("row_number"), fromTypes());
        tester().assertThat(new PushdownLimitIntoWindow())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.limit(3, p.window(
                            new DataOrganizationSpecification(ImmutableList.of(), Optional.of(orderingScheme)),
                            ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(ranking, a)),
                            p.values(a)));
                })
                .matches(
                        topNRanking(pattern -> pattern
                                .specification(
                                        ImmutableList.of(),
                                        ImmutableList.of("a"),
                                        ImmutableMap.of("a", SortOrder.ASC_NULLS_FIRST))
                                .maxRankingPerPartition(3)
                                .partial(false), values("a")));
    }

    @Test
    public void testLimitWithPreSortedInputs()
    {
        // We can push Limit with pre-sorted inputs into WindowNode if ordering scheme is satisfied
        // We don't do it currently to avoid relying on LocalProperties outside of AddExchanges/AddLocalExchanges
        ResolvedFunction ranking = tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of("row_number"), fromTypes());
        tester().assertThat(new PushdownLimitIntoWindow())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.limit(
                            3,
                            false,
                            ImmutableList.of(a),
                            p.window(
                                    new DataOrganizationSpecification(ImmutableList.of(), Optional.of(orderingScheme)),
                                    ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(ranking, a)),
                                    p.values(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testZeroLimit()
    {
        assertZeroLimit("row_number");
        assertZeroLimit("rank");
    }

    private void assertZeroLimit(String rankingFunctionName)
    {
        ResolvedFunction ranking = tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of(rankingFunctionName), fromTypes());
        tester().assertThat(new PushdownLimitIntoWindow())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.limit(
                            0,
                            p.window(
                                    new DataOrganizationSpecification(ImmutableList.of(a), Optional.of(orderingScheme)),
                                    ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(ranking, a)),
                                    p.values(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testWindowNotOrdered()
    {
        assertWindowNotOrdered("row_number");
        assertWindowNotOrdered("rank");
    }

    private void assertWindowNotOrdered(String rankingFunctionName)
    {
        ResolvedFunction ranking = tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of(rankingFunctionName), fromTypes());
        tester().assertThat(new PushdownLimitIntoWindow())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.limit(
                            3,
                            p.window(
                                    new DataOrganizationSpecification(ImmutableList.of(a), Optional.empty()),
                                    ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(ranking, a)),
                                    p.values(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testMultipleWindowFunctions()
    {
        ResolvedFunction rowNumberFunction = tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of("row_number"), fromTypes());
        ResolvedFunction rankFunction = tester().getMetadata().resolveFunction(tester().getSession(), QualifiedName.of("rank"), fromTypes());
        tester().assertThat(new PushdownLimitIntoWindow())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    Symbol rankSymbol = p.symbol("rank_1");
                    return p.limit(
                            3,
                            p.window(
                                    new DataOrganizationSpecification(ImmutableList.of(a), Optional.empty()),
                                    ImmutableMap.of(
                                            rowNumberSymbol,
                                            newWindowNodeFunction(rowNumberFunction, a),
                                            rankSymbol,
                                            newWindowNodeFunction(rankFunction, a)),
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
