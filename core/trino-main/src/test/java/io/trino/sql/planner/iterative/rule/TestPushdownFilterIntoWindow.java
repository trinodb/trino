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
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.TopNRankingSymbolMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.WindowFrame;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topNRanking;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;

public class TestPushdownFilterIntoWindow
        extends BaseRuleTest
{
    @Test
    public void testEliminateFilter()
    {
        ResolvedFunction rowNumber = tester().getMetadata().resolveFunction(QualifiedName.of("row_number"), fromTypes());
        tester().assertThat(new PushdownFilterIntoWindow(tester().getMetadata(), new TypeOperators()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    Symbol a = p.symbol("a", BIGINT);
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.filter(expression("row_number_1 < cast(100 as bigint)"), p.window(
                            new WindowNode.Specification(ImmutableList.of(a), Optional.of(orderingScheme)),
                            ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(rowNumber, a)),
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
        ResolvedFunction rowNumber = tester().getMetadata().resolveFunction(QualifiedName.of("row_number"), fromTypes());
        tester().assertThat(new PushdownFilterIntoWindow(tester().getMetadata(), new TypeOperators()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    Symbol a = p.symbol("a", BIGINT);
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.filter(expression("cast(3 as bigint) < row_number_1 and row_number_1 < cast(100 as bigint)"), p.window(
                            new WindowNode.Specification(ImmutableList.of(a), Optional.of(orderingScheme)),
                            ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(rowNumber, a)),
                            p.values(p.symbol("a"))));
                })
                .matches(filter(
                        "cast(3 as bigint) < row_number_1 and row_number_1 < cast(100 as bigint)",
                        topNRanking(pattern -> pattern
                                .partial(false)
                                .maxRankingPerPartition(99)
                                .specification(
                                        ImmutableList.of("a"),
                                        ImmutableList.of("a"),
                                        ImmutableMap.of("a", SortOrder.ASC_NULLS_FIRST)),
                                values("a")).withAlias("row_number_1", new TopNRankingSymbolMatcher())));

        tester().assertThat(new PushdownFilterIntoWindow(tester().getMetadata(), new TypeOperators()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    Symbol a = p.symbol("a", BIGINT);
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.filter(expression("row_number_1 < cast(100 as bigint) and a = 1"), p.window(
                            new WindowNode.Specification(ImmutableList.of(a), Optional.of(orderingScheme)),
                            ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(rowNumber, a)),
                            p.values(p.symbol("a"))));
                })
                .matches(filter(
                        "a = 1",
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
        ResolvedFunction rowNumber = tester().getMetadata().resolveFunction(QualifiedName.of("row_number"), fromTypes());
        tester().assertThat(new PushdownFilterIntoWindow(tester().getMetadata(), new TypeOperators()))
                .on(p -> {
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    Symbol a = p.symbol("a");
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.filter(
                            expression("cast(3 as bigint) < row_number_1"),
                            p.window(
                                    new WindowNode.Specification(ImmutableList.of(a), Optional.of(orderingScheme)),
                                    ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(rowNumber, a)),
                                    p.values(a)));
                })
                .doesNotFire();
    }

    private static WindowNode.Function newWindowNodeFunction(ResolvedFunction resolvedFunction, Symbol symbol)
    {
        return new WindowNode.Function(
                resolvedFunction,
                ImmutableList.of(symbol.toSymbolReference()),
                new WindowNode.Frame(
                        WindowFrame.Type.RANGE,
                        UNBOUNDED_PRECEDING,
                        Optional.empty(),
                        Optional.empty(),
                        CURRENT_ROW,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                false);
    }
}
