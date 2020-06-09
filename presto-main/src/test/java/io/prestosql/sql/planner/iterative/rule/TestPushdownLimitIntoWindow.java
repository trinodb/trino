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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.connector.SortOrder;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.WindowFrame;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.limit;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.topNRowNumber;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.prestosql.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;

public class TestPushdownLimitIntoWindow
        extends BaseRuleTest
{
    @Test
    public void testLimitAboveWindow()
    {
        ResolvedFunction rowNumberFunction = tester().getMetadata().resolveFunction(QualifiedName.of("row_number"), fromTypes());
        tester().assertThat(new PushdownLimitIntoWindow(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.limit(
                            3,
                            p.window(
                                    new WindowNode.Specification(ImmutableList.of(a), Optional.of(orderingScheme)),
                                    ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(rowNumberFunction, a)),
                                    p.values(a)));
                })
                .matches(
                        limit(3, topNRowNumber(
                                pattern -> pattern
                                        .specification(
                                                ImmutableList.of("a"),
                                                ImmutableList.of("a"),
                                                ImmutableMap.of("a", SortOrder.ASC_NULLS_FIRST))
                                        .maxRowCountPerPartition(3)
                                        .partial(false), values("a"))));
    }

    @Test
    public void testConvertToTopNRowNumber()
    {
        ResolvedFunction rowNumberFunction = tester().getMetadata().resolveFunction(QualifiedName.of("row_number"), fromTypes());
        tester().assertThat(new PushdownLimitIntoWindow(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.limit(3, p.window(
                            new WindowNode.Specification(ImmutableList.of(), Optional.of(orderingScheme)),
                            ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(rowNumberFunction, a)),
                            p.values(a)));
                })
                .matches(
                        topNRowNumber(pattern -> pattern
                                .specification(
                                        ImmutableList.of(),
                                        ImmutableList.of("a"),
                                        ImmutableMap.of("a", SortOrder.ASC_NULLS_FIRST))
                        .maxRowCountPerPartition(3)
                        .partial(false), values("a")));
    }

    @Test
    public void testZeroLimit()
    {
        ResolvedFunction rowNumberFunction = tester().getMetadata().resolveFunction(QualifiedName.of("row_number"), fromTypes());
        tester().assertThat(new PushdownLimitIntoWindow(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    OrderingScheme orderingScheme = new OrderingScheme(
                            ImmutableList.of(a),
                            ImmutableMap.of(a, SortOrder.ASC_NULLS_FIRST));
                    return p.limit(
                            0,
                            p.window(
                                    new WindowNode.Specification(ImmutableList.of(a), Optional.of(orderingScheme)),
                                    ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(rowNumberFunction, a)),
                                    p.values(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testWindowNotOrdered()
    {
        ResolvedFunction rowNumberFunction = tester().getMetadata().resolveFunction(QualifiedName.of("row_number"), fromTypes());
        tester().assertThat(new PushdownLimitIntoWindow(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    return p.limit(
                            3,
                            p.window(
                                    new WindowNode.Specification(ImmutableList.of(a), Optional.empty()),
                                    ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(rowNumberFunction, a)),
                                    p.values(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testMultipleWindowFunctions()
    {
        ResolvedFunction rowNumberFunction = tester().getMetadata().resolveFunction(QualifiedName.of("row_number"), fromTypes());
        ResolvedFunction rankFunction = tester().getMetadata().resolveFunction(QualifiedName.of("rank"), fromTypes());
        tester().assertThat(new PushdownLimitIntoWindow(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumberSymbol = p.symbol("row_number_1");
                    Symbol rankSymbol = p.symbol("rank_1");
                    return p.limit(
                            3,
                            p.window(
                                    new WindowNode.Specification(ImmutableList.of(a), Optional.empty()),
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
