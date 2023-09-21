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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.ExpressionMatcher;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.apply;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;

public class TestPruneApplyColumns
        extends BaseRuleTest
{
    @Test
    public void testRemoveUnusedApplyNode()
    {
        tester().assertThat(new PruneApplyColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol correlationSymbol = p.symbol("correlation_symbol");
                    Symbol subquerySymbol = p.symbol("subquery_symbol");
                    Symbol inResult = p.symbol("in_result");
                    return p.project(
                            Assignments.identity(a),
                            p.apply(
                                    Assignments.of(inResult, new InPredicate(a.toSymbolReference(), subquerySymbol.toSymbolReference())),
                                    ImmutableList.of(correlationSymbol),
                                    p.values(a, correlationSymbol),
                                    p.filter(
                                            new ComparisonExpression(GREATER_THAN, subquerySymbol.toSymbolReference(), correlationSymbol.toSymbolReference()),
                                            p.values(subquerySymbol))));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", PlanMatchPattern.expression("a")),
                                values("a", "correlationSymbol")));
    }

    @Test
    public void testRemoveUnreferencedAssignments()
    {
        // remove assignment and prune unused input symbol
        tester().assertThat(new PruneApplyColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol correlationSymbol = p.symbol("correlation_symbol");
                    Symbol subquerySymbol = p.symbol("subquery_symbol");
                    Symbol inResult1 = p.symbol("in_result_1");
                    Symbol inResult2 = p.symbol("in_result_2");
                    return p.project(
                            Assignments.identity(a, inResult1),
                            p.apply(
                                    Assignments.of(
                                            inResult1, new InPredicate(a.toSymbolReference(), subquerySymbol.toSymbolReference()),
                                            inResult2, new InPredicate(b.toSymbolReference(), subquerySymbol.toSymbolReference())),
                                    ImmutableList.of(correlationSymbol),
                                    p.values(a, b, correlationSymbol),
                                    p.filter(
                                            new ComparisonExpression(GREATER_THAN, subquerySymbol.toSymbolReference(), correlationSymbol.toSymbolReference()),
                                            p.values(subquerySymbol))));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", PlanMatchPattern.expression("a"), "in_result_1", PlanMatchPattern.expression("in_result_1")),
                                apply(
                                        ImmutableList.of("correlation_symbol"),
                                        ImmutableMap.of("in_result_1", ExpressionMatcher.inPredicate(new SymbolReference("a"), new SymbolReference("subquery_symbol"))),
                                        project(
                                                ImmutableMap.of("a", PlanMatchPattern.expression("a"), "correlation_symbol", PlanMatchPattern.expression("correlation_symbol")),
                                                values("a", "b", "correlation_symbol")),
                                        node(
                                                FilterNode.class,
                                                values("subquery_symbol")))));

        // remove assignment and prune unused subquery symbol
        tester().assertThat(new PruneApplyColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol correlationSymbol = p.symbol("correlation_symbol");
                    Symbol subquerySymbol1 = p.symbol("subquery_symbol_1");
                    Symbol subquerySymbol2 = p.symbol("subquery_symbol_2");
                    Symbol inResult1 = p.symbol("in_result_1");
                    Symbol inResult2 = p.symbol("in_result_2");
                    return p.project(
                            Assignments.identity(a, inResult1),
                            p.apply(
                                    Assignments.of(
                                            inResult1, new InPredicate(a.toSymbolReference(), subquerySymbol1.toSymbolReference()),
                                            inResult2, new InPredicate(a.toSymbolReference(), subquerySymbol2.toSymbolReference())),
                                    ImmutableList.of(correlationSymbol),
                                    p.values(a, correlationSymbol),
                                    p.filter(
                                            new ComparisonExpression(GREATER_THAN, subquerySymbol1.toSymbolReference(), correlationSymbol.toSymbolReference()),
                                            p.values(subquerySymbol1, subquerySymbol2))));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", PlanMatchPattern.expression("a"), "in_result_1", PlanMatchPattern.expression("in_result_1")),
                                apply(
                                        ImmutableList.of("correlation_symbol"),
                                        ImmutableMap.of("in_result_1", ExpressionMatcher.inPredicate(new SymbolReference("a"), new SymbolReference("subquery_symbol_1"))),
                                        values("a", "correlation_symbol"),
                                        project(
                                                ImmutableMap.of("subquery_symbol_1", PlanMatchPattern.expression("subquery_symbol_1")),
                                                node(
                                                        FilterNode.class,
                                                        values("subquery_symbol_1", "subquery_symbol_2"))))));
    }

    @Test
    public void testPruneUnreferencedSubquerySymbol()
    {
        tester().assertThat(new PruneApplyColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol correlationSymbol = p.symbol("correlation_symbol");
                    Symbol unreferenced = p.symbol("unreferenced");
                    Symbol subquerySymbol = p.symbol("subquery_symbol");
                    Symbol inResult = p.symbol("in_result");
                    return p.project(
                            Assignments.identity(correlationSymbol, inResult),
                            p.apply(
                                    Assignments.of(inResult, new InPredicate(a.toSymbolReference(), subquerySymbol.toSymbolReference())),
                                    ImmutableList.of(correlationSymbol),
                                    p.values(a, correlationSymbol),
                                    p.filter(
                                            new ComparisonExpression(GREATER_THAN, unreferenced.toSymbolReference(), correlationSymbol.toSymbolReference()),
                                            p.values(unreferenced, subquerySymbol))));
                })
                .matches(
                        project(
                                ImmutableMap.of("correlation_symbol", PlanMatchPattern.expression("correlation_symbol"), "in_result", PlanMatchPattern.expression("in_result")),
                                apply(
                                        ImmutableList.of("correlation_symbol"),
                                        ImmutableMap.of("in_result", ExpressionMatcher.inPredicate(new SymbolReference("a"), new SymbolReference("subquery_symbol"))),
                                        values("a", "correlation_symbol"),
                                        project(
                                                ImmutableMap.of("subquery_symbol", PlanMatchPattern.expression("subquery_symbol")),
                                                node(
                                                        FilterNode.class,
                                                        values("unreferenced", "subquery_symbol"))))));
    }

    @Test
    public void testPruneUnreferencedInputSymbol()
    {
        tester().assertThat(new PruneApplyColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol unreferenced = p.symbol("unreferenced");
                    Symbol correlationSymbol = p.symbol("correlation_symbol");
                    Symbol subquerySymbol = p.symbol("subquery_symbol");
                    Symbol inResult = p.symbol("in_result");
                    return p.project(
                            Assignments.identity(correlationSymbol, inResult),
                            p.apply(
                                    Assignments.of(inResult, new InPredicate(a.toSymbolReference(), subquerySymbol.toSymbolReference())),
                                    ImmutableList.of(correlationSymbol),
                                    p.values(a, unreferenced, correlationSymbol),
                                    p.filter(
                                            new ComparisonExpression(GREATER_THAN, subquerySymbol.toSymbolReference(), correlationSymbol.toSymbolReference()),
                                            p.values(subquerySymbol))));
                })
                .matches(
                        project(
                                ImmutableMap.of("correlation_symbol", PlanMatchPattern.expression("correlation_symbol"), "in_result", PlanMatchPattern.expression("in_result")),
                                apply(
                                        ImmutableList.of("correlation_symbol"),
                                        ImmutableMap.of("in_result", ExpressionMatcher.inPredicate(new SymbolReference("a"), new SymbolReference("subquery_symbol"))),
                                        project(
                                                ImmutableMap.of("a", PlanMatchPattern.expression("a"), "correlation_symbol", PlanMatchPattern.expression("correlation_symbol")),
                                                values("a", "unreferenced", "correlation_symbol")),
                                        node(
                                                FilterNode.class,
                                                values("subquery_symbol")))));
    }

    @Test
    public void testDoNotPruneUnreferencedUsedCorrelationSymbol()
    {
        tester().assertThat(new PruneApplyColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol correlationSymbol = p.symbol("correlation_symbol");
                    Symbol subquerySymbol = p.symbol("subquery_symbol");
                    Symbol inResult = p.symbol("in_result");
                    return p.project(
                            Assignments.identity(a, inResult),
                            p.apply(
                                    Assignments.of(inResult, new InPredicate(a.toSymbolReference(), subquerySymbol.toSymbolReference())),
                                    ImmutableList.of(correlationSymbol),
                                    p.values(a, correlationSymbol),
                                    p.filter(
                                            new ComparisonExpression(GREATER_THAN, subquerySymbol.toSymbolReference(), correlationSymbol.toSymbolReference()),
                                            p.values(subquerySymbol))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneUnreferencedCorrelationSymbol()
    {
        tester().assertThat(new PruneApplyColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol correlationSymbol = p.symbol("correlation_symbol");
                    Symbol subquerySymbol = p.symbol("subquery_symbol");
                    Symbol inResult = p.symbol("in_result");
                    return p.project(
                            Assignments.identity(a, inResult),
                            p.apply(
                                    Assignments.of(inResult, new InPredicate(a.toSymbolReference(), subquerySymbol.toSymbolReference())),
                                    ImmutableList.of(correlationSymbol),
                                    p.values(a, correlationSymbol),
                                    p.values(subquerySymbol)));
                })
                .doesNotFire();
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneApplyColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol correlationSymbol = p.symbol("correlation_symbol");
                    Symbol subquerySymbol = p.symbol("subquery_symbol");
                    Symbol inResult = p.symbol("in_result");
                    return p.project(
                            Assignments.identity(a, correlationSymbol, inResult),
                            p.apply(
                                    Assignments.of(inResult, new InPredicate(a.toSymbolReference(), subquerySymbol.toSymbolReference())),
                                    ImmutableList.of(correlationSymbol),
                                    p.values(a, correlationSymbol),
                                    p.filter(
                                            new ComparisonExpression(GREATER_THAN, subquerySymbol.toSymbolReference(), correlationSymbol.toSymbolReference()),
                                            p.values(subquerySymbol))));
                })
                .doesNotFire();
    }
}
