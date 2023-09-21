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
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.apply;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneApplySourceColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllSubquerySymbolsReferenced()
    {
        tester().assertThat(new PruneApplySourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol subquerySymbol1 = p.symbol("subquery_symbol_1");
                    Symbol subquerySymbol2 = p.symbol("subquery_symbol_2");
                    Symbol inResult = p.symbol("in_result");
                    return p.apply(
                            Assignments.of(inResult, new InPredicate(a.toSymbolReference(), subquerySymbol1.toSymbolReference())),
                            ImmutableList.of(),
                            p.values(a),
                            p.values(subquerySymbol1, subquerySymbol2));
                })
                .matches(
                        apply(
                                ImmutableList.of(),
                                ImmutableMap.of("in_result", ExpressionMatcher.inPredicate(new SymbolReference("a"), new SymbolReference("subquery_symbol_1"))),
                                values("a"),
                                project(
                                        ImmutableMap.of("subquery_symbol_1", PlanMatchPattern.expression("subquery_symbol_1")),
                                        values("subquery_symbol_1", "subquery_symbol_2"))));
    }

    @Test
    public void testAllSubquerySymbolsReferenced()
    {
        tester().assertThat(new PruneApplySourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol subquerySymbol1 = p.symbol("subquery_symbol_1");
                    Symbol subquerySymbol2 = p.symbol("subquery_symbol_2");
                    Symbol inResult1 = p.symbol("in_result_1");
                    Symbol inResult2 = p.symbol("in_result_2");
                    return p.apply(
                            Assignments.of(
                                    inResult1, new InPredicate(a.toSymbolReference(), subquerySymbol1.toSymbolReference()),
                                    inResult2, new InPredicate(a.toSymbolReference(), subquerySymbol2.toSymbolReference())),
                            ImmutableList.of(),
                            p.values(a),
                            p.values(subquerySymbol1, subquerySymbol2));
                })
                .doesNotFire();
    }

    @Test
    public void testNoSubquerySymbolsReferenced()
    {
        tester().assertThat(new PruneApplySourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol subquerySymbol = p.symbol("subquery_symbol");
                    Symbol inResult = p.symbol("in_result");
                    return p.apply(
                            Assignments.of(inResult, new InPredicate(a.toSymbolReference(), a.toSymbolReference())),
                            ImmutableList.of(),
                            p.values(a),
                            p.values(subquerySymbol));
                })
                .matches(
                        apply(
                                ImmutableList.of(),
                                ImmutableMap.of("in_result", ExpressionMatcher.inPredicate(new SymbolReference("a"), new SymbolReference("a"))),
                                values("a"),
                                project(
                                        ImmutableMap.of(),
                                        values("subquery_symbol"))));
    }
}
