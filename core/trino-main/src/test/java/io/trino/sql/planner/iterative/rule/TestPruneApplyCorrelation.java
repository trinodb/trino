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
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.apply;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;

public class TestPruneApplyCorrelation
        extends BaseRuleTest
{
    @Test
    public void testPruneCorrelationSymbolNotReferencedInSubquery()
    {
        tester().assertThat(new PruneApplyCorrelation())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol inputSymbol = p.symbol("input_symbol");
                    Symbol subquerySymbol = p.symbol("subquery_symbol");
                    Symbol inResult = p.symbol("in_result");
                    return p.apply(
                            Assignments.of(inResult, new InPredicate(a.toSymbolReference(), subquerySymbol.toSymbolReference())),
                            ImmutableList.of(inputSymbol),
                            p.values(a, inputSymbol),
                            p.values(subquerySymbol));
                })
                .matches(
                        apply(
                                ImmutableList.of(),
                                ImmutableMap.of("in_result", ExpressionMatcher.inPredicate(new SymbolReference("a"), new SymbolReference("subquery_symbol"))),
                                values("a", "input_symbol"),
                                values("subquery_symbol")));
    }

    @Test
    public void testAllCorrelationSymbolsReferencedInSubquery()
    {
        tester().assertThat(new PruneApplyCorrelation())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol inputSymbol = p.symbol("input_symbol");
                    Symbol subquerySymbol = p.symbol("subquery_symbol");
                    Symbol inResult = p.symbol("in_result");
                    return p.apply(
                            Assignments.of(inResult, new InPredicate(a.toSymbolReference(), subquerySymbol.toSymbolReference())),
                            ImmutableList.of(inputSymbol),
                            p.values(a, inputSymbol),
                            p.filter(
                                    new ComparisonExpression(GREATER_THAN, subquerySymbol.toSymbolReference(), inputSymbol.toSymbolReference()),
                                    p.values(subquerySymbol)));
                })
                .doesNotFire();
    }
}
