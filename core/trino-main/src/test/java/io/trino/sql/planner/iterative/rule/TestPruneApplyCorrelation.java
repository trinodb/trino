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
import io.trino.sql.ir.Comparison;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.ApplyNode;
import org.junit.jupiter.api.Test;

import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.apply;
import static io.trino.sql.planner.assertions.PlanMatchPattern.setExpression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.type.UnknownType.UNKNOWN;

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
                            ImmutableMap.of(inResult, new ApplyNode.In(a, subquerySymbol)),
                            ImmutableList.of(inputSymbol),
                            p.values(a, inputSymbol),
                            p.values(subquerySymbol));
                })
                .matches(
                        apply(
                                ImmutableList.of(),
                                ImmutableMap.of("in_result", setExpression(new ApplyNode.In(new Symbol(UNKNOWN, "a"), new Symbol(UNKNOWN, "subquery_symbol")))),
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
                            ImmutableMap.of(inResult, new ApplyNode.In(a, subquerySymbol)),
                            ImmutableList.of(inputSymbol),
                            p.values(a, inputSymbol),
                            p.filter(
                                    new Comparison(GREATER_THAN, subquerySymbol.toSymbolReference(), inputSymbol.toSymbolReference()),
                                    p.values(subquerySymbol)));
                })
                .doesNotFire();
    }
}
