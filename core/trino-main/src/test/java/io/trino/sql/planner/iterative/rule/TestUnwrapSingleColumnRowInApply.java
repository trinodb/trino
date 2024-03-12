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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.RowType;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.ExpressionMatcher;
import io.trino.sql.planner.assertions.SetExpressionMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.assertions.PlanMatchPattern.apply;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.setExpression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.ApplyNode.Operator.EQUAL;
import static io.trino.sql.planner.plan.ApplyNode.Quantifier.ALL;
import static java.util.Collections.emptyList;

public class TestUnwrapSingleColumnRowInApply
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireOnNoSingleColumnRow()
    {
        tester().assertThat(new UnwrapSingleColumnRowInApply(new IrTypeAnalyzer(tester().getPlannerContext())))
                .on(p -> p.apply(
                        ImmutableMap.<Symbol, ApplyNode.SetExpression>builder()
                                .put(p.symbol("output1", BOOLEAN), new ApplyNode.In(new Symbol("value"), new Symbol("element")))
                                .put(p.symbol("output2", BOOLEAN), new ApplyNode.QuantifiedComparison(EQUAL, ALL, new Symbol("value"), new Symbol("element")))
                                .buildOrThrow(),
                        emptyList(),
                        p.values(p.symbol("value", INTEGER)),
                        p.values(p.symbol("element", INTEGER))))
                .doesNotFire();
    }

    @Test
    public void testUnwrapInPredicate()
    {
        tester().assertThat(new UnwrapSingleColumnRowInApply(new IrTypeAnalyzer(tester().getPlannerContext())))
                .on(p -> p.apply(
                        ImmutableMap.<Symbol, ApplyNode.SetExpression>builder()
                                .put(p.symbol("unwrapped", BOOLEAN), new ApplyNode.In(new Symbol("rowValue"), new Symbol("rowElement")))
                                .put(p.symbol("notUnwrapped", BOOLEAN), new ApplyNode.In(new Symbol("nonRowValue"), new Symbol("nonRowElement")))
                                .buildOrThrow(),
                        emptyList(),
                        p.values(
                                p.symbol("rowValue", RowType.anonymousRow(INTEGER)),
                                p.symbol("nonRowValue", INTEGER)),
                        p.values(
                                p.symbol("rowElement", RowType.anonymousRow(INTEGER)),
                                p.symbol("nonRowElement", INTEGER))))
                .matches(
                        project(
                                apply(
                                        List.of(),
                                        ImmutableMap.<String, SetExpressionMatcher>builder()
                                                .put("unwrapped", setExpression(new ApplyNode.In(new Symbol("unwrappedValue"), new Symbol("unwrappedElement"))))
                                                .put("notUnwrapped", setExpression(new ApplyNode.In(new Symbol("nonRowValue"), new Symbol("nonRowElement"))))
                                                .buildOrThrow(),
                                        project(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("unwrappedValue", expression(new SubscriptExpression(new SymbolReference("rowValue"), new io.trino.sql.tree.LongLiteral("1"))))
                                                        .put("nonRowValue", expression(new SymbolReference("nonRowValue")))
                                                        .buildOrThrow(),
                                                values("rowValue", "nonRowValue")),
                                        project(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("unwrappedElement", expression(new SubscriptExpression(new SymbolReference("rowElement"), new io.trino.sql.tree.LongLiteral("1"))))
                                                        .put("nonRowElement", expression(new SymbolReference("nonRowElement")))
                                                        .buildOrThrow(),
                                                values("rowElement", "nonRowElement")))));
    }

    @Test
    public void testUnwrapQuantifiedComparison()
    {
        tester().assertThat(new UnwrapSingleColumnRowInApply(new IrTypeAnalyzer(tester().getPlannerContext())))
                .on(p -> p.apply(
                        ImmutableMap.<Symbol, ApplyNode.SetExpression>builder()
                                .put(p.symbol("unwrapped", BOOLEAN), new ApplyNode.QuantifiedComparison(EQUAL, ALL, new Symbol("rowValue"), new Symbol("rowElement")))
                                .put(p.symbol("notUnwrapped", BOOLEAN), new ApplyNode.QuantifiedComparison(EQUAL, ALL, new Symbol("nonRowValue"), new Symbol("nonRowElement")))
                                .buildOrThrow(),
                        emptyList(),
                        p.values(
                                p.symbol("rowValue", RowType.anonymousRow(INTEGER)),
                                p.symbol("nonRowValue", INTEGER)),
                        p.values(
                                p.symbol("rowElement", RowType.anonymousRow(INTEGER)),
                                p.symbol("nonRowElement", INTEGER))))
                .matches(
                        project(
                                apply(
                                        List.of(),
                                        ImmutableMap.<String, SetExpressionMatcher>builder()
                                                .put("unwrapped", setExpression(new ApplyNode.QuantifiedComparison(EQUAL, ALL, new Symbol("unwrappedValue"), new Symbol("unwrappedElement"))))
                                                .put("notUnwrapped", setExpression(new ApplyNode.QuantifiedComparison(EQUAL, ALL, new Symbol("nonRowValue"), new Symbol("nonRowElement"))))
                                                .buildOrThrow(),
                                        project(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("unwrappedValue", expression(new SubscriptExpression(new SymbolReference("rowValue"), new io.trino.sql.tree.LongLiteral("1"))))
                                                        .put("nonRowValue", expression(new SymbolReference("nonRowValue")))
                                                        .buildOrThrow(),
                                                values("rowValue", "nonRowValue")),
                                        project(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("unwrappedElement", expression(new SubscriptExpression(new SymbolReference("rowElement"), new io.trino.sql.tree.LongLiteral("1"))))
                                                        .put("nonRowElement", expression(new SymbolReference("nonRowElement")))
                                                        .buildOrThrow(),
                                                values("rowElement", "nonRowElement")))));
    }
}
