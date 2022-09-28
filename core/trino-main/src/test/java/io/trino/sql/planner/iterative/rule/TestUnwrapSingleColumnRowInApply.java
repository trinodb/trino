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
import io.trino.sql.planner.assertions.ExpressionMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.sql.planner.assertions.PlanMatchPattern.apply;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.QuantifiedComparisonExpression.Quantifier.ALL;
import static java.util.Collections.emptyList;

public class TestUnwrapSingleColumnRowInApply
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireOnNoSingleColumnRow()
    {
        tester().assertThat(new UnwrapSingleColumnRowInApply(createTestingTypeAnalyzer(tester().getPlannerContext())))
                .on(p -> p.apply(
                        Assignments.builder()
                                .put(p.symbol("output1", BOOLEAN), new InPredicate(new SymbolReference("value"), new SymbolReference("element")))
                                .put(p.symbol("output2", BOOLEAN), new QuantifiedComparisonExpression(EQUAL, ALL, new SymbolReference("value"), new SymbolReference("element")))
                                .build(),
                        emptyList(),
                        p.values(p.symbol("value", INTEGER)),
                        p.values(p.symbol("element", INTEGER))))
                .doesNotFire();
    }

    @Test
    public void testUnwrapInPredicate()
    {
        tester().assertThat(new UnwrapSingleColumnRowInApply(createTestingTypeAnalyzer(tester().getPlannerContext())))
                .on(p -> p.apply(
                        Assignments.builder()
                                .put(p.symbol("unwrapped", BOOLEAN), new InPredicate(new SymbolReference("rowValue"), new SymbolReference("rowElement")))
                                .put(p.symbol("notUnwrapped", BOOLEAN), new InPredicate(new SymbolReference("nonRowValue"), new SymbolReference("nonRowElement")))
                                .build(),
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
                                        ImmutableMap.<String, ExpressionMatcher>builder()
                                                .put("unwrapped", expression("unwrappedValue IN (unwrappedElement)"))
                                                .put("notUnwrapped", expression("nonRowValue IN (nonRowElement)"))
                                                .buildOrThrow(),
                                        project(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("unwrappedValue", expression("rowValue[1]"))
                                                        .put("nonRowValue", expression("nonRowValue"))
                                                        .buildOrThrow(),
                                                values("rowValue", "nonRowValue")),
                                        project(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("unwrappedElement", expression("rowElement[1]"))
                                                        .put("nonRowElement", expression("nonRowElement"))
                                                        .buildOrThrow(),
                                                values("rowElement", "nonRowElement")))));
    }

    @Test
    public void testUnwrapQuantifiedComparison()
    {
        tester().assertThat(new UnwrapSingleColumnRowInApply(createTestingTypeAnalyzer(tester().getPlannerContext())))
                .on(p -> p.apply(
                        Assignments.builder()
                                .put(p.symbol("unwrapped", BOOLEAN), new QuantifiedComparisonExpression(EQUAL, ALL, new SymbolReference("rowValue"), new SymbolReference("rowElement")))
                                .put(p.symbol("notUnwrapped", BOOLEAN), new QuantifiedComparisonExpression(EQUAL, ALL, new SymbolReference("nonRowValue"), new SymbolReference("nonRowElement")))
                                .build(),
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
                                        ImmutableMap.<String, ExpressionMatcher>builder()
                                                .put("unwrapped", expression(new QuantifiedComparisonExpression(EQUAL, ALL, new SymbolReference("unwrappedValue"), new SymbolReference("unwrappedElement"))))
                                                .put("notUnwrapped", expression(new QuantifiedComparisonExpression(EQUAL, ALL, new SymbolReference("nonRowValue"), new SymbolReference("nonRowElement"))))
                                                .buildOrThrow(),
                                        project(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("unwrappedValue", expression("rowValue[1]"))
                                                        .put("nonRowValue", expression("nonRowValue"))
                                                        .buildOrThrow(),
                                                values("rowValue", "nonRowValue")),
                                        project(
                                                ImmutableMap.<String, ExpressionMatcher>builder()
                                                        .put("unwrappedElement", expression("rowElement[1]"))
                                                        .put("nonRowElement", expression("nonRowElement"))
                                                        .buildOrThrow(),
                                                values("rowElement", "nonRowElement")))));
    }
}
