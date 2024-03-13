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
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static io.trino.sql.tree.LogicalExpression.Operator.OR;

public class TestInlineProjectIntoFilter
        extends BaseRuleTest

{
    @Test
    public void testInlineProjection()
    {
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        new SymbolReference("a"),
                        p.project(
                                Assignments.of(p.symbol("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("0"))),
                                p.values(p.symbol("b")))))
                .matches(
                        project(
                                ImmutableMap.of("a", expression(TRUE_LITERAL)),
                                filter(
                                        new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("0")),
                                        project(
                                                ImmutableMap.of("b", expression(new SymbolReference("b"))),
                                                values("b")))));

        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    return p.filter(
                            new LogicalExpression(AND, ImmutableList.of(new SymbolReference("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new SymbolReference("c")))),
                            p.project(
                                    Assignments.builder()
                                            .put(a, new IsNullPredicate(new SymbolReference("d")))
                                            .put(b, new SymbolReference("b"))
                                            .put(c, new SymbolReference("c"))
                                            .build(),
                                    p.values(b, c, d)));
                })
                .matches(
                        project(
                                ImmutableMap.of("b", expression(new SymbolReference("b")), "c", expression(new SymbolReference("c")), "a", expression(TRUE_LITERAL)),
                                filter(
                                        new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference("d")), new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new SymbolReference("c")))),
                                        project(
                                                ImmutableMap.of("d", expression(new SymbolReference("d")), "b", expression(new SymbolReference("b")), "c", expression(new SymbolReference("c"))),
                                                values("b", "c", "d")))));
    }

    @Test
    public void testNoSimpleConjuncts()
    {
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        new LogicalExpression(OR, ImmutableList.of(new SymbolReference("a"), FALSE_LITERAL)),
                        p.project(
                                Assignments.of(p.symbol("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("0"))),
                                p.values(p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testMultipleReferencesToConjunct()
    {
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("a"), new SymbolReference("a"))),
                        p.project(
                                Assignments.of(p.symbol("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("0"))),
                                p.values(p.symbol("b")))))
                .doesNotFire();

        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("a"), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("a"), FALSE_LITERAL)))),
                        p.project(
                                Assignments.of(p.symbol("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("0"))),
                                p.values(p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testInlineMultiple()
    {
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("a"), new SymbolReference("b"))),
                        p.project(
                                Assignments.of(
                                        p.symbol("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference("c"), new LongLiteral("0")),
                                        p.symbol("b"), new ComparisonExpression(GREATER_THAN, new SymbolReference("c"), new LongLiteral("5"))),
                                p.values(p.symbol("c")))))
                .matches(
                        project(
                                filter(
                                        new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("c"), new LongLiteral("0")), new ComparisonExpression(GREATER_THAN, new SymbolReference("c"), new LongLiteral("5")))),
                                        project(
                                                ImmutableMap.of("c", expression(new SymbolReference("c"))),
                                                values("c")))));
    }

    @Test
    public void testInlinePartially()
    {
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("a"), new SymbolReference("a"), new SymbolReference("b"))),
                        p.project(
                                Assignments.of(
                                        p.symbol("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference("c"), new LongLiteral("0")),
                                        p.symbol("b"), new ComparisonExpression(GREATER_THAN, new SymbolReference("c"), new LongLiteral("5"))),
                                p.values(p.symbol("c")))))
                .matches(
                        project(
                                ImmutableMap.of("a", expression(new SymbolReference("a")), "b", expression(TRUE_LITERAL)),
                                filter(
                                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference("c"), new LongLiteral("5")))), // combineConjuncts() removed duplicate conjunct `a`. The predicate is now eligible for further inlining.
                                        project(
                                                ImmutableMap.of(
                                                        "a", expression(new ComparisonExpression(GREATER_THAN, new SymbolReference("c"), new LongLiteral("0"))),
                                                        "c", expression(new SymbolReference("c"))),
                                                values("c")))));
    }

    @Test
    public void testTrivialProjection()
    {
        // identity projection
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        new SymbolReference("a"),
                        p.project(
                                Assignments.of(p.symbol("a"), new SymbolReference("a")),
                                p.values(p.symbol("a")))))
                .doesNotFire();

        // renaming projection
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        new SymbolReference("a"),
                        p.project(
                                Assignments.of(p.symbol("a"), new SymbolReference("b")),
                                p.values(p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testCorrelationSymbol()
    {
        tester().assertThat(new InlineProjectIntoFilter(tester().getMetadata()))
                .on(p -> p.filter(
                        new SymbolReference("corr"),
                        p.project(
                                Assignments.of(p.symbol("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("0"))),
                                p.values(p.symbol("b")))))
                .doesNotFire();
    }
}
