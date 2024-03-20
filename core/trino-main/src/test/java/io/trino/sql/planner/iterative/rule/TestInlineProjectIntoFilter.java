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
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.IsNullPredicate;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.ir.LogicalExpression.Operator.AND;
import static io.trino.sql.ir.LogicalExpression.Operator.OR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestInlineProjectIntoFilter
        extends BaseRuleTest

{
    @Test
    public void testInlineProjection()
    {
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new SymbolReference(INTEGER, "a"),
                        p.project(
                                Assignments.of(p.symbol("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "b"), new Constant(INTEGER, 0L))),
                                p.values(p.symbol("b", INTEGER)))))
                .matches(
                        project(
                                ImmutableMap.of("a", expression(TRUE_LITERAL)),
                                filter(
                                        new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "b"), new Constant(INTEGER, 0L)),
                                        project(
                                                ImmutableMap.of("b", expression(new SymbolReference(INTEGER, "b"))),
                                                values("b")))));

        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> {
                    Symbol a = p.symbol("a", INTEGER);
                    Symbol b = p.symbol("b", INTEGER);
                    Symbol c = p.symbol("c", INTEGER);
                    Symbol d = p.symbol("d", INTEGER);
                    return p.filter(
                            new LogicalExpression(AND, ImmutableList.of(new SymbolReference(INTEGER, "a"), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "b"), new SymbolReference(INTEGER, "c")))),
                            p.project(
                                    Assignments.builder()
                                            .put(a, new IsNullPredicate(new SymbolReference(INTEGER, "d")))
                                            .put(b, new SymbolReference(INTEGER, "b"))
                                            .put(c, new SymbolReference(INTEGER, "c"))
                                            .build(),
                                    p.values(b, c, d)));
                })
                .matches(
                        project(
                                ImmutableMap.of("b", expression(new SymbolReference(INTEGER, "b")), "c", expression(new SymbolReference(INTEGER, "c")), "a", expression(TRUE_LITERAL)),
                                filter(
                                        new LogicalExpression(AND, ImmutableList.of(new IsNullPredicate(new SymbolReference(INTEGER, "d")), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "b"), new SymbolReference(INTEGER, "c")))),
                                        project(
                                                ImmutableMap.of("d", expression(new SymbolReference(INTEGER, "d")), "b", expression(new SymbolReference(INTEGER, "b")), "c", expression(new SymbolReference(INTEGER, "c"))),
                                                values("b", "c", "d")))));
    }

    @Test
    public void testNoSimpleConjuncts()
    {
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new LogicalExpression(OR, ImmutableList.of(new SymbolReference(INTEGER, "a"), FALSE_LITERAL)),
                        p.project(
                                Assignments.of(p.symbol("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "b"), new Constant(INTEGER, 0L))),
                                p.values(p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testMultipleReferencesToConjunct()
    {
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(INTEGER, "a"), new SymbolReference(INTEGER, "a"))),
                        p.project(
                                Assignments.of(p.symbol("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "b"), new Constant(INTEGER, 0L))),
                                p.values(p.symbol("b")))))
                .doesNotFire();

        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(INTEGER, "a"), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(INTEGER, "a"), FALSE_LITERAL)))),
                        p.project(
                                Assignments.of(p.symbol("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "b"), new Constant(INTEGER, 0L))),
                                p.values(p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testInlineMultiple()
    {
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(INTEGER, "a"), new SymbolReference(INTEGER, "b"))),
                        p.project(
                                Assignments.of(
                                        p.symbol("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "c"), new Constant(INTEGER, 0L)),
                                        p.symbol("b"), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "c"), new Constant(INTEGER, 5L))),
                                p.values(p.symbol("c", INTEGER)))))
                .matches(
                        project(
                                filter(
                                        new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "c"), new Constant(INTEGER, 0L)), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "c"), new Constant(INTEGER, 5L)))),
                                        project(
                                                ImmutableMap.of("c", expression(new SymbolReference(INTEGER, "c"))),
                                                values("c")))));
    }

    @Test
    public void testInlinePartially()
    {
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(INTEGER, "a"), new SymbolReference(INTEGER, "a"), new SymbolReference(INTEGER, "b"))),
                        p.project(
                                Assignments.of(
                                        p.symbol("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "c"), new Constant(INTEGER, 0L)),
                                        p.symbol("b"), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "c"), new Constant(INTEGER, 5L))),
                                p.values(p.symbol("c", INTEGER)))))
                .matches(
                        project(
                                ImmutableMap.of("a", expression(new SymbolReference(INTEGER, "a")), "b", expression(TRUE_LITERAL)),
                                filter(
                                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(INTEGER, "a"), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "c"), new Constant(INTEGER, 5L)))), // combineConjuncts() removed duplicate conjunct `a`. The predicate is now eligible for further inlining.
                                        project(
                                                ImmutableMap.of(
                                                        "a", expression(new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "c"), new Constant(INTEGER, 0L))),
                                                        "c", expression(new SymbolReference(INTEGER, "c"))),
                                                values("c")))));
    }

    @Test
    public void testTrivialProjection()
    {
        // identity projection
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new SymbolReference(INTEGER, "a"),
                        p.project(
                                Assignments.of(p.symbol("a"), new SymbolReference(INTEGER, "a")),
                                p.values(p.symbol("a")))))
                .doesNotFire();

        // renaming projection
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new SymbolReference(INTEGER, "a"),
                        p.project(
                                Assignments.of(p.symbol("a"), new SymbolReference(INTEGER, "b")),
                                p.values(p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testCorrelationSymbol()
    {
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new SymbolReference(INTEGER, "corr"),
                        p.project(
                                Assignments.of(p.symbol("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "b"), new Constant(INTEGER, 0L))),
                                p.values(p.symbol("b")))))
                .doesNotFire();
    }
}
