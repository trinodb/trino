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
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
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
                        new Reference(BOOLEAN, "a"),
                        p.project(
                                Assignments.of(p.symbol("a", BOOLEAN), new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L))),
                                p.values(p.symbol("b", INTEGER)))))
                .matches(
                        project(
                                ImmutableMap.of("a", expression(TRUE)),
                                filter(
                                        new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L)),
                                        project(
                                                ImmutableMap.of("b", expression(new Reference(INTEGER, "b"))),
                                                values("b")))));

        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> {
                    Symbol a = p.symbol("a", BOOLEAN);
                    Symbol b = p.symbol("b", INTEGER);
                    Symbol c = p.symbol("c", INTEGER);
                    Symbol d = p.symbol("d", INTEGER);
                    return p.filter(
                            new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "a"), new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Reference(INTEGER, "c")))),
                            p.project(
                                    Assignments.builder()
                                            .put(a, new IsNull(new Reference(INTEGER, "d")))
                                            .put(b, new Reference(INTEGER, "b"))
                                            .put(c, new Reference(INTEGER, "c"))
                                            .build(),
                                    p.values(b, c, d)));
                })
                .matches(
                        project(
                                ImmutableMap.of("b", expression(new Reference(INTEGER, "b")), "c", expression(new Reference(INTEGER, "c")), "a", expression(TRUE)),
                                filter(
                                        new Logical(AND, ImmutableList.of(new IsNull(new Reference(INTEGER, "d")), new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Reference(INTEGER, "c")))),
                                        project(
                                                ImmutableMap.of("d", expression(new Reference(INTEGER, "d")), "b", expression(new Reference(INTEGER, "b")), "c", expression(new Reference(INTEGER, "c"))),
                                                values("b", "c", "d")))));
    }

    @Test
    public void testNoSimpleConjuncts()
    {
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "a"), FALSE)),
                        p.project(
                                Assignments.of(p.symbol("a", BOOLEAN), new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L))),
                                p.values(p.symbol("b", INTEGER)))))
                .doesNotFire();
    }

    @Test
    public void testMultipleReferencesToConjunct()
    {
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "a"), new Reference(BOOLEAN, "a"))),
                        p.project(
                                Assignments.of(p.symbol("a", BOOLEAN), new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L))),
                                p.values(p.symbol("b", INTEGER)))))
                .doesNotFire();

        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "a"), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "a"), FALSE)))),
                        p.project(
                                Assignments.of(p.symbol("a", BOOLEAN), new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L))),
                                p.values(p.symbol("b", INTEGER)))))
                .doesNotFire();
    }

    @Test
    public void testInlineMultiple()
    {
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "a"), new Reference(BOOLEAN, "b"))),
                        p.project(
                                Assignments.of(
                                        p.symbol("a", BOOLEAN), new Comparison(GREATER_THAN, new Reference(INTEGER, "c"), new Constant(INTEGER, 0L)),
                                        p.symbol("b", BOOLEAN), new Comparison(GREATER_THAN, new Reference(INTEGER, "c"), new Constant(INTEGER, 5L))),
                                p.values(p.symbol("c", INTEGER)))))
                .matches(
                        project(
                                filter(
                                        new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(INTEGER, "c"), new Constant(INTEGER, 0L)), new Comparison(GREATER_THAN, new Reference(INTEGER, "c"), new Constant(INTEGER, 5L)))),
                                        project(
                                                ImmutableMap.of("c", expression(new Reference(INTEGER, "c"))),
                                                values("c")))));
    }

    @Test
    public void testInlinePartially()
    {
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "a"), new Reference(BOOLEAN, "a"), new Reference(BOOLEAN, "b"))),
                        p.project(
                                Assignments.of(
                                        p.symbol("a", BOOLEAN), new Comparison(GREATER_THAN, new Reference(INTEGER, "c"), new Constant(INTEGER, 0L)),
                                        p.symbol("b", BOOLEAN), new Comparison(GREATER_THAN, new Reference(INTEGER, "c"), new Constant(INTEGER, 5L))),
                                p.values(p.symbol("c", INTEGER)))))
                .matches(
                        project(
                                ImmutableMap.of("a", expression(new Reference(BOOLEAN, "a")), "b", expression(TRUE)),
                                filter(
                                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "a"), new Comparison(GREATER_THAN, new Reference(INTEGER, "c"), new Constant(INTEGER, 5L)))), // combineConjuncts() removed duplicate conjunct `a`. The predicate is now eligible for further inlining.
                                        project(
                                                ImmutableMap.of(
                                                        "a", expression(new Comparison(GREATER_THAN, new Reference(INTEGER, "c"), new Constant(INTEGER, 0L))),
                                                        "c", expression(new Reference(INTEGER, "c"))),
                                                values("c")))));
    }

    @Test
    public void testTrivialProjection()
    {
        // identity projection
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new Reference(INTEGER, "a"),
                        p.project(
                                Assignments.of(p.symbol("a", INTEGER), new Reference(INTEGER, "a")),
                                p.values(p.symbol("a", INTEGER)))))
                .doesNotFire();

        // renaming projection
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new Reference(INTEGER, "a"),
                        p.project(
                                Assignments.of(p.symbol("a", INTEGER), new Reference(INTEGER, "b")),
                                p.values(p.symbol("b", INTEGER)))))
                .doesNotFire();
    }

    @Test
    public void testCorrelationSymbol()
    {
        tester().assertThat(new InlineProjectIntoFilter())
                .on(p -> p.filter(
                        new Reference(INTEGER, "corr"),
                        p.project(
                                Assignments.of(p.symbol("a", BOOLEAN), new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L))),
                                p.values(p.symbol("b", INTEGER)))))
                .doesNotFire();
    }
}
