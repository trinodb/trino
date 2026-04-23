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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestSimplifyFilterPredicate
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));

    @Test
    public void testSimplifyIfExpression()
    {
        // true result iff the condition is true
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        ifExpression(new Reference(BOOLEAN, "a"), TRUE, FALSE),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                new Reference(BOOLEAN, "a"),
                                values("a")));

        // true result iff the condition is true
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        ifExpression(new Reference(BOOLEAN, "a"), TRUE, new Constant(BOOLEAN, null)),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                new Reference(BOOLEAN, "a"),
                                values("a")));

        // true result iff the condition is null or false
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        ifExpression(new Reference(BOOLEAN, "a"), FALSE, TRUE),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                new Logical(OR, ImmutableList.of(new IsNull(new Reference(BOOLEAN, "a")), not(new Reference(BOOLEAN, "a")))),
                                values("a")));

        // true result iff the condition is null or false
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        ifExpression(new Reference(BOOLEAN, "a"), new Constant(BOOLEAN, null), TRUE),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                new Logical(OR, ImmutableList.of(new IsNull(new Reference(BOOLEAN, "a")), not(new Reference(BOOLEAN, "a")))),
                                values("a")));

        // always true
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        ifExpression(new Reference(BOOLEAN, "a"), TRUE, TRUE),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                TRUE,
                                values("a")));

        // always false
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        ifExpression(new Reference(BOOLEAN, "a"), FALSE, FALSE),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                FALSE,
                                values("a")));

        // both results equal
        tester().assertThat(new SimplifyFilterPredicate(PLANNER_CONTEXT))
                .on(p -> p.filter(
                        ifExpression(new Reference(BOOLEAN, "a"), new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L)), new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L))),
                        p.values(p.symbol("a", INTEGER), p.symbol("b", INTEGER))))
                .matches(
                        filter(
                                new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L)),
                                values("a", "b")));

        // both results are equal non-deterministic expressions
        Call randomFunction = new Call(
                tester().getMetadata().resolveBuiltinFunction("random", ImmutableList.of()),
                ImmutableList.of());
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        ifExpression(
                                new Reference(BOOLEAN, "a"),
                                new Comparison(EQUAL, randomFunction, new Constant(DOUBLE, 0.0)),
                                new Comparison(EQUAL, randomFunction, new Constant(DOUBLE, 0.0))),
                        p.values(p.symbol("a", INTEGER))))
                .doesNotFire();

        // always null (including the default) -> simplified to FALSE
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        ifExpression(new Reference(BOOLEAN, "a"), new Constant(BOOLEAN, null)),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                FALSE,
                                values("a")));

        // condition is true -> first branch
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        ifExpression(TRUE, new Reference(BOOLEAN, "a"), not(new Reference(BOOLEAN, "a"))),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new Reference(BOOLEAN, "a"),
                                values("a")));

        // condition is true -> second branch
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        ifExpression(FALSE, new Reference(BOOLEAN, "a"), not(new Reference(BOOLEAN, "a"))),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                not(new Reference(BOOLEAN, "a")),
                                values("a")));

        // condition is true, no second branch -> the result is null, simplified to FALSE
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        ifExpression(FALSE, new Reference(BOOLEAN, "a")),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                FALSE,
                                values("a")));

        // not known result (`b`) - cannot optimize
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        ifExpression(new Reference(BOOLEAN, "a"), TRUE, new Reference(BOOLEAN, "b")),
                        p.values(p.symbol("a", INTEGER), p.symbol("b", INTEGER))))
                .doesNotFire();
    }

    @Test
    public void testSimplifyNullIfExpression()
    {
        // NULLIF(x, y) returns true if and only if: x != y AND x = true
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new NullIf(new Reference(BOOLEAN, "a"), new Reference(BOOLEAN, "b")),
                        p.values(p.symbol("a", INTEGER), p.symbol("b", INTEGER))))
                .matches(
                        filter(
                                new Logical(AND, ImmutableList.of(
                                        new Reference(BOOLEAN, "a"),
                                        new Logical(OR, ImmutableList.of(
                                                new IsNull(new Reference(BOOLEAN, "b")),
                                                not(new Reference(BOOLEAN, "b")))))),
                                values("a", "b")));
    }

    @Test
    public void testSimplifySearchedCaseExpression()
    {
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Case(ImmutableList.of(
                                new WhenClause(new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), TRUE),
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), FALSE),
                                new WhenClause(new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), TRUE)),
                                FALSE),
                        p.values(p.symbol("a", INTEGER))))
                .doesNotFire();

        // all results true
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Case(ImmutableList.of(
                                new WhenClause(new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), TRUE),
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), TRUE),
                                new WhenClause(new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), TRUE)),
                                TRUE),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                TRUE,
                                values("a")));

        // all results not true
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Case(ImmutableList.of(
                                new WhenClause(new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), FALSE),
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), new Constant(BOOLEAN, null)),
                                new WhenClause(new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), FALSE)),
                                FALSE),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                FALSE,
                                values("a")));

        // all results not true (including default null result)
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Case(ImmutableList.of(
                                new WhenClause(new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), FALSE),
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), new Constant(BOOLEAN, null)),
                                new WhenClause(new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), FALSE)),
                                NULL_BOOLEAN),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                FALSE,
                                values("a")));

        // one result true, and remaining results not true
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Case(ImmutableList.of(
                                new WhenClause(new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), FALSE),
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), new Constant(BOOLEAN, null)),
                                new WhenClause(new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), TRUE)),
                                FALSE),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new IsNull(new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L))), not(new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L))))), new Logical(OR, ImmutableList.of(new IsNull(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L))), not(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L))))), new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)))),
                                values("a")));

        // first result true, and remaining results not true
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Case(ImmutableList.of(
                                new WhenClause(new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), TRUE),
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), new Constant(BOOLEAN, null)),
                                new WhenClause(new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), FALSE)),
                                FALSE),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)),
                                values("a")));

        // all results not true, and default true
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Case(ImmutableList.of(
                                new WhenClause(new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), FALSE),
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), new Constant(BOOLEAN, null)),
                                new WhenClause(new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L)), FALSE)),
                                TRUE),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                new Logical(AND, ImmutableList.of(
                                        new Logical(OR, ImmutableList.of(
                                                new IsNull(new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L))),
                                                not(new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L))))),
                                        new Logical(OR, ImmutableList.of(
                                                new IsNull(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L))),
                                                not(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L))))),
                                        new Logical(OR, ImmutableList.of(
                                                new IsNull(new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L))),
                                                not(new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 0L))))))),
                                values("a")));

        // all conditions not true - return the default
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Case(ImmutableList.of(
                                new WhenClause(FALSE, new Reference(BOOLEAN, "a")),
                                new WhenClause(FALSE, new Reference(BOOLEAN, "a")),
                                new WhenClause(new Constant(BOOLEAN, null), new Reference(BOOLEAN, "a"))),
                                new Reference(BOOLEAN, "b")),
                        p.values(p.symbol("a", INTEGER), p.symbol("b", INTEGER))))
                .matches(
                        filter(
                                new Reference(BOOLEAN, "b"),
                                values("a", "b")));

        // all conditions not true, no default specified - return false
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Case(ImmutableList.of(
                                new WhenClause(FALSE, new Reference(BOOLEAN, "a")),
                                new WhenClause(FALSE, not(new Reference(BOOLEAN, "a"))),
                                new WhenClause(new Constant(BOOLEAN, null), new Reference(BOOLEAN, "a"))),
                                NULL_BOOLEAN),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                FALSE,
                                values("a")));

        // not true conditions preceding true condition - return the result associated with the true condition
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Case(ImmutableList.of(
                                new WhenClause(FALSE, new Reference(BOOLEAN, "a")),
                                new WhenClause(new Constant(BOOLEAN, null), not(new Reference(BOOLEAN, "a"))),
                                new WhenClause(TRUE, new Reference(BOOLEAN, "b"))),
                                NULL_BOOLEAN),
                        p.values(p.symbol("a", INTEGER), p.symbol("b", INTEGER))))
                .matches(
                        filter(
                                new Reference(BOOLEAN, "b"),
                                values("a", "b")));

        // remove not true condition and move the result associated with the first true condition to default
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Case(ImmutableList.of(
                                new WhenClause(FALSE, new Reference(BOOLEAN, "a")),
                                new WhenClause(new Reference(BOOLEAN, "b"), not(new Reference(BOOLEAN, "a"))),
                                new WhenClause(TRUE, new Reference(BOOLEAN, "b"))),
                                NULL_BOOLEAN),
                        p.values(p.symbol("a", INTEGER), p.symbol("b", INTEGER))))
                .matches(
                        filter(
                                new Case(ImmutableList.of(new WhenClause(new Reference(BOOLEAN, "b"), not(new Reference(BOOLEAN, "a")))), new Reference(BOOLEAN, "b")),
                                values("a", "b")));

        // move the result associated with the first true condition to default
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Case(ImmutableList.of(
                                new WhenClause(new Comparison(LESS_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L)), new Reference(BOOLEAN, "a")),
                                new WhenClause(new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L)), not(new Reference(BOOLEAN, "a"))),
                                new WhenClause(TRUE, new Reference(BOOLEAN, "b")),
                                new WhenClause(TRUE, not(new Reference(BOOLEAN, "b")))),
                                NULL_BOOLEAN),
                        p.values(p.symbol("a", INTEGER), p.symbol("b", INTEGER))))
                .matches(
                        filter(
                                new Case(ImmutableList.of(
                                        new WhenClause(new Comparison(LESS_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L)), new Reference(BOOLEAN, "a")),
                                        new WhenClause(new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L)), not(new Reference(BOOLEAN, "a")))),
                                        new Reference(BOOLEAN, "b")),
                                values("a", "b")));

        // cannot remove any clause
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Case(ImmutableList.of(
                                new WhenClause(new Comparison(LESS_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L)), new Reference(BOOLEAN, "a")),
                                new WhenClause(new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 0L)), not(new Reference(BOOLEAN, "a")))),
                                new Reference(BOOLEAN, "b")),
                        p.values(p.symbol("a", INTEGER), p.symbol("b", INTEGER))))
                .doesNotFire();
    }

    @Test
    public void testSimplifySimpleCaseExpression()
    {
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                                new Switch(
                                        new Reference(BOOLEAN, "a"),
                                        ImmutableList.of(
                                                new WhenClause(new Reference(BOOLEAN, "b"), TRUE),
                                                new WhenClause(new Comparison(EQUAL, new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 1L))), new Constant(INTEGER, 0L)), FALSE)),
                                        TRUE),
                        p.values(p.symbol("a", INTEGER), p.symbol("b", INTEGER))))
                .doesNotFire();

        // comparison with null returns null - no WHEN branch matches, return default value
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Switch(
                                new Constant(BOOLEAN, null),
                                ImmutableList.of(
                                        new WhenClause(new Constant(BOOLEAN, null), TRUE),
                                        new WhenClause(new Reference(BOOLEAN, "a"), FALSE)),
                                new Reference(BOOLEAN, "b")),
                        p.values(p.symbol("a", INTEGER), p.symbol("b", INTEGER))))
                .matches(
                        filter(
                                new Reference(BOOLEAN, "b"),
                                values("a", "b")));

        // comparison with null returns null - no WHEN branch matches, the result is default null, simplified to FALSE
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Switch(
                                new Constant(BOOLEAN, null),
                                ImmutableList.of(
                                        new WhenClause(new Constant(BOOLEAN, null), TRUE),
                                        new WhenClause(new Reference(BOOLEAN, "a"), FALSE)),
                                NULL_BOOLEAN),
                        p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                FALSE,
                                values("a")));

        // all results true
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Switch(
                                new Reference(INTEGER, "a"),
                                ImmutableList.of(
                                        new WhenClause(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 1L))), TRUE),
                                        new WhenClause(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 2L))), TRUE)),
                                TRUE),
                        p.values(p.symbol("a", INTEGER), p.symbol("b", INTEGER))))
                .matches(
                        filter(
                                TRUE,
                                values("a", "b")));

        // all results not true
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Switch(
                                new Reference(INTEGER, "a"),
                                ImmutableList.of(
                                        new WhenClause(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 1L))), FALSE),
                                        new WhenClause(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 2L))), new Constant(BOOLEAN, null))),
                                FALSE),
                        p.values(p.symbol("a", INTEGER), p.symbol("b", INTEGER))))
                .matches(
                        filter(
                                FALSE,
                                values("a", "b")));

        // all results not true (including default null result)
        tester().assertThat(new SimplifyFilterPredicate(FUNCTIONS.getPlannerContext()))
                .on(p -> p.filter(
                        new Switch(
                                new Reference(INTEGER, "a"),
                                ImmutableList.of(
                                        new WhenClause(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 1L))), FALSE),
                                        new WhenClause(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "b"), new Constant(INTEGER, 2L))), new Constant(BOOLEAN, null))),
                                NULL_BOOLEAN),
                        p.values(p.symbol("a", INTEGER), p.symbol("b", INTEGER))))
                .matches(
                        filter(
                                FALSE,
                                values("a", "b")));
    }

    @Test
    public void testSimplifyCaseCompare()
    {
        // CASE WHEN a = 1 THEN 1 ELSE 0 END = 1
        Expression pickLeft1 = new Comparison(
                EQUAL,
                new Case(
                        ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)), new Constant(INTEGER, 1L))),
                        new Constant(INTEGER, 0L)),
                new Constant(INTEGER, 1L));

        // CASE WHEN a = 1 THEN 1 ELSE 0 END <> 0
        Expression pickLeft2 = new Comparison(
                Comparison.Operator.NOT_EQUAL,
                new Case(
                        ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)), new Constant(INTEGER, 1L))),
                        new Constant(INTEGER, 0L)),
                new Constant(INTEGER, 0L));

        Expression pickLeftExpected = new Case(
                ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)), TRUE)),
                FALSE);

        tester().assertThat(new SimplifyFilterPredicate(PLANNER_CONTEXT))
                .on(p -> p.filter(pickLeft1, p.values(p.symbol("a", INTEGER))))
                .matches(filter(pickLeftExpected, values("a")));

        tester().assertThat(new SimplifyFilterPredicate(PLANNER_CONTEXT))
                .on(p -> p.filter(pickLeft2, p.values(p.symbol("a", INTEGER))))
                .matches(filter(pickLeftExpected, values("a")));

        tester().assertThat(new SimplifyFilterPredicate(PLANNER_CONTEXT))
                .on(p -> p.filter(pickLeftExpected, p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)),
                                values("a")));

        // CASE WHEN a = 1 THEN 1 ELSE 0 END = 0
        Expression pickRight1 = new Comparison(
                EQUAL,
                new Case(
                        ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)), new Constant(INTEGER, 1L))),
                        new Constant(INTEGER, 0L)),
                new Constant(INTEGER, 0L));

        // CASE WHEN a = 1 THEN 1 ELSE 0 END <> 1
        Expression pickRight2 = new Comparison(
                Comparison.Operator.NOT_EQUAL,
                new Case(
                        ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)), new Constant(INTEGER, 1L))),
                        new Constant(INTEGER, 0L)),
                new Constant(INTEGER, 1L));

        Expression pickRightExpected = new Case(
                ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)), FALSE)),
                TRUE);

        tester().assertThat(new SimplifyFilterPredicate(PLANNER_CONTEXT))
                .on(p -> p.filter(pickRight1, p.values(p.symbol("a", INTEGER))))
                .matches(filter(pickRightExpected, values("a")));

        tester().assertThat(new SimplifyFilterPredicate(PLANNER_CONTEXT))
                .on(p -> p.filter(pickRight2, p.values(p.symbol("a", INTEGER))))
                .matches(filter(pickRightExpected, values("a")));

        tester().assertThat(new SimplifyFilterPredicate(PLANNER_CONTEXT))
                .on(p -> p.filter(pickRightExpected, p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                new Logical(OR,
                                        ImmutableList.of(
                                                new IsNull(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L))),
                                                not(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L))))),
                                values("a")));

        // CASE WHEN a = 1 THEN 1 WHEN a = 2 THEN 2 WHEN a = 3 THEN 3 WHEN a = 4 THEN 4 ELSE 0 END = 1
        Expression multipleEqualsFirst = new Comparison(
                EQUAL,
                new Case(
                        ImmutableList.of(
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)), new Constant(INTEGER, 1L)),
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 2L)), new Constant(INTEGER, 2L)),
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 3L)), new Constant(INTEGER, 3L)),
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 4L)), new Constant(INTEGER, 4L))),
                        new Constant(INTEGER, 0L)),
                new Constant(INTEGER, 1L));

        Expression multipleEqualsFirstExpected = new Case(
                ImmutableList.of(
                        new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)), TRUE),
                        new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 2L)), FALSE),
                        new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 3L)), FALSE),
                        new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 4L)), FALSE)),
                FALSE);

        tester().assertThat(new SimplifyFilterPredicate(PLANNER_CONTEXT))
                .on(p -> p.filter(multipleEqualsFirst, p.values(p.symbol("a", INTEGER))))
                .matches(filter(multipleEqualsFirstExpected, values("a")));

        tester().assertThat(new SimplifyFilterPredicate(PLANNER_CONTEXT))
                .on(p -> p.filter(multipleEqualsFirstExpected, p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)),
                                values("a")));

        // CASE WHEN a = 1 THEN 1 WHEN a = 2 THEN 2 WHEN a = 3 THEN 3 WHEN a = 4 THEN 4 ELSE 0 END = 2
        Expression multipleMatchesSecond = new Comparison(
                EQUAL,
                new Case(
                        ImmutableList.of(
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)), new Constant(INTEGER, 1L)),
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 2L)), new Constant(INTEGER, 2L)),
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 3L)), new Constant(INTEGER, 3L)),
                                new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 4L)), new Constant(INTEGER, 4L))),
                        new Constant(INTEGER, 0L)),
                new Constant(INTEGER, 2L));

        Expression multipleMatchesSecondExpected = new Case(
                ImmutableList.of(
                        new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)), FALSE),
                        new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 2L)), TRUE),
                        new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 3L)), FALSE),
                        new WhenClause(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 4L)), FALSE)),
                FALSE);

        tester().assertThat(new SimplifyFilterPredicate(PLANNER_CONTEXT))
                .on(p -> p.filter(multipleMatchesSecond, p.values(p.symbol("a", INTEGER))))
                .matches(filter(multipleMatchesSecondExpected, values("a")));

        tester().assertThat(new SimplifyFilterPredicate(PLANNER_CONTEXT))
                .on(p -> p.filter(multipleMatchesSecondExpected, p.values(p.symbol("a", INTEGER))))
                .matches(
                        filter(
                                new Logical(AND,
                                        ImmutableList.of(
                                                new Logical(OR,
                                                        ImmutableList.of(
                                                                new IsNull(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L))),
                                                                not(new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L))))),
                                                new Comparison(EQUAL, new Reference(INTEGER, "a"), new Constant(INTEGER, 2L)))),
                                values("a")));
    }

    @Test
    public void testNestedCareCompare()
    {
        Expression expression = new Comparison(
                NOT_EQUAL,
                new Case(
                        ImmutableList.of(
                                new WhenClause(new IsNull(new Reference(BIGINT, "a")), new Constant(BIGINT, 0L))),
                        new Case(
                                ImmutableList.of(
                                        new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1001L)), new Constant(BIGINT, 1L))),
                                new Constant(BIGINT, 0L))),
                new Constant(BIGINT, 0L));

        Expression stage1 = new Case(
                ImmutableList.of(
                        new WhenClause(new IsNull(new Reference(BIGINT, "a")), FALSE)),
                new Case(
                        ImmutableList.of(
                                new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1001L)), TRUE)),
                        FALSE));

        Expression stage2 = new Logical(AND,
                ImmutableList.of(
                        not(new IsNull(new Reference(BIGINT, "a"))),
                        new Comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1001L))));

        tester().assertThat(new SimplifyFilterPredicate(PLANNER_CONTEXT))
                .on(p -> p.filter(expression, p.values(p.symbol("a", BIGINT))))
                .matches(filter(stage1, values("a")));

        tester().assertThat(new SimplifyFilterPredicate(PLANNER_CONTEXT))
                .on(p -> p.filter(stage1, p.values(p.symbol("a", BIGINT))))
                .matches(filter(stage2, values("a")));
    }

    private static Expression not(Expression expression)
    {
        return IrExpressions.not(FUNCTIONS.getMetadata(), expression);
    }
}
