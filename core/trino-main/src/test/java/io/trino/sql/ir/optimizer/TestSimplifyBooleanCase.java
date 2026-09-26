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
package io.trino.sql.ir.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.rule.SimplifyBooleanCase;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimplifyBooleanCase
{
    private static final ResolvedFunction DIVIDE_BIGINT = PLANNER_CONTEXT.getMetadata().resolveOperator(DIVIDE, ImmutableList.of(BIGINT, BIGINT));
    private static final Reference X = new Reference(BOOLEAN, "x");
    private static final Expression PREDICATE = comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L));

    @Test
    void testConstantDefault()
    {
        // Case([When(c, r)], false) -> And($identical(c, true), r)
        assertThat(optimize(new Case(ImmutableList.of(new WhenClause(X, PREDICATE)), FALSE)))
                .isEqualTo(Optional.of(and(
                        comparison(IDENTICAL, X, TRUE),
                        PREDICATE)));

        // Case([When(c, r)], true) -> Or($not($identical(c, true)), r)
        assertThat(optimize(new Case(ImmutableList.of(new WhenClause(X, PREDICATE)), TRUE)))
                .isEqualTo(Optional.of(or(
                        not(PLANNER_CONTEXT.getMetadata(), comparison(IDENTICAL, X, TRUE)),
                        PREDICATE)));
    }

    @Test
    void testConstantResult()
    {
        // Case([When(c, true)], d) -> Or($identical(c, true), d)
        assertThat(optimize(new Case(ImmutableList.of(new WhenClause(X, TRUE)), PREDICATE)))
                .isEqualTo(Optional.of(or(
                        comparison(IDENTICAL, X, TRUE),
                        PREDICATE)));

        // Case([When(c, false)], d) -> And($not($identical(c, true)), d)
        assertThat(optimize(new Case(ImmutableList.of(new WhenClause(X, FALSE)), PREDICATE)))
                .isEqualTo(Optional.of(and(
                        not(PLANNER_CONTEXT.getMetadata(), comparison(IDENTICAL, X, TRUE)),
                        PREDICATE)));
    }

    @Test
    void testImplicitNullDefault()
    {
        // A CASE with no ELSE carries a null default. Logical AND/OR reproduce its null semantics,
        // so the rewrite still applies.
        assertThat(optimize(new Case(ImmutableList.of(new WhenClause(X, TRUE)), NULL_BOOLEAN)))
                .isEqualTo(Optional.of(or(
                        comparison(IDENTICAL, X, TRUE),
                        NULL_BOOLEAN)));

        assertThat(optimize(new Case(ImmutableList.of(new WhenClause(X, PREDICATE)), NULL_BOOLEAN)))
                .isEmpty(); // neither side is a TRUE/FALSE constant
    }

    @Test
    void testDoesNotApply()
    {
        // Both branches constant: SimplifyRedundantCase handles this.
        assertThat(optimize(new Case(ImmutableList.of(new WhenClause(X, TRUE)), FALSE)))
                .isEmpty();

        // Neither branch constant: nothing to dissolve.
        assertThat(optimize(new Case(ImmutableList.of(new WhenClause(X, PREDICATE)), new Reference(BOOLEAN, "y"))))
                .isEmpty();

        // More than one WHEN clause: expanding would duplicate the negated conditions.
        assertThat(optimize(new Case(
                ImmutableList.of(new WhenClause(X, PREDICATE), new WhenClause(new Reference(BOOLEAN, "y"), PREDICATE)),
                FALSE)))
                .isEmpty();

        // Non-boolean CASE.
        assertThat(optimize(new Case(
                ImmutableList.of(new WhenClause(X, new Constant(BIGINT, 1L))),
                new Constant(BIGINT, 2L))))
                .isEmpty();
    }

    @Test
    void testBranchMayFail()
    {
        // The CASE guards the division against a zero divisor. Lifting it out of the CASE would make it
        // unconditional, so the rewrite must not apply.
        Expression divideByB = comparison(
                EQUAL,
                new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))),
                new Constant(BIGINT, 1L));

        assertThat(optimize(new Case(
                ImmutableList.of(new WhenClause(comparison(EQUAL, new Reference(BIGINT, "b"), new Constant(BIGINT, 0L)), FALSE)),
                divideByB)))
                .isEmpty();

        assertThat(optimize(new Case(
                ImmutableList.of(new WhenClause(X, divideByB)),
                FALSE)))
                .isEmpty();
    }

    @Test
    void testConditionMayFail()
    {
        Expression failingCondition = comparison(
                EQUAL,
                new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))),
                new Constant(BIGINT, 1L));

        // The And forms split into conjuncts the planner places independently, so a failing condition
        // could descend to rows the intact Case never saw. The rewrite must not apply.
        assertThat(optimize(new Case(ImmutableList.of(new WhenClause(failingCondition, PREDICATE)), FALSE)))
                .isEmpty();
        assertThat(optimize(new Case(ImmutableList.of(new WhenClause(failingCondition, FALSE)), PREDICATE)))
                .isEmpty();

        // The Or forms stay a single conjunct with the same symbols, evaluated exactly where the Case
        // was, and the Case evaluated its condition unconditionally there anyway.
        assertThat(optimize(new Case(ImmutableList.of(new WhenClause(failingCondition, PREDICATE)), TRUE)))
                .isEqualTo(Optional.of(or(
                        not(PLANNER_CONTEXT.getMetadata(), comparison(IDENTICAL, failingCondition, TRUE)),
                        PREDICATE)));
        assertThat(optimize(new Case(ImmutableList.of(new WhenClause(failingCondition, TRUE)), PREDICATE)))
                .isEqualTo(Optional.of(or(
                        comparison(IDENTICAL, failingCondition, TRUE),
                        PREDICATE)));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new SimplifyBooleanCase(PLANNER_CONTEXT).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }
}
