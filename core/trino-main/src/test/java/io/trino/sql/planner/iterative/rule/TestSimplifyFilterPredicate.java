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
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FunctionCall;
import io.trino.sql.ir.IsNullPredicate;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.NotExpression;
import io.trino.sql.ir.NullIfExpression;
import io.trino.sql.ir.SearchedCaseExpression;
import io.trino.sql.ir.SimpleCaseExpression;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.type.UnknownType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.LogicalExpression.Operator.AND;
import static io.trino.sql.ir.LogicalExpression.Operator.OR;
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
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        ifExpression(new SymbolReference("a"), TRUE_LITERAL, FALSE_LITERAL),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new SymbolReference("a"),
                                values("a")));

        // true result iff the condition is true
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        ifExpression(new SymbolReference("a"), TRUE_LITERAL, new Constant(UnknownType.UNKNOWN, null)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new SymbolReference("a"),
                                values("a")));

        // true result iff the condition is null or false
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        ifExpression(new SymbolReference("a"), FALSE_LITERAL, TRUE_LITERAL),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new SymbolReference("a")))),
                                values("a")));

        // true result iff the condition is null or false
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        ifExpression(new SymbolReference("a"), new Constant(UnknownType.UNKNOWN, null), TRUE_LITERAL),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new SymbolReference("a")))),
                                values("a")));

        // always true
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        ifExpression(new SymbolReference("a"), TRUE_LITERAL, TRUE_LITERAL),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                TRUE_LITERAL,
                                values("a")));

        // always false
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        ifExpression(new SymbolReference("a"), FALSE_LITERAL, FALSE_LITERAL),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));

        // both results equal
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        ifExpression(new SymbolReference("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new Constant(INTEGER, 0L)), new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new Constant(INTEGER, 0L))),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new Constant(INTEGER, 0L)),
                                values("a", "b")));

        // both results are equal non-deterministic expressions
        FunctionCall randomFunction = new FunctionCall(
                tester().getMetadata().resolveBuiltinFunction("random", ImmutableList.of()),
                ImmutableList.of());
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        ifExpression(
                                new SymbolReference("a"),
                                new ComparisonExpression(EQUAL, randomFunction, new Constant(INTEGER, 0L)),
                                new ComparisonExpression(EQUAL, randomFunction, new Constant(INTEGER, 0L))),
                        p.values(p.symbol("a"))))
                .doesNotFire();

        // always null (including the default) -> simplified to FALSE
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        ifExpression(new SymbolReference("a"), new Constant(UnknownType.UNKNOWN, null)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));

        // condition is true -> first branch
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        ifExpression(TRUE_LITERAL, new SymbolReference("a"), new NotExpression(new SymbolReference("a"))),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new SymbolReference("a"),
                                values("a")));

        // condition is true -> second branch
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        ifExpression(FALSE_LITERAL, new SymbolReference("a"), new NotExpression(new SymbolReference("a"))),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new NotExpression(new SymbolReference("a")),
                                values("a")));

        // condition is true, no second branch -> the result is null, simplified to FALSE
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        ifExpression(FALSE_LITERAL, new SymbolReference("a")),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));

        // not known result (`b`) - cannot optimize
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        ifExpression(new SymbolReference("a"), TRUE_LITERAL, new SymbolReference("b")),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testSimplifyNullIfExpression()
    {
        // NULLIF(x, y) returns true if and only if: x != y AND x = true
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new NullIfExpression(new SymbolReference("a"), new SymbolReference("b")),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                new LogicalExpression(AND, ImmutableList.of(
                                        new SymbolReference("a"),
                                        new LogicalExpression(OR, ImmutableList.of(
                                                new IsNullPredicate(new SymbolReference("b")),
                                                new NotExpression(new SymbolReference("b")))))),
                                values("a", "b")));
    }

    @Test
    public void testSimplifySearchedCaseExpression()
    {
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)), TRUE_LITERAL),
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), new Constant(INTEGER, 0L)), FALSE_LITERAL),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)), TRUE_LITERAL)),
                                Optional.of(FALSE_LITERAL)),
                        p.values(p.symbol("a"))))
                .doesNotFire();

        // all results true
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)), TRUE_LITERAL),
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), new Constant(INTEGER, 0L)), TRUE_LITERAL),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)), TRUE_LITERAL)),
                                Optional.of(TRUE_LITERAL)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                TRUE_LITERAL,
                                values("a")));

        // all results not true
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)), FALSE_LITERAL),
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), new Constant(INTEGER, 0L)), new Constant(UnknownType.UNKNOWN, null)),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)), FALSE_LITERAL)),
                                Optional.of(FALSE_LITERAL)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));

        // all results not true (including default null result)
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)), FALSE_LITERAL),
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), new Constant(INTEGER, 0L)), new Constant(UnknownType.UNKNOWN, null)),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)), FALSE_LITERAL)),
                                Optional.empty()),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));

        // one result true, and remaining results not true
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)), FALSE_LITERAL),
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), new Constant(INTEGER, 0L)), new Constant(UnknownType.UNKNOWN, null)),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)), TRUE_LITERAL)),
                                Optional.of(FALSE_LITERAL)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L))), new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L))))), new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new ComparisonExpression(EQUAL, new SymbolReference("a"), new Constant(INTEGER, 0L))), new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference("a"), new Constant(INTEGER, 0L))))), new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)))),
                                values("a")));

        // first result true, and remaining results not true
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)), TRUE_LITERAL),
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), new Constant(INTEGER, 0L)), new Constant(UnknownType.UNKNOWN, null)),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)), FALSE_LITERAL)),
                                Optional.of(FALSE_LITERAL)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)),
                                values("a")));

        // all results not true, and default true
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)), FALSE_LITERAL),
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), new Constant(INTEGER, 0L)), new Constant(UnknownType.UNKNOWN, null)),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L)), FALSE_LITERAL)),
                                Optional.of(TRUE_LITERAL)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new LogicalExpression(AND, ImmutableList.of(
                                        new LogicalExpression(OR, ImmutableList.of(
                                                new IsNullPredicate(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L))),
                                                new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L))))),
                                        new LogicalExpression(OR, ImmutableList.of(
                                                new IsNullPredicate(new ComparisonExpression(EQUAL, new SymbolReference("a"), new Constant(INTEGER, 0L))),
                                                new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference("a"), new Constant(INTEGER, 0L))))),
                                        new LogicalExpression(OR, ImmutableList.of(
                                                new IsNullPredicate(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L))),
                                                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new Constant(INTEGER, 0L))))))),
                                values("a")));

        // all conditions not true - return the default
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(FALSE_LITERAL, new SymbolReference("a")),
                                new WhenClause(FALSE_LITERAL, new SymbolReference("a")),
                                new WhenClause(new Constant(UnknownType.UNKNOWN, null), new SymbolReference("a"))),
                                Optional.of(new SymbolReference("b"))),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                new SymbolReference("b"),
                                values("a", "b")));

        // all conditions not true, no default specified - return false
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(FALSE_LITERAL, new SymbolReference("a")),
                                new WhenClause(FALSE_LITERAL, new NotExpression(new SymbolReference("a"))),
                                new WhenClause(new Constant(UnknownType.UNKNOWN, null), new SymbolReference("a"))),
                                Optional.empty()),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));

        // not true conditions preceding true condition - return the result associated with the true condition
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(FALSE_LITERAL, new SymbolReference("a")),
                                new WhenClause(new Constant(UnknownType.UNKNOWN, null), new NotExpression(new SymbolReference("a"))),
                                new WhenClause(TRUE_LITERAL, new SymbolReference("b"))),
                                Optional.empty()),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                new SymbolReference("b"),
                                values("a", "b")));

        // remove not true condition and move the result associated with the first true condition to default
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(FALSE_LITERAL, new SymbolReference("a")),
                                new WhenClause(new SymbolReference("b"), new NotExpression(new SymbolReference("a"))),
                                new WhenClause(TRUE_LITERAL, new SymbolReference("b"))),
                                Optional.empty()),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                new SearchedCaseExpression(ImmutableList.of(new WhenClause(new SymbolReference("b"), new NotExpression(new SymbolReference("a")))), Optional.of(new SymbolReference("b"))),
                                values("a", "b")));

        // move the result associated with the first true condition to default
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("b"), new Constant(INTEGER, 0L)), new SymbolReference("a")),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new Constant(INTEGER, 0L)), new NotExpression(new SymbolReference("a"))),
                                new WhenClause(TRUE_LITERAL, new SymbolReference("b")),
                                new WhenClause(TRUE_LITERAL, new NotExpression(new SymbolReference("b")))),
                                Optional.empty()),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                new SearchedCaseExpression(ImmutableList.of(
                                        new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("b"), new Constant(INTEGER, 0L)), new SymbolReference("a")),
                                        new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new Constant(INTEGER, 0L)), new NotExpression(new SymbolReference("a")))),
                                        Optional.of(new SymbolReference("b"))),
                                values("a", "b")));

        // cannot remove any clause
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("b"), new Constant(INTEGER, 0L)), new SymbolReference("a")),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new Constant(INTEGER, 0L)), new NotExpression(new SymbolReference("a")))),
                                Optional.of(new SymbolReference("b"))),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testSimplifySimpleCaseExpression()
    {
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                                new SimpleCaseExpression(
                                        new SymbolReference("a"),
                                        ImmutableList.of(
                                                new WhenClause(new SymbolReference("b"), TRUE_LITERAL),
                                                new WhenClause(new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new SymbolReference("b"), new Constant(INTEGER, 1L)), FALSE_LITERAL)),
                                        Optional.of(TRUE_LITERAL)),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .doesNotFire();

        // comparison with null returns null - no WHEN branch matches, return default value
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SimpleCaseExpression(
                                new Constant(UnknownType.UNKNOWN, null),
                                ImmutableList.of(
                                        new WhenClause(new Constant(UnknownType.UNKNOWN, null), TRUE_LITERAL),
                                        new WhenClause(new SymbolReference("a"), FALSE_LITERAL)),
                                Optional.of(new SymbolReference("b"))),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                new SymbolReference("b"),
                                values("a", "b")));

        // comparison with null returns null - no WHEN branch matches, the result is default null, simplified to FALSE
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SimpleCaseExpression(
                                new Constant(UnknownType.UNKNOWN, null),
                                ImmutableList.of(
                                        new WhenClause(new Constant(UnknownType.UNKNOWN, null), TRUE_LITERAL),
                                        new WhenClause(new SymbolReference("a"), FALSE_LITERAL)),
                                Optional.empty()),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));

        // all results true
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SimpleCaseExpression(
                                new SymbolReference("a"),
                                ImmutableList.of(
                                        new WhenClause(new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new SymbolReference("b"), new Constant(INTEGER, 1L)), TRUE_LITERAL),
                                        new WhenClause(new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new SymbolReference("b"), new Constant(INTEGER, 2L)), TRUE_LITERAL)),
                                Optional.of(TRUE_LITERAL)),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                TRUE_LITERAL,
                                values("a", "b")));

        // all results not true
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SimpleCaseExpression(
                                new SymbolReference("a"),
                                ImmutableList.of(
                                        new WhenClause(new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new SymbolReference("b"), new Constant(INTEGER, 1L)), FALSE_LITERAL),
                                        new WhenClause(new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new SymbolReference("b"), new Constant(INTEGER, 2L)), new Constant(UnknownType.UNKNOWN, null))),
                                Optional.of(FALSE_LITERAL)),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a", "b")));

        // all results not true (including default null result)
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        new SimpleCaseExpression(
                                new SymbolReference("a"),
                                ImmutableList.of(
                                        new WhenClause(new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new SymbolReference("b"), new Constant(INTEGER, 1L)), FALSE_LITERAL),
                                        new WhenClause(new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new SymbolReference("b"), new Constant(INTEGER, 2L)), new Constant(UnknownType.UNKNOWN, null))),
                                Optional.empty()),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a", "b")));
    }

    @Test
    public void testCastNull()
    {
        tester().assertThat(new SimplifyFilterPredicate())
                .on(p -> p.filter(
                        ifExpression(new SymbolReference("a"), new Cast(new Cast(new Constant(BOOLEAN, null), BIGINT), BOOLEAN), FALSE_LITERAL),
                        p.values(p.symbol("a", BOOLEAN))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));
    }
}
