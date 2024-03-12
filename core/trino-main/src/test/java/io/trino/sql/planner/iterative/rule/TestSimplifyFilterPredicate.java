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
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.WhenClause;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static io.trino.sql.tree.LogicalExpression.Operator.OR;

public class TestSimplifyFilterPredicate
        extends BaseRuleTest
{
    @Test
    public void testSimplifyIfExpression()
    {
        // true result iff the condition is true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(new SymbolReference("a"), TRUE_LITERAL, FALSE_LITERAL),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new SymbolReference("a"),
                                values("a")));

        // true result iff the condition is true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(new SymbolReference("a"), TRUE_LITERAL, new NullLiteral()),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new SymbolReference("a"),
                                values("a")));

        // true result iff the condition is null or false
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(new SymbolReference("a"), FALSE_LITERAL, TRUE_LITERAL),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new SymbolReference("a")))),
                                values("a")));

        // true result iff the condition is null or false
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(new SymbolReference("a"), new NullLiteral(), TRUE_LITERAL),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new SymbolReference("a")), new NotExpression(new SymbolReference("a")))),
                                values("a")));

        // always true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(new SymbolReference("a"), TRUE_LITERAL, TRUE_LITERAL),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                TRUE_LITERAL,
                                values("a")));

        // always false
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(new SymbolReference("a"), FALSE_LITERAL, FALSE_LITERAL),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));

        // both results equal
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(new SymbolReference("a"), new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("0")), new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("0"))),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("0")),
                                values("a", "b")));

        // both results are equal non-deterministic expressions
        FunctionCall randomFunction = new FunctionCall(
                tester().getMetadata().resolveBuiltinFunction("random", ImmutableList.of()).toQualifiedName(),
                ImmutableList.of());
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(
                                new SymbolReference("a"),
                                new ComparisonExpression(EQUAL, randomFunction, new LongLiteral("0")),
                                new ComparisonExpression(EQUAL, randomFunction, new LongLiteral("0"))),
                        p.values(p.symbol("a"))))
                .doesNotFire();

        // always null (including the default) -> simplified to FALSE
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(new SymbolReference("a"), new NullLiteral()),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));

        // condition is true -> first branch
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(TRUE_LITERAL, new SymbolReference("a"), new NotExpression(new SymbolReference("a"))),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new SymbolReference("a"),
                                values("a")));

        // condition is true -> second branch
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(FALSE_LITERAL, new SymbolReference("a"), new NotExpression(new SymbolReference("a"))),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new NotExpression(new SymbolReference("a")),
                                values("a")));

        // condition is true, no second branch -> the result is null, simplified to FALSE
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(FALSE_LITERAL, new SymbolReference("a")),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));

        // not known result (`b`) - cannot optimize
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(new SymbolReference("a"), TRUE_LITERAL, new SymbolReference("b")),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testSimplifyNullIfExpression()
    {
        // NULLIF(x, y) returns true if and only if: x != y AND x = true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
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
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new LongLiteral("0")), TRUE_LITERAL),
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), new LongLiteral("0")), FALSE_LITERAL),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("0")), TRUE_LITERAL)),
                                Optional.of(FALSE_LITERAL)),
                        p.values(p.symbol("a"))))
                .doesNotFire();

        // all results true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new LongLiteral("0")), TRUE_LITERAL),
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), new LongLiteral("0")), TRUE_LITERAL),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("0")), TRUE_LITERAL)),
                                Optional.of(TRUE_LITERAL)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                TRUE_LITERAL,
                                values("a")));

        // all results not true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new LongLiteral("0")), FALSE_LITERAL),
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), new LongLiteral("0")), new NullLiteral()),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("0")), FALSE_LITERAL)),
                                Optional.of(FALSE_LITERAL)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));

        // all results not true (including default null result)
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new LongLiteral("0")), FALSE_LITERAL),
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), new LongLiteral("0")), new NullLiteral()),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("0")), FALSE_LITERAL)),
                                Optional.empty()),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));

        // one result true, and remaining results not true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new LongLiteral("0")), FALSE_LITERAL),
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), new LongLiteral("0")), new NullLiteral()),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("0")), TRUE_LITERAL)),
                                Optional.of(FALSE_LITERAL)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new LongLiteral("0"))), new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new LongLiteral("0"))))), new LogicalExpression(OR, ImmutableList.of(new IsNullPredicate(new ComparisonExpression(EQUAL, new SymbolReference("a"), new LongLiteral("0"))), new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference("a"), new LongLiteral("0"))))), new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("0")))),
                                values("a")));

        // first result true, and remaining results not true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new LongLiteral("0")), TRUE_LITERAL),
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), new LongLiteral("0")), new NullLiteral()),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("0")), FALSE_LITERAL)),
                                Optional.of(FALSE_LITERAL)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new LongLiteral("0")),
                                values("a")));

        // all results not true, and default true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new LongLiteral("0")), FALSE_LITERAL),
                                new WhenClause(new ComparisonExpression(EQUAL, new SymbolReference("a"), new LongLiteral("0")), new NullLiteral()),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("0")), FALSE_LITERAL)),
                                Optional.of(TRUE_LITERAL)),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                new LogicalExpression(AND, ImmutableList.of(
                                        new LogicalExpression(OR, ImmutableList.of(
                                                new IsNullPredicate(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new LongLiteral("0"))),
                                                new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("a"), new LongLiteral("0"))))),
                                        new LogicalExpression(OR, ImmutableList.of(
                                                new IsNullPredicate(new ComparisonExpression(EQUAL, new SymbolReference("a"), new LongLiteral("0"))),
                                                new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference("a"), new LongLiteral("0"))))),
                                        new LogicalExpression(OR, ImmutableList.of(
                                                new IsNullPredicate(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("0"))),
                                                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("a"), new LongLiteral("0"))))))),
                                values("a")));

        // all conditions not true - return the default
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(FALSE_LITERAL, new SymbolReference("a")),
                                new WhenClause(FALSE_LITERAL, new SymbolReference("a")),
                                new WhenClause(new NullLiteral(), new SymbolReference("a"))),
                                Optional.of(new SymbolReference("b"))),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                new SymbolReference("b"),
                                values("a", "b")));

        // all conditions not true, no default specified - return false
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(FALSE_LITERAL, new SymbolReference("a")),
                                new WhenClause(FALSE_LITERAL, new NotExpression(new SymbolReference("a"))),
                                new WhenClause(new NullLiteral(), new SymbolReference("a"))),
                                Optional.empty()),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));

        // not true conditions preceding true condition - return the result associated with the true condition
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(FALSE_LITERAL, new SymbolReference("a")),
                                new WhenClause(new NullLiteral(), new NotExpression(new SymbolReference("a"))),
                                new WhenClause(TRUE_LITERAL, new SymbolReference("b"))),
                                Optional.empty()),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                new SymbolReference("b"),
                                values("a", "b")));

        // remove not true condition and move the result associated with the first true condition to default
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
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
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("b"), new LongLiteral("0")), new SymbolReference("a")),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("0")), new NotExpression(new SymbolReference("a"))),
                                new WhenClause(TRUE_LITERAL, new SymbolReference("b")),
                                new WhenClause(TRUE_LITERAL, new NotExpression(new SymbolReference("b")))),
                                Optional.empty()),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                new SearchedCaseExpression(ImmutableList.of(
                                        new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("b"), new LongLiteral("0")), new SymbolReference("a")),
                                        new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("0")), new NotExpression(new SymbolReference("a")))),
                                        Optional.of(new SymbolReference("b"))),
                                values("a", "b")));

        // cannot remove any clause
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SearchedCaseExpression(ImmutableList.of(
                                new WhenClause(new ComparisonExpression(LESS_THAN, new SymbolReference("b"), new LongLiteral("0")), new SymbolReference("a")),
                                new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("b"), new LongLiteral("0")), new NotExpression(new SymbolReference("a")))),
                                Optional.of(new SymbolReference("b"))),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .doesNotFire();
    }

    @Test
    public void testSimplifySimpleCaseExpression()
    {
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                                new SimpleCaseExpression(
                                        new SymbolReference("a"),
                                        ImmutableList.of(
                                                new WhenClause(new SymbolReference("b"), TRUE_LITERAL),
                                                new WhenClause(new ArithmeticBinaryExpression(ADD, new SymbolReference("b"), new LongLiteral("1")), FALSE_LITERAL)),
                                        Optional.of(TRUE_LITERAL)),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .doesNotFire();

        // comparison with null returns null - no WHEN branch matches, return default value
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SimpleCaseExpression(
                                new NullLiteral(),
                                ImmutableList.of(
                                        new WhenClause(new NullLiteral(), TRUE_LITERAL),
                                        new WhenClause(new SymbolReference("a"), FALSE_LITERAL)),
                                Optional.of(new SymbolReference("b"))),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                new SymbolReference("b"),
                                values("a", "b")));

        // comparison with null returns null - no WHEN branch matches, the result is default null, simplified to FALSE
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SimpleCaseExpression(
                                new NullLiteral(),
                                ImmutableList.of(
                                        new WhenClause(new NullLiteral(), TRUE_LITERAL),
                                        new WhenClause(new SymbolReference("a"), FALSE_LITERAL)),
                                Optional.empty()),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));

        // all results true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SimpleCaseExpression(
                                new SymbolReference("a"),
                                ImmutableList.of(
                                        new WhenClause(new ArithmeticBinaryExpression(ADD, new SymbolReference("b"), new LongLiteral("1")), TRUE_LITERAL),
                                        new WhenClause(new ArithmeticBinaryExpression(ADD, new SymbolReference("b"), new LongLiteral("2")), TRUE_LITERAL)),
                                Optional.of(TRUE_LITERAL)),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                TRUE_LITERAL,
                                values("a", "b")));

        // all results not true
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SimpleCaseExpression(
                                new SymbolReference("a"),
                                ImmutableList.of(
                                        new WhenClause(new ArithmeticBinaryExpression(ADD, new SymbolReference("b"), new LongLiteral("1")), FALSE_LITERAL),
                                        new WhenClause(new ArithmeticBinaryExpression(ADD, new SymbolReference("b"), new LongLiteral("2")), new NullLiteral())),
                                Optional.of(FALSE_LITERAL)),
                        p.values(p.symbol("a"), p.symbol("b"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a", "b")));

        // all results not true (including default null result)
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new SimpleCaseExpression(
                                new SymbolReference("a"),
                                ImmutableList.of(
                                        new WhenClause(new ArithmeticBinaryExpression(ADD, new SymbolReference("b"), new LongLiteral("1")), FALSE_LITERAL),
                                        new WhenClause(new ArithmeticBinaryExpression(ADD, new SymbolReference("b"), new LongLiteral("2")), new NullLiteral())),
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
        tester().assertThat(new SimplifyFilterPredicate(tester().getMetadata()))
                .on(p -> p.filter(
                        new IfExpression(new SymbolReference("a"), new Cast(new Cast(new Cast(new NullLiteral(), dataType("boolean")), dataType("bigint")), dataType("boolean")), FALSE_LITERAL),
                        p.values(p.symbol("a"))))
                .matches(
                        filter(
                                FALSE_LITERAL,
                                values("a")));
    }
}
