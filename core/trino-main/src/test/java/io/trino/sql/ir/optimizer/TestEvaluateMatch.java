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
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.EvaluateMatch;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluateMatch
{
    @Test
    void test()
    {
        assertThat(optimize(
                new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(equalityClause(new Constant(BIGINT, 1L), new Reference(VARCHAR, "a"))),
                        new Reference(VARCHAR, "b"))))
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Match(
                        new Constant(BIGINT, 1L),
                        ImmutableList.of(equalityClause(new Constant(BIGINT, 1L), new Reference(VARCHAR, "a"))),
                        new Reference(VARCHAR, "b"))))
                .describedAs("match")
                .isEqualTo(Optional.of(new Reference(VARCHAR, "a")));

        assertThat(optimize(
                new Match(
                        new Constant(BIGINT, 1L),
                        ImmutableList.of(equalityClause(new Constant(BIGINT, 2L), new Reference(VARCHAR, "a"))),
                        new Reference(VARCHAR, "b"))))
                .describedAs("no match")
                .isEqualTo(Optional.of(new Reference(VARCHAR, "b")));

        // A clause whose Bind captures a non-constant outer reference must not be evaluated even
        // when the operand is constant: the body depends on a value unknown at planning time.
        Symbol capturedX = new Symbol(BIGINT, "captured_x");
        Symbol parameter = new Symbol(BIGINT, "p");
        MatchClause clauseWithCapture = new MatchClause(
                new Bind(
                        ImmutableList.of(new Reference(BIGINT, "x")),
                        new Lambda(
                                ImmutableList.of(capturedX, parameter),
                                comparison(EQUAL, new Reference(BIGINT, parameter.name()), new Reference(BIGINT, capturedX.name())))),
                new Reference(VARCHAR, "a"));
        assertThat(optimize(
                new Match(
                        new Constant(BIGINT, 1L),
                        ImmutableList.of(clauseWithCapture),
                        new Reference(VARCHAR, "b"))))
                .describedAs("non-constant capture")
                .isEqualTo(Optional.empty());

        // A clause whose lambda body references an outer symbol directly (no Bind) must not be
        // evaluated: the evaluator would silently resolve the free reference to null.
        MatchClause clauseWithFreeReference = new MatchClause(
                new Lambda(
                        ImmutableList.of(parameter),
                        comparison(EQUAL, new Reference(BIGINT, parameter.name()), new Reference(BIGINT, "x"))),
                new Reference(VARCHAR, "a"));
        assertThat(optimize(
                new Match(
                        new Constant(BIGINT, 1L),
                        ImmutableList.of(clauseWithFreeReference),
                        new Reference(VARCHAR, "b"))))
                .describedAs("free reference in predicate body")
                .isEqualTo(Optional.empty());
    }

    @Test
    void nonEqualityPredicate()
    {
        // A clause may carry any single-argument boolean lambda, not just an equality test;
        // with a constant operand the rule folds it by evaluating the predicate body.
        Symbol parameter = new Symbol(BIGINT, "p");
        MatchClause greaterThan100 = new MatchClause(
                new Lambda(
                        ImmutableList.of(parameter),
                        comparison(GREATER_THAN, new Reference(BIGINT, parameter.name()), new Constant(BIGINT, 100L))),
                new Reference(VARCHAR, "a"));

        assertThat(optimize(
                new Match(
                        new Constant(BIGINT, 200L),
                        ImmutableList.of(greaterThan100),
                        new Reference(VARCHAR, "b"))))
                .describedAs("predicate true")
                .isEqualTo(Optional.of(new Reference(VARCHAR, "a")));

        assertThat(optimize(
                new Match(
                        new Constant(BIGINT, 50L),
                        ImmutableList.of(greaterThan100),
                        new Reference(VARCHAR, "b"))))
                .describedAs("predicate false")
                .isEqualTo(Optional.of(new Reference(VARCHAR, "b")));
    }

    @Test
    void nonDeterministicPredicate()
    {
        // A clause whose body is non-deterministic must not be folded even with a constant
        // operand: evaluating it at plan time would bake a single random() draw into the plan.
        ResolvedFunction random = PLANNER_CONTEXT.getMetadata().resolveBuiltinFunction("random", ImmutableList.of());
        Symbol parameter = new Symbol(DOUBLE, "p");
        MatchClause greaterThanRandom = new MatchClause(
                new Lambda(
                        ImmutableList.of(parameter),
                        comparison(GREATER_THAN, new Reference(DOUBLE, parameter.name()), new Call(random, ImmutableList.of()))),
                new Reference(VARCHAR, "a"));

        assertThat(optimize(
                new Match(
                        new Constant(DOUBLE, 5.0),
                        ImmutableList.of(greaterThanRandom),
                        new Reference(VARCHAR, "b"))))
                .describedAs("non-deterministic clause body")
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new EvaluateMatch(PLANNER_CONTEXT).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }

    private static MatchClause equalityClause(Expression value, Expression result)
    {
        return IrExpressions.equalityClause(PLANNER_CONTEXT.getMetadata(), new Symbol(value.type(), "operand"), value, result);
    }
}
