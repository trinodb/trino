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
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.RemoveRedundantMatchClauses;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRemoveRedundantMatchClauses
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", ImmutableList.of());

    @Test
    void test()
    {
        assertThat(optimize(
                new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r1")),
                                equalityClause(new Reference(BIGINT, "b"), new Reference(VARCHAR, "r2")),
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r3"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("redundant terms")
                .isEqualTo(Optional.of(new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r1")),
                                equalityClause(new Reference(BIGINT, "b"), new Reference(VARCHAR, "r2"))),
                        new Reference(VARCHAR, "d"))));

        assertThat(optimize(
                new Match(
                        new Constant(BIGINT, 1L),
                        ImmutableList.of(
                                equalityClause(new Constant(BIGINT, 2L), new Reference(VARCHAR, "r1")),
                                equalityClause(new Reference(BIGINT, "x"), new Reference(VARCHAR, "r2"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("redundant constants")
                .isEqualTo(Optional.of(new Match(
                        new Constant(BIGINT, 1L),
                        ImmutableList.of(equalityClause(new Reference(BIGINT, "x"), new Reference(VARCHAR, "r2"))),
                        new Reference(VARCHAR, "d"))));

        assertThat(optimize(
                new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r1")),
                                equalityClause(new Reference(BIGINT, "x"), new Reference(VARCHAR, "r2"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("short-circuit")
                .isEqualTo(Optional.of(new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r1"))),
                        new Reference(VARCHAR, "r2"))));

        assertThat(optimize(
                new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                equalityClause(new Reference(BIGINT, "x"), new Reference(VARCHAR, "r1")),
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r2"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("short-circuit on first term")
                .isEqualTo(Optional.of(new Reference(VARCHAR, "r1")));

        assertThat(optimize(
                new Match(
                        new Reference(DOUBLE, "x"),
                        ImmutableList.of(
                                equalityClause(new Reference(DOUBLE, "a"), new Reference(VARCHAR, "r1")),
                                equalityClause(new Reference(DOUBLE, "b"), new Reference(VARCHAR, "r2")),
                                equalityClause(new Call(RANDOM, ImmutableList.of()), new Reference(VARCHAR, "r3")),
                                equalityClause(new Call(RANDOM, ImmutableList.of()), new Reference(VARCHAR, "r4")),
                                equalityClause(new Reference(DOUBLE, "a"), new Reference(VARCHAR, "r5"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("non-deterministic terms")
                .isEqualTo(Optional.of(new Match(
                        new Reference(DOUBLE, "x"),
                        ImmutableList.of(
                                equalityClause(new Reference(DOUBLE, "a"), new Reference(VARCHAR, "r1")),
                                equalityClause(new Reference(DOUBLE, "b"), new Reference(VARCHAR, "r2")),
                                equalityClause(new Call(RANDOM, ImmutableList.of()), new Reference(VARCHAR, "r3")),
                                equalityClause(new Call(RANDOM, ImmutableList.of()), new Reference(VARCHAR, "r4"))),
                        new Reference(VARCHAR, "d"))));
    }

    @Test
    void nonEqualityClausesActAsBarrier()
    {
        // Only equality-shaped clauses are deduplicated. A non-equality predicate is opaque, so it
        // acts as a barrier: equality duplicates before it are still removed, but the duplicate `a`
        // clause after it is preserved — once a general predicate gates dispatch, the operand's
        // value can no longer be reasoned about.
        Symbol parameter = new Symbol(BIGINT, "p");
        MatchClause nonEquality = new MatchClause(
                new Lambda(
                        ImmutableList.of(parameter),
                        comparison(GREATER_THAN, new Reference(BIGINT, parameter.name()), new Constant(BIGINT, 100L))),
                new Reference(VARCHAR, "barrier"));

        assertThat(optimize(
                new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r1")),
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r2")),
                                nonEquality,
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r4"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("non-equality clause is a barrier")
                .isEqualTo(Optional.of(new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r1")),
                                nonEquality,
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r4"))),
                        new Reference(VARCHAR, "d"))));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new RemoveRedundantMatchClauses(FUNCTIONS.getPlannerContext()).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }

    private static MatchClause equalityClause(Expression value, Expression result)
    {
        return IrExpressions.equalityClause(PLANNER_CONTEXT.getMetadata(), new Symbol(value.type(), "operand"), value, result);
    }
}
