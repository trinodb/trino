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
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.rule.SpecializeCaseToMatch;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSpecializeCaseToMatch
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", ImmutableList.of());

    private static final Expression X = new Reference(BIGINT, "x");
    private static final Expression R1 = new Reference(VARCHAR, "r1");
    private static final Expression R2 = new Reference(VARCHAR, "r2");
    private static final Expression DEFAULT = new Reference(VARCHAR, "d");

    @Test
    void testSpecialize()
    {
        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(comparison(EQUAL, X, new Constant(BIGINT, 1L)), R1),
                                new WhenClause(comparison(EQUAL, X, new Constant(BIGINT, 2L)), R2)),
                        DEFAULT)))
                .isEqualTo(Optional.of(new Match(
                        X,
                        ImmutableList.of(
                                equalityClause(new Constant(BIGINT, 1L), R1),
                                equalityClause(new Constant(BIGINT, 2L), R2)),
                        DEFAULT)));

        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(comparison(EQUAL, new Constant(BIGINT, 1L), X), R1),
                                new WhenClause(comparison(EQUAL, X, new Constant(BIGINT, 2L)), R2)),
                        DEFAULT)))
                .describedAs("operand on either side of the equality")
                .isEqualTo(Optional.of(new Match(
                        X,
                        ImmutableList.of(
                                equalityClause(new Constant(BIGINT, 1L), R1),
                                equalityClause(new Constant(BIGINT, 2L), R2)),
                        DEFAULT)));

        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(comparison(EQUAL, X, new Reference(BIGINT, "a")), R1),
                                new WhenClause(comparison(EQUAL, X, new Reference(BIGINT, "b")), R2)),
                        DEFAULT)))
                .describedAs("non-constant values are lifted into Bind captures")
                .isEqualTo(Optional.of(new Match(
                        X,
                        ImmutableList.of(
                                captureClause(new Reference(BIGINT, "a"), R1),
                                captureClause(new Reference(BIGINT, "b"), R2)),
                        DEFAULT)));
    }

    @Test
    void testDoesNotFire()
    {
        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(comparison(EQUAL, X, new Constant(BIGINT, 1L)), R1)),
                        DEFAULT)))
                .describedAs("single clause")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(comparison(EQUAL, X, new Constant(BIGINT, 1L)), R1),
                                new WhenClause(comparison(EQUAL, new Reference(BIGINT, "y"), new Constant(BIGINT, 2L)), R2)),
                        DEFAULT)))
                .describedAs("no common operand")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(comparison(EQUAL, X, new Constant(BIGINT, 0L)), R1),
                                new WhenClause(comparison(EQUAL, new Reference(BIGINT, "y"), new Constant(BIGINT, 0L)), R2)),
                        DEFAULT)))
                .describedAs("constant common operand")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(comparison(EQUAL, X, new Constant(BIGINT, 1L)), R1),
                                new WhenClause(comparison(GREATER_THAN, X, new Constant(BIGINT, 2L)), R2)),
                        DEFAULT)))
                .describedAs("non-equality condition")
                .isEqualTo(Optional.empty());

        Expression random = new Call(RANDOM, ImmutableList.of());
        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(comparison(EQUAL, random, new Constant(DOUBLE, 1.0)), R1),
                                new WhenClause(comparison(EQUAL, random, new Constant(DOUBLE, 2.0)), R2)),
                        DEFAULT)))
                .describedAs("non-deterministic common operand")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Case(
                        ImmutableList.of(
                                new WhenClause(new Reference(BOOLEAN, "a"), R1),
                                new WhenClause(comparison(EQUAL, X, new Constant(BIGINT, 2L)), R2)),
                        DEFAULT)))
                .describedAs("non-comparison condition")
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new SpecializeCaseToMatch(PLANNER_CONTEXT).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }

    private static MatchClause equalityClause(Expression value, Expression result)
    {
        return IrExpressions.equalityClause(PLANNER_CONTEXT.getMetadata(), new Symbol(BIGINT, "match"), value, result);
    }

    private static MatchClause captureClause(Reference value, Expression result)
    {
        MatchClause plain = equalityClause(value, result);
        return new MatchClause(
                new Bind(
                        ImmutableList.of(value),
                        new Lambda(
                                ImmutableList.of(new Symbol(value.type(), value.name()), new Symbol(BIGINT, "match")),
                                plain.lambda().body())),
                result);
    }
}
