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
import io.trino.sql.ir.optimizer.rule.MergeMatchClauses;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMergeMatchClauses
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction DIVIDE_BIGINT = FUNCTIONS.resolveOperator(DIVIDE, ImmutableList.of(BIGINT, BIGINT));

    private static final Symbol PARAMETER = new Symbol(BIGINT, "operand");

    @Test
    void testMergeAdjacentClauses()
    {
        assertThat(optimize(
                new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r1")),
                                equalityClause(new Reference(BIGINT, "b"), new Reference(VARCHAR, "r1")),
                                equalityClause(new Reference(BIGINT, "c"), new Reference(VARCHAR, "r2"))),
                        new Reference(VARCHAR, "d"))))
                .isEqualTo(Optional.of(new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                mergedEqualityClause(new Reference(VARCHAR, "r1"), new Reference(BIGINT, "a"), new Reference(BIGINT, "b")),
                                equalityClause(new Reference(BIGINT, "c"), new Reference(VARCHAR, "r2"))),
                        new Reference(VARCHAR, "d"))));

        assertThat(optimize(
                new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r1")),
                                equalityClause(new Reference(BIGINT, "b"), new Reference(VARCHAR, "r1")),
                                equalityClause(new Reference(BIGINT, "c"), new Reference(VARCHAR, "r1"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("run of three clauses")
                .isEqualTo(Optional.of(new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                mergedEqualityClause(new Reference(VARCHAR, "r1"), new Reference(BIGINT, "a"), new Reference(BIGINT, "b"), new Reference(BIGINT, "c"))),
                        new Reference(VARCHAR, "d"))));

        MatchClause differentParameter = new MatchClause(
                new Lambda(
                        ImmutableList.of(new Symbol(BIGINT, "q")),
                        comparison(EQUAL, new Reference(BIGINT, "q"), new Reference(BIGINT, "b"))),
                new Reference(VARCHAR, "r1"));
        assertThat(optimize(
                new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r1")),
                                differentParameter),
                        new Reference(VARCHAR, "d"))))
                .describedAs("lambda parameters are unified when merging")
                .isEqualTo(Optional.of(new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                mergedEqualityClause(new Reference(VARCHAR, "r1"), new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))),
                        new Reference(VARCHAR, "d"))));
    }

    @Test
    void testDropTrailingClauses()
    {
        assertThat(optimize(
                new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r1")),
                                equalityClause(new Reference(BIGINT, "b"), new Reference(VARCHAR, "d"))),
                        new Reference(VARCHAR, "d"))))
                .isEqualTo(Optional.of(new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r1"))),
                        new Reference(VARCHAR, "d"))));

        assertThat(optimize(
                new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "d"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("all clauses produce the default")
                .isEqualTo(Optional.of(new Reference(VARCHAR, "d")));
    }

    @Test
    void testDoesNotFire()
    {
        assertThat(optimize(
                new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r1")),
                                equalityClause(new Reference(BIGINT, "b"), new Reference(VARCHAR, "r2")),
                                equalityClause(new Reference(BIGINT, "c"), new Reference(VARCHAR, "r1"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("non-adjacent clauses with the same result")
                .isEqualTo(Optional.empty());

        Expression failing = comparison(
                EQUAL,
                new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "y"), new Constant(BIGINT, 0L))),
                new Constant(BIGINT, 1L));
        assertThat(optimize(
                new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "r1")),
                                new MatchClause(
                                        new Lambda(ImmutableList.of(PARAMETER), failing),
                                        new Reference(VARCHAR, "d"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("trailing clause with failing predicate")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Match(
                        new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "y"), new Constant(BIGINT, 0L))),
                        ImmutableList.of(equalityClause(new Reference(BIGINT, "a"), new Reference(VARCHAR, "d"))),
                        new Reference(VARCHAR, "d"))))
                .describedAs("dropping the last clause would skip the failing operand")
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new MergeMatchClauses(PLANNER_CONTEXT).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }

    private static MatchClause equalityClause(Expression value, Expression result)
    {
        return IrExpressions.equalityClause(PLANNER_CONTEXT.getMetadata(), new Symbol(value.type(), "operand"), value, result);
    }

    private static MatchClause mergedEqualityClause(Expression result, Expression... values)
    {
        ImmutableList.Builder<Expression> disjuncts = ImmutableList.builder();
        for (Expression value : values) {
            disjuncts.add(IrExpressions.equalityClause(PLANNER_CONTEXT.getMetadata(), PARAMETER, value, result).lambda().body());
        }
        return new MatchClause(new Lambda(ImmutableList.of(PARAMETER), or(disjuncts.build())), result);
    }
}
