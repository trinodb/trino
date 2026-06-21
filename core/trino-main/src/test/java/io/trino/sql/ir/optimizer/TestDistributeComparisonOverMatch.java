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
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.DistributeComparisonOverMatch;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDistributeComparisonOverMatch
{
    @Test
    void test()
    {
        assertThat(optimize(
                comparison(
                        LESS_THAN,
                        new Match(
                                new Reference(BIGINT, "s"),
                                ImmutableList.of(
                                        equalityClause(new Reference(BIGINT, "a"), new Reference(BIGINT, "x")),
                                        equalityClause(new Reference(BIGINT, "b"), new Reference(BIGINT, "y"))),
                                new Reference(BIGINT, "z")),
                        new Reference(BIGINT, "m"))))
                .describedAs("switch(...) < reference")
                .isEqualTo(Optional.of(
                        new Match(
                                new Reference(BIGINT, "s"),
                                ImmutableList.of(
                                        equalityClause(
                                                new Reference(BIGINT, "a"),
                                                comparison(LESS_THAN, new Reference(BIGINT, "x"), new Reference(BIGINT, "m"))),
                                        equalityClause(
                                                new Reference(BIGINT, "b"),
                                                comparison(LESS_THAN, new Reference(BIGINT, "y"), new Reference(BIGINT, "m")))),
                                comparison(LESS_THAN, new Reference(BIGINT, "z"), new Reference(BIGINT, "m")))));

        assertThat(optimize(
                comparison(
                        LESS_THAN,
                        new Match(
                                new Reference(BIGINT, "s"),
                                ImmutableList.of(
                                        equalityClause(new Reference(BIGINT, "a"), new Reference(BIGINT, "x")),
                                        equalityClause(new Reference(BIGINT, "b"), new Reference(BIGINT, "y"))),
                                new Reference(BIGINT, "z")),
                        new Constant(BIGINT, 1L))))
                .describedAs("switch(...) < constant")
                .isEqualTo(Optional.of(
                        new Match(
                                new Reference(BIGINT, "s"),
                                ImmutableList.of(
                                        equalityClause(
                                                new Reference(BIGINT, "a"),
                                                comparison(LESS_THAN, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L))),
                                        equalityClause(
                                                new Reference(BIGINT, "b"),
                                                comparison(LESS_THAN, new Reference(BIGINT, "y"), new Constant(BIGINT, 1L)))),
                                comparison(LESS_THAN, new Reference(BIGINT, "z"), new Constant(BIGINT, 1L)))));

        assertThat(optimize(
                comparison(
                        LESS_THAN,
                        new Reference(BIGINT, "m"),
                        new Match(
                                new Reference(BIGINT, "s"),
                                ImmutableList.of(
                                        equalityClause(new Reference(BIGINT, "a"), new Reference(BIGINT, "x")),
                                        equalityClause(new Reference(BIGINT, "b"), new Reference(BIGINT, "y"))),
                                new Reference(BIGINT, "z")))))
                .describedAs("reference < switch(...)")
                .isEqualTo(Optional.of(
                        new Match(
                                new Reference(BIGINT, "s"),
                                ImmutableList.of(
                                        equalityClause(
                                                new Reference(BIGINT, "a"),
                                                comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Reference(BIGINT, "m"))),
                                        equalityClause(
                                                new Reference(BIGINT, "b"),
                                                comparison(GREATER_THAN, new Reference(BIGINT, "y"), new Reference(BIGINT, "m")))),
                                comparison(GREATER_THAN, new Reference(BIGINT, "z"), new Reference(BIGINT, "m")))));

        assertThat(optimize(
                comparison(
                        LESS_THAN,
                        new Constant(BIGINT, 1L),
                        new Match(
                                new Reference(BIGINT, "s"),
                                ImmutableList.of(
                                        equalityClause(new Reference(BIGINT, "a"), new Reference(BIGINT, "x")),
                                        equalityClause(new Reference(BIGINT, "b"), new Reference(BIGINT, "y"))),
                                new Reference(BIGINT, "z")))))
                .describedAs("constant < switch(...)")
                .isEqualTo(Optional.of(
                        new Match(
                                new Reference(BIGINT, "s"),
                                ImmutableList.of(
                                        equalityClause(
                                                new Reference(BIGINT, "a"),
                                                comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L))),
                                        equalityClause(
                                                new Reference(BIGINT, "b"),
                                                comparison(GREATER_THAN, new Reference(BIGINT, "y"), new Constant(BIGINT, 1L)))),
                                comparison(GREATER_THAN, new Reference(BIGINT, "z"), new Constant(BIGINT, 1L)))));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new DistributeComparisonOverMatch(PLANNER_CONTEXT).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }

    private static MatchClause equalityClause(Expression value, Expression result)
    {
        return IrExpressions.equalityClause(PLANNER_CONTEXT.getMetadata(), new Symbol(value.type(), "operand"), value, result);
    }
}
