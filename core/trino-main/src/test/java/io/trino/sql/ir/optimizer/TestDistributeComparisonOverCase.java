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
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.rule.DistributeComparisonOverCase;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.ir.TestingIr.nullIf;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDistributeComparisonOverCase
{
    @Test
    void test()
    {
        assertThat(optimize(
                comparison(
                        LESS_THAN,
                        new Case(
                                ImmutableList.of(
                                        new WhenClause(new Reference(BOOLEAN, "a"), new Reference(BIGINT, "x")),
                                        new WhenClause(new Reference(BOOLEAN, "b"), new Reference(BIGINT, "y"))),
                                new Reference(BIGINT, "z")),
                        new Reference(BIGINT, "m"))))
                .describedAs("case(...) < reference")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(
                                        new Reference(BOOLEAN, "a"),
                                        comparison(LESS_THAN, new Reference(BIGINT, "x"), new Reference(BIGINT, "m"))),
                                new WhenClause(
                                        new Reference(BOOLEAN, "b"),
                                        comparison(LESS_THAN, new Reference(BIGINT, "y"), new Reference(BIGINT, "m")))),
                        comparison(LESS_THAN, new Reference(BIGINT, "z"), new Reference(BIGINT, "m")))));

        assertThat(optimize(
                comparison(
                        LESS_THAN,
                        new Case(
                                ImmutableList.of(
                                        new WhenClause(new Reference(BOOLEAN, "a"), new Reference(BIGINT, "x")),
                                        new WhenClause(new Reference(BOOLEAN, "b"), new Reference(BIGINT, "y"))),
                                new Reference(BIGINT, "z")),
                        new Constant(BIGINT, 1L))))
                .describedAs("case(...) < constant")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(
                                        new Reference(BOOLEAN, "a"),
                                        comparison(LESS_THAN, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L))),
                                new WhenClause(
                                        new Reference(BOOLEAN, "b"),
                                        comparison(LESS_THAN, new Reference(BIGINT, "y"), new Constant(BIGINT, 1L)))),
                        comparison(LESS_THAN, new Reference(BIGINT, "z"), new Constant(BIGINT, 1L)))));

        assertThat(optimize(
                comparison(
                        LESS_THAN,
                        new Reference(BIGINT, "m"),
                        new Case(
                                ImmutableList.of(
                                        new WhenClause(new Reference(BOOLEAN, "a"), new Reference(BIGINT, "x")),
                                        new WhenClause(new Reference(BOOLEAN, "b"), new Reference(BIGINT, "y"))),
                                new Reference(BIGINT, "z")))))
                .describedAs("reference < case(...)")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(
                                        new Reference(BOOLEAN, "a"),
                                        comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Reference(BIGINT, "m"))),
                                new WhenClause(
                                        new Reference(BOOLEAN, "b"),
                                        comparison(GREATER_THAN, new Reference(BIGINT, "y"), new Reference(BIGINT, "m")))),
                        comparison(GREATER_THAN, new Reference(BIGINT, "z"), new Reference(BIGINT, "m")))));

        assertThat(optimize(
                comparison(
                        LESS_THAN,
                        new Constant(BIGINT, 1L),
                        new Case(
                                ImmutableList.of(
                                        new WhenClause(new Reference(BOOLEAN, "a"), new Reference(BIGINT, "x")),
                                        new WhenClause(new Reference(BOOLEAN, "b"), new Reference(BIGINT, "y"))),
                                new Reference(BIGINT, "z")))))
                .describedAs("constant < case(...)")
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(
                                        new Reference(BOOLEAN, "a"),
                                        comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L))),
                                new WhenClause(
                                        new Reference(BOOLEAN, "b"),
                                        comparison(GREATER_THAN, new Reference(BIGINT, "y"), new Constant(BIGINT, 1L)))),
                        comparison(GREATER_THAN, new Reference(BIGINT, "z"), new Constant(BIGINT, 1L)))));
    }

    @Test
    void testDoesNotDistributeNullIf()
    {
        // NULLIF(x, 1) desugars to a Case; it must be left intact so it stays recognizable for connector pushdown.
        Expression nullIf = nullIf(new SymbolAllocator(), new Reference(BIGINT, "x"), new Constant(BIGINT, 1L));

        assertThat(optimize(comparison(EQUAL, nullIf, new Reference(BIGINT, "m"))))
                .describedAs("nullif(...) = reference")
                .isEqualTo(Optional.empty());

        assertThat(optimize(comparison(EQUAL, new Reference(BIGINT, "m"), nullIf)))
                .describedAs("reference = nullif(...)")
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new DistributeComparisonOverCase(PLANNER_CONTEXT).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }
}
