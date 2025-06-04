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
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.DesugarBetween;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDesugarBetween
{
    @Test
    void test()
    {
        assertThat(optimize(
                new Between(new Constant(BIGINT, null), new Constant(BIGINT, 0L), new Constant(BIGINT, 5L))))
                .describedAs("null vs non-empty range")
                .isEqualTo(Optional.of(new Logical(
                        AND,
                        ImmutableList.of(
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 0L), new Constant(BIGINT, null)),
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, null), new Constant(BIGINT, 5L))))));

        assertThat(optimize(
                new Between(new Constant(BIGINT, null), new Constant(BIGINT, 5L), new Constant(BIGINT, 0L))))
                .describedAs("null vs empty range")
                .isEqualTo(Optional.of(new Logical(
                        AND,
                        ImmutableList.of(
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 5L), new Constant(BIGINT, null)),
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, null), new Constant(BIGINT, 0L))))));

        assertThat(optimize(
                new Between(new Constant(BIGINT, null), new Reference(BIGINT, "min"), new Reference(BIGINT, "max"))))
                .describedAs("null vs non-constant range")
                .isEqualTo(Optional.of(new Logical(
                        AND,
                        ImmutableList.of(
                                new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "min"), new Constant(BIGINT, null)),
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, null), new Reference(BIGINT, "max"))))));

        assertThat(optimize(
                new Between(new Reference(BIGINT, "x"), new Constant(BIGINT, 0L), new Constant(BIGINT, 5L))))
                .describedAs("non-constant vs non-empty range")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Between(new Reference(BIGINT, "x"), new Constant(BIGINT, 5L), new Constant(BIGINT, 0L))))
                .describedAs("non-constant vs empty range")
                .isEqualTo(Optional.of(ifExpression(not(PLANNER_CONTEXT.getMetadata(), new IsNull(new Reference(BIGINT, "x"))), FALSE)));

        assertThat(optimize(
                new Between(new Constant(BIGINT, 3L), new Constant(BIGINT, 5L), new Constant(BIGINT, 0L))))
                .describedAs("constant vs empty range")
                .isEqualTo(Optional.of(new Logical(
                        AND,
                        ImmutableList.of(
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 5L), new Constant(BIGINT, 3L)),
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 3L), new Constant(BIGINT, 0L))))));

        assertThat(optimize(
                new Between(new Constant(BIGINT, 0L), new Constant(BIGINT, 5L), new Constant(BIGINT, 10L))))
                .describedAs("constant below range")
                .isEqualTo(Optional.of(new Logical(
                        AND,
                        ImmutableList.of(
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 5L), new Constant(BIGINT, 0L)),
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 0L), new Constant(BIGINT, 10L))))));

        assertThat(optimize(
                new Between(new Constant(BIGINT, 7L), new Constant(BIGINT, 5L), new Constant(BIGINT, 10L))))
                .describedAs("constant in range")
                .isEqualTo(Optional.of(new Logical(
                        AND,
                        ImmutableList.of(
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 5L), new Constant(BIGINT, 7L)),
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 7L), new Constant(BIGINT, 10L))))));

        assertThat(optimize(
                new Between(new Constant(BIGINT, 20L), new Constant(BIGINT, 5L), new Constant(BIGINT, 10L))))
                .describedAs("constant above range")
                .isEqualTo(Optional.of(new Logical(
                        AND,
                        ImmutableList.of(
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 5L), new Constant(BIGINT, 20L)),
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 20L), new Constant(BIGINT, 10L))))));

        assertThat(optimize(
                new Between(new Constant(BIGINT, 20L), new Constant(BIGINT, null), new Constant(BIGINT, 10L))))
                .describedAs("null min, constant above range")
                .isEqualTo(Optional.of(new Logical(
                        AND,
                        ImmutableList.of(
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, null), new Constant(BIGINT, 20L)),
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 20L), new Constant(BIGINT, 10L))))));

        assertThat(optimize(
                new Between(new Constant(BIGINT, 0L), new Constant(BIGINT, 10L), new Constant(BIGINT, null))))
                .describedAs("null max, constant below range")
                .isEqualTo(Optional.of(new Logical(
                        AND,
                        ImmutableList.of(
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 10L), new Constant(BIGINT, 0L)),
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 0L), new Constant(BIGINT, null))))));

        assertThat(optimize(
                new Between(new Constant(BIGINT, 5L), new Constant(BIGINT, null), new Constant(BIGINT, 10L))))
                .describedAs("null min, constant below max")
                .isEqualTo(Optional.of(new Logical(
                        AND,
                        ImmutableList.of(
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, null), new Constant(BIGINT, 5L)),
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 5L), new Constant(BIGINT, 10L))))));

        assertThat(optimize(
                new Between(new Constant(BIGINT, 20L), new Constant(BIGINT, 10L), new Constant(BIGINT, null))))
                .describedAs("null max, constant above min")
                .isEqualTo(Optional.of(new Logical(
                        AND,
                        ImmutableList.of(
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 10L), new Constant(BIGINT, 20L)),
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 20L), new Constant(BIGINT, null))))));

        assertThat(optimize(
                new Between(new Reference(BIGINT, "x"), new Constant(BIGINT, 10L), new Constant(BIGINT, null))))
                .describedAs("null max, non-constant value")
                .isEqualTo(Optional.of(ifExpression(new Comparison(LESS_THAN, new Reference(BIGINT, "x"), new Constant(BIGINT, 10L)), FALSE)));

        assertThat(optimize(
                new Between(new Reference(BIGINT, "x"), new Constant(BIGINT, null), new Constant(BIGINT, 10L))))
                .describedAs("null min, non-constant value")
                .isEqualTo(Optional.of(ifExpression(new Comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Constant(BIGINT, 10L)), FALSE)));

        assertThat(optimize(
                new Between(new Reference(BIGINT, "x"), new Constant(BIGINT, null), new Constant(BIGINT, null))))
                .describedAs("null min/max, non-constant value")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Between(new Constant(BIGINT, 0L), new Constant(BIGINT, null), new Constant(BIGINT, null))))
                .describedAs("null min/max, constant value")
                .isEqualTo(Optional.of(new Logical(
                        AND,
                        ImmutableList.of(
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, null), new Constant(BIGINT, 0L)),
                                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 0L), new Constant(BIGINT, null))))));

        assertThat(optimize(
                new Between(new Reference(BIGINT, "x"), new Reference(BIGINT, "min"), new Constant(BIGINT, null))))
                .describedAs("null max, non-constant min")
                .isEqualTo(Optional.of(ifExpression(new Comparison(LESS_THAN, new Reference(BIGINT, "x"), new Reference(BIGINT, "min")), FALSE)));

        assertThat(optimize(
                new Between(new Reference(BIGINT, "x"), new Constant(BIGINT, null), new Reference(BIGINT, "max"))))
                .describedAs("null min, non-constant max")
                .isEqualTo(Optional.of(ifExpression(new Comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Reference(BIGINT, "max")), FALSE)));

        assertThat(optimize(
                new Between(new Reference(BIGINT, "x"), new Constant(BIGINT, null), new Constant(BIGINT, null))))
                .describedAs("null min/max, non-constant value")
                .isEqualTo(Optional.of(NULL_BOOLEAN));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new DesugarBetween(PLANNER_CONTEXT).apply(expression, testSession(), ImmutableMap.of());
    }
}
