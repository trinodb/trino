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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.EvaluateComparison;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IDENTICAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluateComparison
{
    @Test
    void testIdentical()
    {
        assertThat(optimize(
                new Comparison(IDENTICAL, new Constant(BIGINT, 1L), new Constant(BIGINT, 1L))))
                .describedAs("equal constants")
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new Comparison(IDENTICAL, new Constant(BIGINT, 1L), new Constant(BIGINT, 2L))))
                .describedAs("different constants")
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new Comparison(IDENTICAL, new Constant(BIGINT, 1L), new Constant(BIGINT, null))))
                .describedAs("constant vs null")
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new Comparison(IDENTICAL, new Constant(BIGINT, null), new Constant(BIGINT, 1L))))
                .describedAs("constant vs null")
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new Comparison(IDENTICAL, new Constant(BIGINT, null), new Constant(BIGINT, null))))
                .describedAs("both null")
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new Comparison(IDENTICAL, new Reference(BIGINT, "x"), new Constant(BIGINT, null))))
                .describedAs("non-constant vs null")
                .isEqualTo(Optional.of(new IsNull(new Reference(BIGINT, "x"))));

        assertThat(optimize(
                new Comparison(IDENTICAL, new Constant(BIGINT, null), new Reference(BIGINT, "x"))))
                .describedAs("non-constant vs null")
                .isEqualTo(Optional.of(new IsNull(new Reference(BIGINT, "x"))));

        assertThat(optimize(
                new Comparison(IDENTICAL, new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))))
                .describedAs("non-constants")
                .isEqualTo(Optional.empty());
    }

    @Test
    void testEqual()
    {
        assertThat(optimize(
                new Comparison(EQUAL, new Constant(BIGINT, 1L), new Constant(BIGINT, 2L))))
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new Comparison(EQUAL, new Constant(BIGINT, 1L), new Constant(BIGINT, 1L))))
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new Comparison(EQUAL, new Constant(BIGINT, 1L), new Constant(BIGINT, null))))
                .describedAs("constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(EQUAL, new Constant(BIGINT, null), new Constant(BIGINT, 1L))))
                .describedAs("constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(EQUAL, new Constant(BIGINT, null), new Constant(BIGINT, null))))
                .describedAs("both null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, null))))
                .describedAs("non-constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(EQUAL, new Constant(BIGINT, null), new Reference(BIGINT, "x"))))
                .describedAs("non-constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(EQUAL, new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))))
                .describedAs("non-constants")
                .isEqualTo(Optional.empty());
    }

    @Test
    void testNotEqual()
    {
        assertThat(optimize(
                new Comparison(NOT_EQUAL, new Constant(BIGINT, 1L), new Constant(BIGINT, 1L))))
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new Comparison(NOT_EQUAL, new Constant(BIGINT, 1L), new Constant(BIGINT, 2L))))
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new Comparison(NOT_EQUAL, new Constant(BIGINT, 1L), new Constant(BIGINT, null))))
                .describedAs("constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(NOT_EQUAL, new Constant(BIGINT, null), new Constant(BIGINT, 1L))))
                .describedAs("constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(NOT_EQUAL, new Constant(BIGINT, null), new Constant(BIGINT, null))))
                .describedAs("both null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(NOT_EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, null))))
                .describedAs("non-constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(NOT_EQUAL, new Constant(BIGINT, null), new Reference(BIGINT, "x"))))
                .describedAs("non-constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(NOT_EQUAL, new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))))
                .describedAs("non-constants")
                .isEqualTo(Optional.empty());
    }

    @Test
    void testLessThan()
    {
        assertThat(optimize(
                new Comparison(LESS_THAN, new Constant(BIGINT, 1L), new Constant(BIGINT, 1L))))
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new Comparison(LESS_THAN, new Constant(BIGINT, 1L), new Constant(BIGINT, 2L))))
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new Comparison(LESS_THAN, new Constant(BIGINT, 1L), new Constant(BIGINT, null))))
                .describedAs("constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(LESS_THAN, new Constant(BIGINT, null), new Constant(BIGINT, 1L))))
                .describedAs("constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(LESS_THAN, new Constant(BIGINT, null), new Constant(BIGINT, null))))
                .describedAs("both null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(LESS_THAN, new Reference(BIGINT, "x"), new Constant(BIGINT, null))))
                .describedAs("non-constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(LESS_THAN, new Constant(BIGINT, null), new Reference(BIGINT, "x"))))
                .describedAs("non-constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(LESS_THAN, new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))))
                .describedAs("non-constants")
                .isEqualTo(Optional.empty());
    }

    @Test
    void testLessThanOrEqual()
    {
        assertThat(optimize(
                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 1L), new Constant(BIGINT, 0L))))
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 1L), new Constant(BIGINT, 1L))))
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, 1L), new Constant(BIGINT, null))))
                .describedAs("constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, null), new Constant(BIGINT, 1L))))
                .describedAs("constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, null), new Constant(BIGINT, null))))
                .describedAs("both null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, null))))
                .describedAs("non-constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(LESS_THAN_OR_EQUAL, new Constant(BIGINT, null), new Reference(BIGINT, "x"))))
                .describedAs("non-constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))))
                .describedAs("non-constants")
                .isEqualTo(Optional.empty());
    }

    @Test
    void testGreaterThan()
    {
        assertThat(optimize(
                new Comparison(GREATER_THAN, new Constant(BIGINT, 1L), new Constant(BIGINT, 1L))))
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new Comparison(GREATER_THAN, new Constant(BIGINT, 2L), new Constant(BIGINT, 1L))))
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new Comparison(GREATER_THAN, new Constant(BIGINT, 1L), new Constant(BIGINT, null))))
                .describedAs("constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(GREATER_THAN, new Constant(BIGINT, null), new Constant(BIGINT, 1L))))
                .describedAs("constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(GREATER_THAN, new Constant(BIGINT, null), new Constant(BIGINT, null))))
                .describedAs("both null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Constant(BIGINT, null))))
                .describedAs("non-constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(GREATER_THAN, new Constant(BIGINT, null), new Reference(BIGINT, "x"))))
                .describedAs("non-constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))))
                .describedAs("non-constants")
                .isEqualTo(Optional.empty());
    }

    @Test
    void testGreaterThanOrEqual()
    {
        assertThat(optimize(
                new Comparison(GREATER_THAN_OR_EQUAL, new Constant(BIGINT, 1L), new Constant(BIGINT, 2L))))
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new Comparison(GREATER_THAN_OR_EQUAL, new Constant(BIGINT, 1L), new Constant(BIGINT, 1L))))
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new Comparison(GREATER_THAN_OR_EQUAL, new Constant(BIGINT, 1L), new Constant(BIGINT, null))))
                .describedAs("constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(GREATER_THAN_OR_EQUAL, new Constant(BIGINT, null), new Constant(BIGINT, 1L))))
                .describedAs("constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(GREATER_THAN_OR_EQUAL, new Constant(BIGINT, null), new Constant(BIGINT, null))))
                .describedAs("both null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, null))))
                .describedAs("non-constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(GREATER_THAN_OR_EQUAL, new Constant(BIGINT, null), new Reference(BIGINT, "x"))))
                .describedAs("non-constant vs null")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new Comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))))
                .describedAs("non-constants")
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new EvaluateComparison(PLANNER_CONTEXT).apply(expression, testSession(), ImmutableMap.of());
    }
}
