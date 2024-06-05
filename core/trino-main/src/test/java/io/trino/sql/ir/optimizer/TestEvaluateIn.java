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
import io.trino.sql.ir.In;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.EvaluateIn;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluateIn
{
    @Test
    void test()
    {
        assertThat(optimize(
                new In(new Reference(BIGINT, "x"), ImmutableList.of())))
                .describedAs("non-constant value")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new In(new Constant(BIGINT, 0L), ImmutableList.of(new Reference(BIGINT, "x")))))
                .describedAs("non-constant list")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new In(new Constant(BIGINT, 0L), ImmutableList.of())))
                .describedAs("empty list")
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new In(new Constant(BIGINT, 0L), ImmutableList.of())))
                .describedAs("empty list")
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new In(new Constant(BIGINT, null), ImmutableList.of())))
                .describedAs("null value, empty list")
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new In(new Constant(BIGINT, null), ImmutableList.of(new Constant(BIGINT, 1L)))))
                .describedAs("null value")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new In(new Constant(BIGINT, 1L), ImmutableList.of(new Constant(BIGINT, 1L)))))
                .describedAs("match")
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new In(new Constant(BIGINT, 0L), ImmutableList.of(new Constant(BIGINT, 1L)))))
                .describedAs("no match")
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new In(new Constant(BIGINT, 0L), ImmutableList.of(new Constant(BIGINT, null), new Constant(BIGINT, 1L)))))
                .describedAs("null item, no match")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new In(new Constant(BIGINT, 0L), ImmutableList.of(new Constant(BIGINT, null), new Constant(BIGINT, 0L)))))
                .describedAs("null item, match")
                .isEqualTo(Optional.of(TRUE));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new EvaluateIn(PLANNER_CONTEXT).apply(expression, testSession(), ImmutableMap.of());
    }
}
