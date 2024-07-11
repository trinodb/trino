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
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.EvaluateNullIf;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluateNullIf
{
    @Test
    void test()
    {
        assertThat(optimize(
                new NullIf(new Constant(BIGINT, null), new Reference(BIGINT, "x"))))
                .describedAs("first is null")
                .isEqualTo(Optional.of(new Constant(BIGINT, null)));

        assertThat(optimize(
                new NullIf(new Reference(BIGINT, "x"), new Constant(BIGINT, null))))
                .describedAs("second is null")
                .isEqualTo(Optional.of(new Reference(BIGINT, "x")));

        assertThat(optimize(
                new NullIf(new Reference(BIGINT, "x"), new Reference(BIGINT, "x"))))
                .describedAs("equal deterministic expressions")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new NullIf(new Constant(BIGINT, 1L), new Constant(INTEGER, 1L))))
                .describedAs("equal constants of different types")
                .isEqualTo(Optional.of(new Constant(BIGINT, null)));

        assertThat(optimize(
                new NullIf(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L))))
                .describedAs("different constants")
                .isEqualTo(Optional.of(new Constant(BIGINT, 1L)));

        assertThat(optimize(
                new NullIf(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))))
                .describedAs("different non-constants")
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new EvaluateNullIf(PLANNER_CONTEXT).apply(expression, testSession(), ImmutableMap.of());
    }
}
