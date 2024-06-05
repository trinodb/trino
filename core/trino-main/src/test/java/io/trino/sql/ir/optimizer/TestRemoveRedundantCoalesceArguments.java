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
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.RemoveRedundantCoalesceArguments;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRemoveRedundantCoalesceArguments
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(createTestTransactionManager(), PLANNER_CONTEXT);
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", ImmutableList.of());

    @Test
    void test()
    {
        assertThat(optimize(
                new Coalesce(new Constant(BIGINT, null), new Constant(BIGINT, null))))
                .isEqualTo(Optional.of(new Constant(BIGINT, null)));

        assertThat(optimize(
                new Coalesce(new Constant(BIGINT, null), new Constant(BIGINT, 1L))))
                .isEqualTo(Optional.of(new Constant(BIGINT, 1L)));

        assertThat(optimize(
                new Coalesce(
                        new Reference(BIGINT, "a"),
                        new Reference(BIGINT, "a"),
                        new Reference(BIGINT, "a"))))
                .isEqualTo(Optional.of(new Reference(BIGINT, "a")));

        assertThat(optimize(
                new Coalesce(
                        new Reference(BIGINT, "a"),
                        new Reference(BIGINT, "b"),
                        new Constant(BIGINT, 1L),
                        new Reference(BIGINT, "c"),
                        new Reference(BIGINT, "d"))))
                .isEqualTo(Optional.of(new Coalesce(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"), new Constant(BIGINT, 1L))));

        assertThat(optimize(
                new Coalesce(
                        new Reference(BIGINT, "a"),
                        new Reference(BIGINT, "b"),
                        new Reference(BIGINT, "a"),
                        new Reference(BIGINT, "c"))))
                .isEqualTo(Optional.of(new Coalesce(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"), new Reference(BIGINT, "c"))));

        assertThat(optimize(
                new Coalesce(
                        new Call(RANDOM, ImmutableList.of()),
                        new Call(RANDOM, ImmutableList.of()))))
                .describedAs("non-deterministic arguments")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Coalesce(
                        new Reference(DOUBLE, "a"),
                        new Reference(DOUBLE, "b"),
                        new Call(RANDOM, ImmutableList.of()),
                        new Call(RANDOM, ImmutableList.of()),
                        new Reference(DOUBLE, "a"))))
                .describedAs("non-deterministic and deterministic arguments")
                .isEqualTo(Optional.of(new Coalesce(
                        new Reference(DOUBLE, "a"),
                        new Reference(DOUBLE, "b"),
                        new Call(RANDOM, ImmutableList.of()),
                        new Call(RANDOM, ImmutableList.of()))));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new RemoveRedundantCoalesceArguments().apply(expression, testSession(), ImmutableMap.of());
    }
}
