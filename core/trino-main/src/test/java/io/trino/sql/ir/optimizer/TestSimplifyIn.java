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
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.SimplifyIn;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimplifyIn
{
    @Test
    void test()
    {
        assertThat(optimize(
                new In(new Constant(BIGINT, 0L), ImmutableList.of(new Reference(BIGINT, "x"), new Constant(BIGINT, null)))))
                .describedAs("null item")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new In(new Constant(BIGINT, null), ImmutableList.of(new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)))))
                .describedAs("null value")
                .isEqualTo(Optional.of(NULL_BOOLEAN));

        assertThat(optimize(
                new In(new Constant(BIGINT, null), ImmutableList.of(new Cast(new Reference(VARCHAR, "y"), BIGINT), new Constant(BIGINT, 1L)))))
                .describedAs("null value, item can fail")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new In(new Constant(BIGINT, 0L), ImmutableList.of(new Reference(BIGINT, "x"), new Constant(BIGINT, 1L), new Reference(BIGINT, "y")))))
                .describedAs("no match")
                .isEqualTo(Optional.of(new In(new Constant(BIGINT, 0L), ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y")))));

        assertThat(optimize(
                new In(new Constant(BIGINT, 0L), ImmutableList.of(new Reference(BIGINT, "x"), new Constant(BIGINT, 0L)))))
                .describedAs("match")
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new In(new Constant(BIGINT, 0L), ImmutableList.of(new Cast(new Reference(VARCHAR, "y"), BIGINT), new Constant(BIGINT, 1L), new Constant(BIGINT, 0L)))))
                .describedAs("match, another item can fail")
                .isEqualTo(Optional.of(new In(new Constant(BIGINT, 0L), ImmutableList.of(new Cast(new Reference(VARCHAR, "y"), BIGINT), new Constant(BIGINT, 0L)))));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new SimplifyIn(PLANNER_CONTEXT).apply(expression, testSession(), emptySymbolAllocator(), ImmutableMap.of());
    }
}
