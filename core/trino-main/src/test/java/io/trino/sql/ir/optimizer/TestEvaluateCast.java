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
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.rule.EvaluateCast;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluateCast
{
    @Test
    void test()
    {
        assertThat(optimize(new Cast(new Constant(INTEGER, null), BIGINT)))
                .isEqualTo(Optional.of(new Constant(BIGINT, null)));

        assertThat(optimize(new Cast(new Constant(INTEGER, 1L), VARCHAR)))
                .isEqualTo(Optional.of(new Constant(VARCHAR, utf8Slice("1"))));

        assertThat(optimize(new Cast(new Constant(INTEGER, 1L), VARCHAR)))
                .isEqualTo(Optional.of(new Constant(VARCHAR, utf8Slice("1"))));

        assertThat(optimize(new Cast(new Constant(VARCHAR, utf8Slice("abc")), BIGINT)))
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new EvaluateCast(PLANNER_CONTEXT).apply(expression, testSession(), ImmutableMap.of());
    }
}
