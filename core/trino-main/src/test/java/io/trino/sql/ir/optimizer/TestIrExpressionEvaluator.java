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
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIrExpressionEvaluator
{
    @Test
    void testIn()
    {
        assertThat(evaluate(new In(new Constant(BIGINT, null), ImmutableList.of())))
                .describedAs("null value, empty list")
                .isEqualTo(false);

        assertThat(evaluate(new In(new Constant(BIGINT, 1L), ImmutableList.of())))
                .describedAs("empty list")
                .isEqualTo(false);

        assertThat(evaluate(new In(new Constant(BIGINT, null), ImmutableList.of(new Constant(BIGINT, 1L)))))
                .describedAs("null value")
                .isNull();
    }

    private static Object evaluate(Expression expression)
    {
        return new IrExpressionEvaluator(PLANNER_CONTEXT).evaluate(expression, testSession(), ImmutableMap.of());
    }
}
