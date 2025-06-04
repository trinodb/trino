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
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.optimizer.rule.EvaluateIsNull;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IDENTICAL;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluateIsNull
{
    @Test
    void test()
    {
        assertThat(optimize(new IsNull(new Constant(BIGINT, null))))
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(new IsNull(new Constant(BIGINT, 1L))))
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(new IsNull(new Cast(new Reference(BIGINT, "a"), INTEGER))))
                .isEqualTo(Optional.empty());

        assertThat(optimize(new IsNull(not(PLANNER_CONTEXT.getMetadata(), new Reference(BOOLEAN, "a")))))
                .isEqualTo(Optional.of(new IsNull(new Reference(BOOLEAN, "a"))));

        assertThat(optimize(new IsNull(new IsNull(new Reference(BIGINT, "a")))))
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(new IsNull(new Row(ImmutableList.of(new Reference(BIGINT, "a"))))))
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(new IsNull(new Comparison(EQUAL, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))))
                .isEqualTo(Optional.of(new Logical(OR, ImmutableList.of(new IsNull(new Reference(BIGINT, "a")), new IsNull(new Reference(BIGINT, "b"))))));

        assertThat(optimize(new IsNull(new Comparison(IDENTICAL, new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))))
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(new IsNull(new Reference(BIGINT, "a"))))
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new EvaluateIsNull().apply(expression, testSession(), ImmutableMap.of());
    }
}
