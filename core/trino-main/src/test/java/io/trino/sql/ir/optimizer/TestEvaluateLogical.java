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
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.EvaluateLogical;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

class TestEvaluateLogical
{
    @Test
    void testOr()
    {
        assertThat(optimize(
                new Logical(
                        OR,
                        ImmutableList.of(TRUE, FALSE, new Reference(BOOLEAN, "a")))))
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new Logical(
                        OR,
                        ImmutableList.of(FALSE, new Reference(BOOLEAN, "a")))))
                .isEmpty();

        assertThat(optimize(
                new Logical(
                        OR,
                        ImmutableList.of(new Constant(BOOLEAN, null), FALSE))))
                .isEqualTo(Optional.of(new Constant(BOOLEAN, null)));

        assertThat(optimize(
                new Logical(OR, ImmutableList.of(TRUE, TRUE))))
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new Logical(OR, ImmutableList.of(TRUE, FALSE))))
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new Logical(OR, ImmutableList.of(FALSE, TRUE))))
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new Logical(OR, ImmutableList.of(FALSE, FALSE))))
                .isEqualTo(Optional.of(FALSE));
    }

    @Test
    void testAnd()
    {
        assertThat(optimize(
                new Logical(
                        AND,
                        ImmutableList.of(TRUE, FALSE, new Reference(BOOLEAN, "a")))))
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new Logical(
                        AND,
                        ImmutableList.of(TRUE, new Reference(BOOLEAN, "a")))))
                .isEmpty();

        assertThat(optimize(
                new Logical(
                        AND,
                        ImmutableList.of(new Constant(BOOLEAN, null), TRUE))))
                .isEqualTo(Optional.of(new Constant(BOOLEAN, null)));

        assertThat(optimize(
                new Logical(AND, ImmutableList.of(TRUE, TRUE))))
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new Logical(AND, ImmutableList.of(TRUE, FALSE))))
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new Logical(AND, ImmutableList.of(FALSE, TRUE))))
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new Logical(AND, ImmutableList.of(FALSE, FALSE))))
                .isEqualTo(Optional.of(FALSE));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new EvaluateLogical().apply(expression, testSession(), ImmutableMap.of());
    }
}
