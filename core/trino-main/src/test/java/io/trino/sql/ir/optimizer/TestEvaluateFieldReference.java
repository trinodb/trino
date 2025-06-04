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
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.optimizer.rule.EvaluateFieldReference;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrExpressions.row;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluateFieldReference
{
    @Test
    void test()
    {
        assertThat(optimize(
                new FieldReference(new Row(ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"))), 1)))
                .isEqualTo(Optional.of(new Reference(BIGINT, "b")));

        assertThat(optimize(
                new FieldReference(new Constant(RowType.anonymousRow(BIGINT, BIGINT), null), 1)))
                .isEqualTo(Optional.of(new Constant(BIGINT, null)));

        assertThat(optimize(
                new FieldReference(row(ImmutableList.of(TRUE, FALSE)), 1)))
                .isEqualTo(Optional.of(FALSE));

        assertThat(optimize(
                new FieldReference(new Reference(RowType.anonymousRow(BIGINT, BIGINT), "x"), 1)))
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new EvaluateFieldReference().apply(expression, testSession(), ImmutableMap.of());
    }
}
