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
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.optimizer.rule.EvaluateRow;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluateRow
{
    @Test
    void test()
    {
        assertThat(optimize(
                new Row(ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)))))
                .isPresent()
                .get()
                .satisfies(row -> {
                    if (!(row instanceof Constant(RowType type, SqlRow value))) {
                        throw new AssertionError("Expected SqlRow");
                    }

                    assertThat(readNativeValue(BIGINT, value.getRawFieldBlock(0), value.getRawIndex()))
                            .isEqualTo(1L);
                    assertThat(readNativeValue(BIGINT, value.getRawFieldBlock(1), value.getRawIndex()))
                            .isEqualTo(2L);
                });

        assertThat(optimize(
                new Row(ImmutableList.of(new Constant(BIGINT, 1L), new Reference(BIGINT, "x")))))
                .isEmpty();
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new EvaluateRow().apply(expression, testSession(), ImmutableMap.of());
    }
}
