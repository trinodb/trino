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
import io.trino.spi.block.Block;
import io.trino.sql.ir.Array;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.EvaluateArray;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluateArray
{
    @Test
    void test()
    {
        assertThat(optimize(
                new Array(BIGINT, ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)))))
                .isPresent()
                .satisfies(result -> {
                    Constant constant = (Constant) result.get();
                    assertThat(BIGINT.getLong((Block) constant.value(), 0))
                            .isEqualTo(1L);
                });

        assertThat(optimize(
                new Array(BIGINT, ImmutableList.of(new Constant(BIGINT, 1L), new Reference(BIGINT, "x")))))
                .isEmpty();
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new EvaluateArray().apply(expression, testSession(), ImmutableMap.of());
    }
}
