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
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.SimplifyStackedArithmeticNegation;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimplifyStackedArithmeticNegation
{
    @Test
    void test()
    {
        ResolvedFunction negation = new TestingFunctionResolution().resolveOperator(NEGATION, ImmutableList.of(BIGINT));

        assertThat(optimize(
                new Call(negation, ImmutableList.of(
                        new Call(negation, ImmutableList.of(new Reference(BIGINT, "a")))))))
                .isEqualTo(Optional.of(new Reference(BIGINT, "a")));

        assertThat(optimize(
                new Call(negation, ImmutableList.of(new Reference(BIGINT, "a")))))
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new SimplifyStackedArithmeticNegation().apply(expression, testSession(), ImmutableMap.of());
    }
}
