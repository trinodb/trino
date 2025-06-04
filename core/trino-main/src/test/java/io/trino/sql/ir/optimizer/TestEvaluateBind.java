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
import io.trino.sql.ir.Array;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.EvaluateBind;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluateBind
{
    @Test
    void test()
    {
        assertThat(optimize(
                new Bind(
                        ImmutableList.of(new Constant(BIGINT, 1L)),
                        new Lambda(
                                ImmutableList.of(new Symbol(BIGINT, "a")),
                                new Reference(BIGINT, "a")))))
                .isEqualTo(Optional.of(new Lambda(
                        ImmutableList.of(),
                        new Constant(BIGINT, 1L))));

        assertThat(optimize(
                new Bind(
                        ImmutableList.of(new Constant(BIGINT, 1L), new Reference(BIGINT, "x")),
                        new Lambda(
                                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b")),
                                new Array(BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")))))))
                .isEqualTo(Optional.of(new Bind(
                        ImmutableList.of(new Reference(BIGINT, "x")),
                        new Lambda(
                                ImmutableList.of(new Symbol(BIGINT, "b")),
                                new Array(BIGINT, ImmutableList.of(new Constant(BIGINT, 1L), new Reference(BIGINT, "b")))))));

        assertThat(optimize(
                new Bind(
                        ImmutableList.of(new Constant(BIGINT, 1L), new Reference(BIGINT, "x")),
                        new Lambda(
                                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "b"), new Symbol(BIGINT, "c")),
                                new Array(BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"), new Reference(BIGINT, "c")))))))
                .isEqualTo(Optional.of(new Bind(
                        ImmutableList.of(new Reference(BIGINT, "x")),
                        new Lambda(
                                ImmutableList.of(new Symbol(BIGINT, "b"), new Symbol(BIGINT, "c")),
                                new Array(BIGINT, ImmutableList.of(new Constant(BIGINT, 1L), new Reference(BIGINT, "b"), new Reference(BIGINT, "c")))))));

        assertThat(optimize(
                new Bind(
                        ImmutableList.of(new Reference(BIGINT, "x")),
                        new Lambda(
                                ImmutableList.of(new Symbol(BIGINT, "a")),
                                new Reference(BIGINT, "a")))))
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new EvaluateBind().apply(expression, testSession(), ImmutableMap.of());
    }
}
