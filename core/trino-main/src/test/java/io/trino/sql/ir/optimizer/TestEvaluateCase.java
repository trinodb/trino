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
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.rule.EvaluateCase;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluateCase
{
    @Test
    void test()
    {
        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(new Reference(BOOLEAN, "x"), new Reference(VARCHAR, "a"))),
                        new Reference(VARCHAR, "b"))))
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(TRUE, new Reference(VARCHAR, "a"))),
                        new Reference(VARCHAR, "b"))))
                .describedAs("match")
                .isEqualTo(Optional.of(new Reference(VARCHAR, "a")));

        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(FALSE, new Reference(VARCHAR, "a"))),
                        new Reference(VARCHAR, "b"))))
                .describedAs("no match - false")
                .isEqualTo(Optional.of(new Reference(VARCHAR, "b")));

        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(NULL_BOOLEAN, new Reference(VARCHAR, "a"))),
                        new Reference(VARCHAR, "b"))))
                .describedAs("no match - null")
                .isEqualTo(Optional.of(new Reference(VARCHAR, "b")));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new EvaluateCase().apply(expression, testSession(), ImmutableMap.of());
    }
}
