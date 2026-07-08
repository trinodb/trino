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
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.rule.DistributeIsNullOverCase;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.TestingIr.nullIf;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDistributeIsNullOverCase
{
    @Test
    void testDistribute()
    {
        assertThat(optimize(
                new IsNull(new Case(
                        ImmutableList.of(
                                new WhenClause(new Reference(BOOLEAN, "a"), new Reference(VARCHAR, "r1")),
                                new WhenClause(new Reference(BOOLEAN, "b"), new Constant(VARCHAR, null))),
                        new Reference(VARCHAR, "d")))))
                .isEqualTo(Optional.of(new Case(
                        ImmutableList.of(
                                new WhenClause(new Reference(BOOLEAN, "a"), new IsNull(new Reference(VARCHAR, "r1"))),
                                new WhenClause(new Reference(BOOLEAN, "b"), new IsNull(new Constant(VARCHAR, null)))),
                        new IsNull(new Reference(VARCHAR, "d")))));
    }

    @Test
    void testDoesNotFire()
    {
        assertThat(optimize(new IsNull(nullIf(new SymbolAllocator(), new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)))))
                .describedAs("desugared NULLIF is preserved for connector pushdown")
                .isEqualTo(Optional.empty());

        assertThat(optimize(new IsNull(new Reference(VARCHAR, "a"))))
                .describedAs("not a case expression")
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new DistributeIsNullOverCase().apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }
}
