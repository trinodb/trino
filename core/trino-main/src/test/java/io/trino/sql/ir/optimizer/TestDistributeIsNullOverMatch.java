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
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.DistributeIsNullOverMatch;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDistributeIsNullOverMatch
{
    @Test
    void testDistribute()
    {
        assertThat(optimize(
                new IsNull(new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                equalityClause(new Constant(BIGINT, 1L), new Reference(VARCHAR, "r1")),
                                equalityClause(new Constant(BIGINT, 2L), new Constant(VARCHAR, null))),
                        new Reference(VARCHAR, "d")))))
                .isEqualTo(Optional.of(new Match(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                equalityClause(new Constant(BIGINT, 1L), new IsNull(new Reference(VARCHAR, "r1"))),
                                equalityClause(new Constant(BIGINT, 2L), new IsNull(new Constant(VARCHAR, null)))),
                        new IsNull(new Reference(VARCHAR, "d")))));
    }

    @Test
    void testDoesNotFire()
    {
        assertThat(optimize(new IsNull(new Reference(VARCHAR, "a"))))
                .describedAs("not a match expression")
                .isEqualTo(Optional.empty());
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new DistributeIsNullOverMatch().apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }

    private static MatchClause equalityClause(Expression value, Expression result)
    {
        return IrExpressions.equalityClause(PLANNER_CONTEXT.getMetadata(), new Symbol(value.type(), "operand"), value, result);
    }
}
