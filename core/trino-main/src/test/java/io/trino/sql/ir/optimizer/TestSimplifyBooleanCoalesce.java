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
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.SimplifyBooleanCoalesce;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimplifyBooleanCoalesce
{
    private static final Reference X = new Reference(BOOLEAN, "x");
    private static final Reference Y = new Reference(BOOLEAN, "y");

    @Test
    void testFalseFallback()
    {
        // Coalesce(v, false) -> $identical(v, true)
        assertThat(optimize(new Coalesce(ImmutableList.of(X, FALSE))))
                .isEqualTo(Optional.of(comparison(IDENTICAL, X, TRUE)));

        Expression predicate = comparison(EQUAL, new Reference(BIGINT, "a"), new Constant(BIGINT, 1L));
        assertThat(optimize(new Coalesce(ImmutableList.of(predicate, FALSE))))
                .isEqualTo(Optional.of(comparison(IDENTICAL, predicate, TRUE)));
    }

    @Test
    void testTrueFallback()
    {
        // Coalesce(v, true) -> $not($identical(v, false))
        assertThat(optimize(new Coalesce(ImmutableList.of(X, TRUE))))
                .isEqualTo(Optional.of(not(PLANNER_CONTEXT.getMetadata(), comparison(IDENTICAL, X, FALSE))));
    }

    @Test
    void testMultipleOperands()
    {
        // The leading operands stay in a Coalesce; only the boolean fallback is dissolved.
        assertThat(optimize(new Coalesce(ImmutableList.of(X, Y, FALSE))))
                .isEqualTo(Optional.of(comparison(IDENTICAL, new Coalesce(ImmutableList.of(X, Y)), TRUE)));
    }

    @Test
    void testDoesNotApply()
    {
        // Last operand isn't a boolean constant.
        assertThat(optimize(new Coalesce(ImmutableList.of(X, Y))))
                .isEmpty();

        // Non-boolean Coalesce.
        assertThat(optimize(new Coalesce(ImmutableList.of(new Reference(BIGINT, "a"), new Constant(BIGINT, 0L)))))
                .isEmpty();
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new SimplifyBooleanCoalesce(PLANNER_CONTEXT).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }
}
