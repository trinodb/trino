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
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.SimplifyRedundantTryCast;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.scalar.TryCastFunction.TRY_CAST_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimplifyRedundantTryCast
{
    @Test
    void testInfallibleCoercionIsLoweredToCast()
    {
        // cast(integer as bigint) never fails, so $try_cast is equivalent to a plain Cast
        assertThat(optimize(tryCast(new Reference(INTEGER, "a"), BIGINT)))
                .isEqualTo(Optional.of(new Cast(new Reference(INTEGER, "a"), BIGINT)));
    }

    @Test
    void testFallibleCoercionIsLeftUnchanged()
    {
        // cast(varchar as integer) can fail (bad input), so the $try_cast must be preserved
        assertThat(optimize(tryCast(new Reference(VARCHAR, "a"), INTEGER)))
                .isEqualTo(Optional.empty());
    }

    private static Expression tryCast(Expression value, Type targetType)
    {
        ResolvedFunction function = PLANNER_CONTEXT.getMetadata().getCoercion(builtinFunctionName(TRY_CAST_FUNCTION_NAME), value.type(), targetType);
        return new Call(function, ImmutableList.of(value));
    }

    private static Optional<Expression> optimize(Expression expression)
    {
        return new SimplifyRedundantTryCast(PLANNER_CONTEXT).apply(expression, testSession(), emptySymbolAllocator(), ImmutableMap.of());
    }
}
