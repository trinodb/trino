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
import io.trino.spi.type.FunctionType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.PushDownTry;
import io.trino.sql.planner.BuiltinFunctionCallBuilder;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.operator.scalar.TryFunction.TRY_FUNCTION_NAME;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.IDENTICAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPushDownTry
{
    private static final ResolvedFunction EQUAL_BIGINT = PLANNER_CONTEXT.getMetadata()
            .resolveOperator(EQUAL, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction IDENTICAL_BIGINT = PLANNER_CONTEXT.getMetadata()
            .resolveOperator(IDENTICAL, ImmutableList.of(BIGINT, BIGINT));

    @Test
    void testNarrowsToFallibleArgument()
    {
        // $try(() -> cast(v as bigint) = a) -> $try(() -> cast(v as bigint)) = a
        // EQUAL is infallible and strict, so the $try moves onto the only fallible argument
        Expression fallible = new Cast(new Reference(VARCHAR, "v"), BIGINT);
        Expression infallible = new Reference(BIGINT, "a");

        assertThat(optimize(tryExpression(new Call(EQUAL_BIGINT, ImmutableList.of(fallible, infallible)))))
                .isEqualTo(Optional.of(new Call(EQUAL_BIGINT, ImmutableList.of(tryExpression(fallible), infallible))));
    }

    @Test
    void testNarrowsThroughCast()
    {
        // cast(bigint as varchar) never fails, so $try(() -> cast(cast(v as bigint) as varchar))
        // -> cast($try(() -> cast(v as bigint)) as varchar)
        Expression fallible = new Cast(new Reference(VARCHAR, "v"), BIGINT);

        assertThat(optimize(tryExpression(new Cast(fallible, VARCHAR))))
                .isEqualTo(Optional.of(new Cast(tryExpression(fallible), VARCHAR)));
    }

    @Test
    void testFallibleRootIsLeftUnchanged()
    {
        // cast(varchar as bigint) can fail on its own; dropping the $try would let it escape
        Expression body = new Cast(new Reference(VARCHAR, "v"), BIGINT);
        assertThat(optimize(tryExpression(body)))
                .isEqualTo(Optional.empty());
    }

    @Test
    void testInfallibleBodyIsLeftToRemoveRedundantTry()
    {
        // a = b with both arguments infallible has nothing to narrow onto
        Expression body = new Call(EQUAL_BIGINT, ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")));
        assertThat(optimize(tryExpression(body)))
                .isEqualTo(Optional.empty());
    }

    @Test
    void testNonStrictArgumentPositionIsLeftUnchanged()
    {
        // IDENTICAL (IS NOT DISTINCT FROM) never fails but is called-on-null: a NULL argument yields a
        // non-null boolean, so a pushed $try would change the result. The narrowing must not fire.
        Expression fallible = new Cast(new Reference(VARCHAR, "v"), BIGINT);
        Expression body = new Call(IDENTICAL_BIGINT, ImmutableList.of(fallible, new Constant(BIGINT, 0L)));
        assertThat(optimize(tryExpression(body)))
                .isEqualTo(Optional.empty());
    }

    @Test
    void testNullAbsorbingConstructIsLeftUnchanged()
    {
        // coalesce can absorb a NULL into a non-null result, so it is not a narrowing target
        Expression fallible = new Cast(new Reference(VARCHAR, "v"), BIGINT);
        Expression body = new Coalesce(fallible, new Constant(BIGINT, 0L));
        assertThat(optimize(tryExpression(body)))
                .isEqualTo(Optional.empty());
    }

    private static Expression tryExpression(Expression body)
    {
        return BuiltinFunctionCallBuilder.resolve(PLANNER_CONTEXT.getMetadata())
                .setName(TRY_FUNCTION_NAME)
                .addArgument(new FunctionType(ImmutableList.of(), body.type()), new Lambda(ImmutableList.of(), body))
                .build();
    }

    private static Optional<Expression> optimize(Expression expression)
    {
        return new PushDownTry(PLANNER_CONTEXT).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }
}
