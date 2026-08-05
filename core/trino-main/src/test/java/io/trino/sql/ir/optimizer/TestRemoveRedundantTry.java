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
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.RemoveRedundantTry;
import io.trino.sql.planner.BuiltinFunctionCallBuilder;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.operator.scalar.TryFunction.TRY_FUNCTION_NAME;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRemoveRedundantTry
{
    private static final ResolvedFunction EQUAL_BIGINT = PLANNER_CONTEXT.getMetadata()
            .resolveOperator(EQUAL, ImmutableList.of(BIGINT, BIGINT));

    @Test
    void testInfallibleBodyIsUnwrapped()
    {
        // a constant body can never fail, so $try is redundant
        Expression body = new Constant(BIGINT, 1L);
        assertThat(optimize(tryExpression(body)))
                .isEqualTo(Optional.of(body));
    }

    @Test
    void testFallibleBodyIsLeftUnchanged()
    {
        // cast(varchar as bigint) can fail (bad input), so the $try must be preserved
        Expression body = new Cast(new Reference(VARCHAR, "a"), BIGINT);
        assertThat(optimize(tryExpression(body)))
                .isEqualTo(Optional.empty());
    }

    @Test
    void testCapturedInfallibleBodyIsUnwrappedWithLet()
    {
        Symbol parameter = new Symbol(BIGINT, "a$captured");
        Expression value = new Reference(BIGINT, "a");
        Expression body = new Call(EQUAL_BIGINT, ImmutableList.of(parameter.toSymbolReference(), parameter.toSymbolReference()));
        Bind bind = new Bind(ImmutableList.of(value), new Lambda(ImmutableList.of(parameter), body));

        Expression expression = BuiltinFunctionCallBuilder.resolve(PLANNER_CONTEXT.getMetadata())
                .setName(TRY_FUNCTION_NAME)
                .addArgument(bind.type(), bind)
                .build();

        assertThat(optimize(expression))
                .isEqualTo(Optional.of(new Let(parameter, value, body)));
    }

    @Test
    void testCapturedInfallibleBodyWithMultipleCapturesIsUnwrappedWithNestedLets()
    {
        Symbol firstParameter = new Symbol(BIGINT, "a$captured");
        Symbol secondParameter = new Symbol(BIGINT, "b$captured");
        Expression firstValue = new Reference(BIGINT, "a");
        Expression secondValue = new Reference(BIGINT, "b");
        Expression body = new Call(EQUAL_BIGINT, ImmutableList.of(firstParameter.toSymbolReference(), secondParameter.toSymbolReference()));
        Bind bind = new Bind(
                ImmutableList.of(firstValue, secondValue),
                new Lambda(ImmutableList.of(firstParameter, secondParameter), body));

        Expression expression = BuiltinFunctionCallBuilder.resolve(PLANNER_CONTEXT.getMetadata())
                .setName(TRY_FUNCTION_NAME)
                .addArgument(bind.type(), bind)
                .build();

        assertThat(optimize(expression))
                .isEqualTo(Optional.of(new Let(firstParameter, firstValue, new Let(secondParameter, secondValue, body))));
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
        return new RemoveRedundantTry(PLANNER_CONTEXT).apply(expression, testSession(), emptySymbolAllocator(), ImmutableMap.of());
    }
}
