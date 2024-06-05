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
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.EvaluateCall;
import io.trino.sql.planner.Symbol;
import io.trino.type.FunctionType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEvaluateCall
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(new InternalFunctionBundle(APPLY_FUNCTION));
    private static final ResolvedFunction ADD_DOUBLE = FUNCTIONS.resolveOperator(ADD, ImmutableList.of(DOUBLE, DOUBLE));
    private static final ResolvedFunction DIVIDE_BIGINT = FUNCTIONS.resolveOperator(DIVIDE, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", ImmutableList.of());
    private static final ResolvedFunction APPLY = FUNCTIONS.resolveFunction("apply", fromTypes(BIGINT, new FunctionType(ImmutableList.of(BIGINT), BOOLEAN)));

    @Test
    void test()
    {
        assertThat(optimize(
                new Call(ADD_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "x"), new Reference(DOUBLE, "y")))))
                .describedAs("non-constant arguments")
                .isEmpty();

        assertThat(optimize(
                new Call(ADD_DOUBLE, ImmutableList.of(
                        new Call(RANDOM, ImmutableList.of()),
                        new Call(RANDOM, ImmutableList.of())))))
                .describedAs("non-deterministic arguments")
                .isEmpty();

        assertThat(optimize(
                new Call(ADD_DOUBLE, ImmutableList.of(
                        new Constant(DOUBLE, 1.0),
                        new Constant(DOUBLE, 2.0)))))
                .describedAs("non-deterministic arguments")
                .isEqualTo(Optional.of(new Constant(DOUBLE, 3.0)));

        assertThat(optimize(
                new Call(APPLY, ImmutableList.of(
                        new Constant(BIGINT, 1L),
                        new Lambda(
                                ImmutableList.of(new Symbol(BIGINT, "x")),
                                new Comparison(EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)))))))
                .describedAs("higher-order function")
                .isEqualTo(Optional.of(TRUE));

        assertThat(optimize(
                new Call(DIVIDE_BIGINT, ImmutableList.of(
                        new Constant(BIGINT, 1L),
                        new Constant(BIGINT, 0L)))))
                .describedAs("expression that fails")
                .isEmpty();
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new EvaluateCall(FUNCTIONS.getPlannerContext()).apply(expression, testSession(), ImmutableMap.of());
    }
}
