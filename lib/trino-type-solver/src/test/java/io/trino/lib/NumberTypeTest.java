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
package io.trino.lib;

import org.junit.jupiter.api.Test;
import org.weakref.solver.FunctionResolver;
import org.weakref.solver.TypeLibrary;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.symbol;

public class NumberTypeTest
{
    private static final TypeLibrary LIBRARY = TrinoPreset.library();

    @Test
    void testIntegralWidensImplicitlyToNumber()
    {
        for (String integral : List.of("tinyint", "smallint", "integer", "bigint")) {
            assertThat(LIBRARY.typeSystem().coercionPlan(symbol(integral), symbol("number")))
                    .as("%s -> number", integral)
                    .isPresent();
        }
    }

    @Test
    void testDecimalWidensImplicitlyToNumber()
    {
        assertThat(LIBRARY.typeSystem().coercionPlan(apply("decimal", literal(10), literal(2)), symbol("number")))
                .isPresent();
    }

    @Test
    void testFloatingPointToNumberIsCastOnly()
    {
        for (String fp : List.of("real", "double")) {
            assertThat(LIBRARY.resolveCast(symbol(fp), symbol("number"))).as("cast %s -> number", fp).isPresent();
            assertThat(LIBRARY.typeSystem().coercionPlan(symbol(fp), symbol("number")))
                    .as("no implicit %s -> number", fp)
                    .isEmpty();
        }
    }

    @Test
    void testVarcharToNumberIsCastOnly()
    {
        assertThat(LIBRARY.resolveCast(apply("varchar", literal(32)), symbol("number"))).isPresent();
        assertThat(LIBRARY.typeSystem().coercionPlan(apply("varchar", literal(32)), symbol("number"))).isEmpty();
    }

    @Test
    void testNumberToNumericTargetsIsCastOnly()
    {
        for (String target : List.of("tinyint", "smallint", "integer", "bigint", "real", "double")) {
            assertThat(LIBRARY.resolveCast(symbol("number"), symbol(target)))
                    .as("cast number -> %s", target)
                    .isPresent();
            assertThat(LIBRARY.typeSystem().coercionPlan(symbol("number"), symbol(target)))
                    .as("no implicit number -> %s", target)
                    .isEmpty();
        }
    }

    @Test
    void testNumberToVarcharAndDecimalIsCastOnly()
    {
        assertThat(LIBRARY.resolveCast(symbol("number"), apply("varchar", literal(50)))).isPresent();
        assertThat(LIBRARY.typeSystem().coercionPlan(symbol("number"), apply("varchar", literal(50)))).isEmpty();

        assertThat(LIBRARY.resolveCast(symbol("number"), apply("decimal", literal(18), literal(4)))).isPresent();
        assertThat(LIBRARY.typeSystem().coercionPlan(symbol("number"), apply("decimal", literal(18), literal(4)))).isEmpty();
    }

    @Test
    void testArithmeticOperatorsOnNumber()
    {
        for (String op : List.of("+", "-", "*", "/", "%")) {
            assertThat(LIBRARY.resolveFunction(op, List.of(symbol("number"), symbol("number"))))
                    .as("number %s number", op)
                    .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                            assertThat(r.resolution().returnType()).isEqualTo(symbol("number")));
        }
    }

    @Test
    void testUnaryNegateOnNumber()
    {
        assertThat(LIBRARY.resolveFunction("-", List.of(symbol("number"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("number")));
    }

    @Test
    void testIntegerPlusNumberWidensToNumber()
    {
        // integer widens implicitly to number, so mixed arithmetic dispatches to number + number.
        assertThat(LIBRARY.resolveFunction("+", List.of(symbol("integer"), symbol("number"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("number")));
    }

    @Test
    void testDecimalPlusNumberWidensToNumber()
    {
        assertThat(LIBRARY.resolveFunction("+", List.of(apply("decimal", literal(10), literal(2)), symbol("number"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("number")));
    }

    @Test
    void testUnaryFunctionsOnNumber()
    {
        for (String f : List.of("abs", "ceil", "ceiling", "floor", "round", "truncate", "sign")) {
            assertThat(LIBRARY.resolveFunction(f, List.of(symbol("number"))))
                    .as("%s(number)", f)
                    .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                            assertThat(r.resolution().returnType()).isEqualTo(symbol("number")));
        }
    }

    @Test
    void testRoundNumberWithDecimals()
    {
        assertThat(LIBRARY.resolveFunction("round", List.of(symbol("number"), symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("number")));
    }

    @Test
    void testModOnNumber()
    {
        assertThat(LIBRARY.resolveFunction("mod", List.of(symbol("number"), symbol("number"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("number")));
    }

    @Test
    void testClassificationPredicatesOnNumber()
    {
        for (String f : List.of("is_nan", "is_finite", "is_infinite")) {
            assertThat(LIBRARY.resolveFunction(f, List.of(symbol("number"))))
                    .as("%s(number)", f)
                    .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                            assertThat(r.resolution().returnType()).isEqualTo(symbol("boolean")));
        }
    }

    @Test
    void testNumberIsComparableAndOrderable()
    {
        for (String op : List.of("=", "<>", "<", "<=", ">", ">=")) {
            assertThat(LIBRARY.resolveFunction(op, List.of(symbol("number"), symbol("number"))))
                    .as("number %s number", op)
                    .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                            assertThat(r.resolution().returnType()).isEqualTo(symbol("boolean")));
        }
    }
}
