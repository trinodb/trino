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
import static org.weakref.solver.Expression.symbol;

public class TrinoOperatorsTest
{
    private static final TypeLibrary LIBRARY = TrinoPreset.library();

    @Test
    void testEqualsOnSameTypeReturnsBoolean()
    {
        assertThat(LIBRARY.resolveFunction("=", List.of(symbol("integer"), symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("boolean")));
    }

    @Test
    void testEqualsWithWideningCoercesToCommonType()
    {
        assertThat(LIBRARY.resolveFunction("=", List.of(symbol("integer"), symbol("bigint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome -> {
                    assertThat(outcome.resolution().returnType()).isEqualTo(symbol("boolean"));
                    assertThat(outcome.resolution().typeBindings()).containsEntry("@T", symbol("bigint"));
                });
    }

    @Test
    void testLessThanSelectsOrderableOverload()
    {
        assertThat(LIBRARY.resolveFunction("<", List.of(symbol("tinyint"), symbol("bigint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().typeBindings()).containsEntry("@T", symbol("bigint")));
    }

    @Test
    void testAndRequiresBoolean()
    {
        assertThat(LIBRARY.resolveFunction("and", List.of(symbol("boolean"), symbol("boolean"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("boolean")));
    }

    @Test
    void testNotOnBoolean()
    {
        assertThat(LIBRARY.resolveFunction("not", List.of(symbol("boolean"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("boolean")));
    }

    @Test
    void testAdditionOnSameTypeReturnsThatType()
    {
        assertThat(LIBRARY.resolveFunction("+", List.of(symbol("integer"), symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("integer")));
    }

    @Test
    void testAdditionWithMixedTypesPicksWiderOverload()
    {
        assertThat(LIBRARY.resolveFunction("+", List.of(symbol("integer"), symbol("bigint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("bigint")));
    }

    @Test
    void testAdditionPrefersIntegerOverDouble()
    {
        assertThat(LIBRARY.resolveFunction("+", List.of(symbol("tinyint"), symbol("tinyint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("tinyint")));
    }

    @Test
    void testCoalesceBindsCommonSupertype()
    {
        assertThat(LIBRARY.resolveFunction("coalesce", List.of(symbol("integer"), symbol("bigint"), symbol("tinyint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome -> {
                    assertThat(outcome.resolution().returnType()).isEqualTo(symbol("bigint"));
                    assertThat(outcome.resolution().typeBindings()).containsEntry("@T", symbol("bigint"));
                });
    }

    @Test
    void testCoalesceWithNullArgumentReturnsConcreteType()
    {
        assertThat(LIBRARY.resolveFunction("coalesce", List.of(symbol("unknown"), symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome -> {
                    assertThat(outcome.resolution().returnType()).isEqualTo(symbol("integer"));
                    assertThat(outcome.resolution().typeBindings()).containsEntry("@T", symbol("integer"));
                });
    }

    @Test
    void testCoalesceRequiresAtLeastOneArgument()
    {
        assertThat(LIBRARY.resolveFunction("coalesce", List.of()))
                .isInstanceOf(FunctionResolver.NoMatch.class);
    }

    @Test
    void testDecimalAdditionComputesCommonSupertype()
    {
        // decimal(10, 2) + decimal(5, 3) = decimal(12, 3)
        //   scale = max(2, 3) = 3; p-s = max(10-2, 5-3) + 1 = 9; p = 12.
        assertThat(LIBRARY.resolveFunction("+",
                List.of(
                        org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(10), org.weakref.solver.Expression.literal(2)),
                        org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(5), org.weakref.solver.Expression.literal(3)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(
                                org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(12), org.weakref.solver.Expression.literal(3))));
    }

    @Test
    void testDecimalMultiplicationAddsPrecisionAndScale()
    {
        // decimal(10, 2) * decimal(5, 3):
        //   naturalPrecision = p1 + p2 = 15
        //   intDigits = 15 - (2 + 3) = 10
        //   precision = min(38, 15) = 15
        //   scale = min(5, max(6, 38 - 10)) = min(5, 28) = 5
        assertThat(LIBRARY.resolveFunction("*",
                List.of(
                        org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(10), org.weakref.solver.Expression.literal(2)),
                        org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(5), org.weakref.solver.Expression.literal(3)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(
                                org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(15), org.weakref.solver.Expression.literal(5))));

        // decimal(10, 2) * decimal(10, 2) = decimal(20, 4) — Trino's p1 + p2 precision, not p1 + p2 + 1.
        assertThat(LIBRARY.resolveFunction("*",
                List.of(
                        org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(10), org.weakref.solver.Expression.literal(2)),
                        org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(10), org.weakref.solver.Expression.literal(2)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(
                                org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(20), org.weakref.solver.Expression.literal(4))));
    }

    @Test
    void testVarcharConcatenationAddsLengths()
    {
        // varchar(5) || varchar(10) = varchar(15)
        assertThat(LIBRARY.resolveFunction("||",
                List.of(
                        org.weakref.solver.Expression.apply("varchar", org.weakref.solver.Expression.literal(5)),
                        org.weakref.solver.Expression.apply("varchar", org.weakref.solver.Expression.literal(10)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(
                                org.weakref.solver.Expression.apply("varchar", org.weakref.solver.Expression.literal(15))));
    }

    @Test
    void testCharConcatenationAddsLengths()
    {
        assertThat(LIBRARY.resolveFunction("||",
                List.of(
                        org.weakref.solver.Expression.apply("char", org.weakref.solver.Expression.literal(5)),
                        org.weakref.solver.Expression.apply("char", org.weakref.solver.Expression.literal(10)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(
                                org.weakref.solver.Expression.apply("char", org.weakref.solver.Expression.literal(15))));
    }

    @Test
    void testModuloOnIntegers()
    {
        assertThat(LIBRARY.resolveFunction("%", List.of(symbol("integer"), symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("integer")));
    }

    @Test
    void testDecimalMultiplicationOverflowClampsScale()
    {
        // decimal(30, 5) * decimal(25, 10):
        //   naturalPrecision = 30 + 25 + 1 = 56
        //   intDigits = 56 - (5 + 10) = 41 (> 32)
        //   precision = min(38, 56) = 38
        //   scale = min(15, max(6, 38 - 41)) = min(15, 6) = 6
        assertThat(LIBRARY.resolveFunction("*",
                List.of(
                        org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(30), org.weakref.solver.Expression.literal(5)),
                        org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(25), org.weakref.solver.Expression.literal(10)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(
                                org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(38), org.weakref.solver.Expression.literal(6))));
    }

    @Test
    void testDecimalDivisionReturnType()
    {
        // decimal(10, 2) / decimal(5, 3):
        //   scale = max(6, s1 + p2 + 1) = max(6, 2 + 5 + 1) = 8
        //   precision = min(38, p1 - s1 + s2 + scale) = min(38, 10 - 2 + 3 + 8) = 19
        assertThat(LIBRARY.resolveFunction("/",
                List.of(
                        org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(10), org.weakref.solver.Expression.literal(2)),
                        org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(5), org.weakref.solver.Expression.literal(3)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(
                                org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(19), org.weakref.solver.Expression.literal(8))));
    }

    @Test
    void testUnaryNegateOnInteger()
    {
        assertThat(LIBRARY.resolveFunction("-", List.of(symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("integer")));
    }

    @Test
    void testUnaryNegateOnDecimalPreservesShape()
    {
        assertThat(LIBRARY.resolveFunction(
                "-",
                List.of(org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(10), org.weakref.solver.Expression.literal(2)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(
                                org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(10), org.weakref.solver.Expression.literal(2))));
    }

    @Test
    void testBinarySubtractStillResolves()
    {
        // Ensure unary `-` doesn't shadow binary `-`.
        assertThat(LIBRARY.resolveFunction("-", List.of(symbol("integer"), symbol("bigint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("bigint")));
    }

    @Test
    void testDecimalDivisionClampsScaleWhenIntegerDigitsExceed32()
    {
        // decimal(38, 0) / decimal(38, 38):
        //   integerDigits = p1 - s1 + s2 = 38 - 0 + 38 = 76 (> 32)
        //   naturalScale = max(6, s1 + p2 + 1) = max(6, 0 + 38 + 1) = 39
        //   scale = min(naturalScale, max(6, 38 - integerDigits)) = min(39, max(6, -38)) = min(39, 6) = 6
        //   precision = min(38, integerDigits + naturalScale) = min(38, 115) = 38
        assertThat(LIBRARY.resolveFunction("/",
                List.of(
                        org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(38), org.weakref.solver.Expression.literal(0)),
                        org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(38), org.weakref.solver.Expression.literal(38)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(
                                org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(38), org.weakref.solver.Expression.literal(6))));
    }

    @Test
    void testDecimalModuloReturnType()
    {
        // decimal(10, 2) % decimal(5, 3):
        //   scale = max(2, 3) = 3
        //   precision = min(p1 - s1, p2 - s2) + scale = min(8, 2) + 3 = 5
        assertThat(LIBRARY.resolveFunction("%",
                List.of(
                        org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(10), org.weakref.solver.Expression.literal(2)),
                        org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(5), org.weakref.solver.Expression.literal(3)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(
                                org.weakref.solver.Expression.apply("decimal", org.weakref.solver.Expression.literal(5), org.weakref.solver.Expression.literal(3))));
    }

    @Test
    void testEqualityAcceptsComparableJson()
    {
        // json is comparable (it supports equality, like the engine's JsonType) but not orderable.
        assertThat(LIBRARY.resolveFunction("=", List.of(symbol("json"), symbol("json"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("boolean")));
    }

    @Test
    void testOrderingRejectsNonOrderableJson()
    {
        assertThat(LIBRARY.resolveFunction("<", List.of(symbol("json"), symbol("json"))))
                .isInstanceOf(FunctionResolver.NoMatch.class);
    }

    @Test
    void testEqualityAcceptsComparableMapButOrderingRejectsIt()
    {
        // A map with comparable key and value is comparable...
        assertThat(LIBRARY.resolveFunction(
                "=",
                List.of(apply("map", symbol("integer"), symbol("integer")), apply("map", symbol("integer"), symbol("integer")))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("boolean")));

        // ...but a map is never orderable.
        assertThat(LIBRARY.resolveFunction(
                "<",
                List.of(apply("map", symbol("integer"), symbol("integer")), apply("map", symbol("integer"), symbol("integer")))))
                .isInstanceOf(FunctionResolver.NoMatch.class);
    }

    @Test
    void testEqualityRecursesThroughArrayElements()
    {
        // array(integer) is comparable because integer is...
        assertThat(LIBRARY.resolveFunction(
                "=",
                List.of(apply("array", symbol("integer")), apply("array", symbol("integer")))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("boolean")));

        // ...and array(json) is too, because json supports equality.
        assertThat(LIBRARY.resolveFunction(
                "=",
                List.of(apply("array", symbol("json")), apply("array", symbol("json")))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("boolean")));
    }
}
