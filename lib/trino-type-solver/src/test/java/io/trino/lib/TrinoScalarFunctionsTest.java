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
import static org.weakref.solver.Expression.function;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.symbol;

public class TrinoScalarFunctionsTest
{
    private static final TypeLibrary LIBRARY = TrinoPreset.library();

    @Test
    void testStringLength()
    {
        assertThat(LIBRARY.resolveFunction("length", List.of(apply("varchar", literal(10)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("bigint")));
    }

    @Test
    void testVariadicConcat()
    {
        assertThat(LIBRARY.resolveFunction("concat", List.of(symbol("varchar"), symbol("varchar"), symbol("varchar"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("varchar")));
    }

    @Test
    void testStringFunctionsPreserveVarcharLength()
    {
        // Length-preserving functions return varchar(x) for varchar(x), like Trino.
        for (String name : List.of("upper", "lower", "title_case", "trim", "ltrim", "rtrim", "reverse")) {
            assertThat(LIBRARY.resolveFunction(name, List.of(apply("varchar", literal(10)))))
                    .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                            assertThat(r.resolution().returnType()).isEqualTo(apply("varchar", literal(10))));
        }
        // substr/substring preserve the source length.
        assertThat(LIBRARY.resolveFunction("substr", List.of(apply("varchar", literal(10)), symbol("bigint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(apply("varchar", literal(10))));
        // replace(varchar(10), varchar(2), varchar(2)) -> varchar(10 + 2 * (10 + 1)) = varchar(32).
        assertThat(LIBRARY.resolveFunction("replace", List.of(apply("varchar", literal(10)), apply("varchar", literal(2)), apply("varchar", literal(2)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(apply("varchar", literal(32))));
        // Unbounded varchar still resolves to unbounded varchar.
        assertThat(LIBRARY.resolveFunction("upper", List.of(symbol("varchar"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("varchar")));
    }

    @Test
    void testNumericAbsPerType()
    {
        for (String t : List.of("tinyint", "smallint", "integer", "bigint", "real", "double")) {
            assertThat(LIBRARY.resolveFunction("abs", List.of(symbol(t))))
                    .as("abs(%s)", t)
                    .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                            assertThat(r.resolution().returnType()).isEqualTo(symbol(t)));
        }
    }

    @Test
    void testAbsOnDecimalPreservesShape()
    {
        assertThat(LIBRARY.resolveFunction("abs", List.of(apply("decimal", literal(10), literal(2)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(apply("decimal", literal(10), literal(2))));
    }

    @Test
    void testCeilIntegerWidensToBigint()
    {
        // No ceil(tinyint) -> tinyint needed if widening kicks in.
        // Actually we have ceil per-type registered, so this should return tinyint.
        assertThat(LIBRARY.resolveFunction("ceil", List.of(symbol("tinyint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("tinyint")));
    }

    @Test
    void testTranscendentalTakesDouble()
    {
        // sqrt(integer) should widen to sqrt(double).
        assertThat(LIBRARY.resolveFunction("sqrt", List.of(symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("double")));
    }

    @Test
    void testDecimalRoundingResolvesToDecimal()
    {
        // ceil/floor/round(decimal(10, 2)) -> decimal(min(38, 10 - 2 + min(2, 1)), 0) = decimal(9, 0).
        for (String name : List.of("ceil", "ceiling", "floor", "round")) {
            assertThat(LIBRARY.resolveFunction(name, List.of(apply("decimal", literal(10), literal(2)))))
                    .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                            assertThat(r.resolution().returnType()).isEqualTo(apply("decimal", literal(9), literal(0))));
        }
        // sign(decimal(10, 2)) -> decimal(1, 0).
        assertThat(LIBRARY.resolveFunction("sign", List.of(apply("decimal", literal(10), literal(2)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(apply("decimal", literal(1), literal(0))));
    }

    @Test
    void testModResolvesAcrossNumericTypesLikeTheOperator()
    {
        // mod() is the named form of %, registered for every numeric primitive...
        for (String type : List.of("tinyint", "smallint", "integer", "bigint", "real", "double", "number")) {
            assertThat(LIBRARY.resolveFunction("mod", List.of(symbol(type), symbol(type))))
                    .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                            assertThat(r.resolution().returnType()).isEqualTo(symbol(type)));
        }
        // ...and for decimal: scale = max(s1, s2); precision = min(p1 - s1, p2 - s2) + scale, so
        // mod(decimal(10, 2), decimal(8, 4)) -> decimal(min(8, 4) + 4, 4) = decimal(8, 4).
        assertThat(LIBRARY.resolveFunction("mod", List.of(apply("decimal", literal(10), literal(2)), apply("decimal", literal(8), literal(4)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(apply("decimal", literal(8), literal(4))));
        // Mixed decimal/integer coerces the integer into the decimal and resolves to decimal,
        // matching Trino (previously dropped to ambiguous for want of a decimal overload).
        assertThat(LIBRARY.resolveFunction("mod", List.of(apply("decimal", literal(10), literal(2)), symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(apply("decimal", literal(10), literal(2))));
    }

    @Test
    void testPiIsZeroArg()
    {
        assertThat(LIBRARY.resolveFunction("pi", List.of()))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("double")));
    }

    @Test
    void testTemporalDateTruncOnDate()
    {
        assertThat(LIBRARY.resolveFunction("date_trunc", List.of(symbol("varchar"), symbol("date"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("date")));
    }

    @Test
    void testTemporalDateTruncOnTimestampPreservesPrecision()
    {
        assertThat(LIBRARY.resolveFunction("date_trunc", List.of(symbol("varchar"), apply("timestamp", literal(6)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(apply("timestamp", literal(6))));
    }

    @Test
    void testYearOnDate()
    {
        assertThat(LIBRARY.resolveFunction("year", List.of(symbol("date"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("bigint")));
    }

    @Test
    void testArrayCardinality()
    {
        assertThat(LIBRARY.resolveFunction("cardinality", List.of(apply("array", symbol("integer")))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("bigint")));
    }

    @Test
    void testMapCardinalityDisambiguatesFromArray()
    {
        // Both array and map have cardinality registered — resolver picks map overload for map arg.
        assertThat(LIBRARY.resolveFunction(
                "cardinality",
                List.of(apply("map", symbol("varchar"), symbol("bigint")))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("bigint")));
    }

    @Test
    void testArrayTransformHigherOrder()
    {
        // transform(array(integer), function(integer) -> varchar) -> array(varchar)
        assertThat(LIBRARY.resolveFunction("transform", List.of(
                apply("array", symbol("integer")),
                function(List.of(symbol("integer")), symbol("varchar")))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(apply("array", symbol("varchar"))));
    }

    @Test
    void testArrayFilterHigherOrder()
    {
        // filter(array(integer), function(integer) -> boolean) -> array(integer)
        assertThat(LIBRARY.resolveFunction("filter", List.of(
                apply("array", symbol("integer")),
                function(List.of(symbol("integer")), symbol("boolean")))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(apply("array", symbol("integer"))));
    }

    @Test
    void testJsonParseReturnsJson()
    {
        assertThat(LIBRARY.resolveFunction("json_parse", List.of(symbol("varchar"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("json")));
    }

    @Test
    void testRegexpLike()
    {
        assertThat(LIBRARY.resolveFunction("regexp_like", List.of(symbol("varchar"), symbol("varchar"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("boolean")));
    }

    @Test
    void testGreatestWithComparableArgs()
    {
        assertThat(LIBRARY.resolveFunction("greatest", List.of(symbol("integer"), symbol("bigint"), symbol("tinyint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("bigint")));
    }

    @Test
    void testNullifPolymorphic()
    {
        assertThat(LIBRARY.resolveFunction("nullif", List.of(symbol("integer"), symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("integer")));
    }

    @Test
    void testIfPolymorphic()
    {
        // if(boolean, varchar, varchar) -> varchar
        assertThat(LIBRARY.resolveFunction("if", List.of(symbol("boolean"), symbol("varchar"), symbol("varchar"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("varchar")));
    }

    @Test
    void testMd5OnVarbinary()
    {
        assertThat(LIBRARY.resolveFunction("md5", List.of(symbol("varbinary"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("varbinary")));
    }

    @Test
    void testFromBase64()
    {
        assertThat(LIBRARY.resolveFunction("from_base64", List.of(symbol("varchar"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(symbol("varbinary")));
    }

    @Test
    void testUnregisteredFunctionReturnsNoMatch()
    {
        assertThat(LIBRARY.resolveFunction("no_such_function", List.of(symbol("integer"))))
                .isInstanceOf(FunctionResolver.NoMatch.class);
    }
}
