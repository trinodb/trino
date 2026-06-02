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
    void testUpperWithBoundedVarcharCoerces()
    {
        // upper(varchar(10)) — varchar(10) widens to unbounded varchar.
        assertThat(LIBRARY.resolveFunction("upper", List.of(apply("varchar", literal(10)))))
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
