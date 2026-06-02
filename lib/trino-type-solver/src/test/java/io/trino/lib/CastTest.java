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
import org.weakref.solver.RequireCastableTo;
import org.weakref.solver.TypeLibrary;
import org.weakref.solver.TypeScheme;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.function;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.symbol;
import static org.weakref.solver.Expression.variable;

public class CastTest
{
    private static final TypeLibrary LIBRARY = TrinoPreset.library();

    @Test
    void testImplicitCoercionIsAlsoACast()
    {
        // integer -> bigint is an implicit widening; must also resolve as cast.
        assertThat(LIBRARY.resolveCast(symbol("integer"), symbol("bigint"))).isPresent();
    }

    @Test
    void testVarcharToIntegerIsCastOnly()
    {
        // Cast exists.
        assertThat(LIBRARY.resolveCast(apply("varchar", literal(10)), symbol("integer"))).isPresent();
        // Implicit does not.
        assertThat(LIBRARY.typeSystem().coercionPlan(apply("varchar", literal(10)), symbol("integer"))).isEmpty();
    }

    @Test
    void testIntegerToVarcharIsCastOnly()
    {
        assertThat(LIBRARY.resolveCast(symbol("integer"), apply("varchar", literal(20)))).isPresent();
        assertThat(LIBRARY.typeSystem().coercionPlan(symbol("integer"), apply("varchar", literal(20)))).isEmpty();
    }

    @Test
    void testCharToVarcharIsCastOnly()
    {
        // char(10) -> varchar(20): cast allowed, implicit forbidden (asymmetric).
        assertThat(LIBRARY.resolveCast(apply("char", literal(10)), apply("varchar", literal(20)))).isPresent();
        assertThat(LIBRARY.typeSystem().coercionPlan(apply("char", literal(10)), apply("varchar", literal(20)))).isEmpty();
    }

    @Test
    void testNarrowingCastsAreCastOnly()
    {
        // bigint -> integer: cast allowed, implicit forbidden (only widening is implicit).
        assertThat(LIBRARY.resolveCast(symbol("bigint"), symbol("integer"))).isPresent();
        assertThat(LIBRARY.typeSystem().coercionPlan(symbol("bigint"), symbol("integer"))).isEmpty();
    }

    @Test
    void testUnrelatedTypesHaveNoCast()
    {
        // boolean -> date: no cast rule registered.
        assertThat(LIBRARY.resolveCast(symbol("boolean"), symbol("date"))).isEmpty();
    }

    @Test
    void testArrayOfCastableElementsCastsToJson()
    {
        // array(integer) -> json: valid because integer -> json exists.
        assertThat(LIBRARY.resolveCast(apply("array", symbol("integer")), symbol("json"))).isPresent();
    }

    @Test
    void testMapWithCastableKVCastsToJson()
    {
        // map(varchar(10), integer) -> json: K castableTo varchar AND V castableTo json.
        assertThat(LIBRARY.resolveCast(
                apply("map", apply("varchar", literal(10)), symbol("integer")),
                symbol("json")))
                .isPresent();
    }

    @Test
    void testArrayOfNonCastableElementsRejectsJsonCast()
    {
        // array(date) -> json: date has no cast to json → compositional constraint fails.
        assertThat(LIBRARY.resolveCast(apply("array", symbol("date")), symbol("json"))).isEmpty();
    }

    @Test
    void testCastableToConstraintPermitsCompatibleType()
    {
        // Synthetic function: f<T castableTo varchar(20)>(T) -> boolean.
        // Called with integer — integer is castable to varchar → succeeds.
        TypeScheme scheme = new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireCastableTo(variable("@T"), apply("varchar", literal(20)))),
                function(List.of(variable("@T")), symbol("boolean")));

        TypeLibrary library = TrinoPreset.install(TypeLibrary.builder())
                .registerFunction("requires_castable_to_varchar", scheme)
                .build();

        assertThat(library.resolveFunction("requires_castable_to_varchar", List.of(symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("boolean")));
    }

    @Test
    void testCastableToConstraintRejectsIncompatibleType()
    {
        // Synthetic function: f<T castableTo date>(T) -> boolean.
        // Called with boolean — boolean has no cast to date → rejected.
        TypeScheme scheme = new TypeScheme(
                List.of(variable("@T")),
                List.of(new RequireCastableTo(variable("@T"), symbol("date"))),
                function(List.of(variable("@T")), symbol("boolean")));

        TypeLibrary library = TrinoPreset.install(TypeLibrary.builder())
                .registerFunction("requires_castable_to_date", scheme)
                .build();

        assertThat(library.resolveFunction("requires_castable_to_date", List.of(symbol("boolean"))))
                .isNotInstanceOf(FunctionResolver.Resolved.class);  // Incomplete or NoMatch — both non-success
    }

    @Test
    void testUnboundedVarcharCastsLikeBounded()
    {
        // Unbounded varchar casts wherever bounded varchar(n) does (parse + format directions).
        assertThat(LIBRARY.resolveCast(symbol("varchar"), symbol("integer"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("integer"), symbol("varchar"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("varchar"), apply("decimal", literal(10), literal(2)))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("varchar"), apply("timestamp", literal(3)))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("varchar"), symbol("uuid"))).isPresent();
        // Still cast-only, not an implicit coercion.
        assertThat(LIBRARY.typeSystem().coercionPlan(symbol("varchar"), symbol("integer"))).isEmpty();
    }

    @Test
    void testVarcharParseCastsForVarbinaryAndTimeZoneTemporals()
    {
        assertThat(LIBRARY.resolveCast(apply("varchar", literal(10)), symbol("varbinary"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("varchar"), symbol("varbinary"))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("varchar", literal(10)), apply("timestamp_with_time_zone", literal(3)))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("varchar"), apply("time_with_time_zone", literal(6)))).isPresent();
    }

    @Test
    void testCharHasNoDirectJsonCast()
    {
        // Trino registers json casts only for varchar, not char; char goes via varchar explicitly.
        assertThat(LIBRARY.resolveCast(apply("char", literal(3)), symbol("json"))).isEmpty();
        assertThat(LIBRARY.resolveCast(symbol("json"), apply("char", literal(3)))).isEmpty();
        // varchar <-> json is unaffected.
        assertThat(LIBRARY.resolveCast(apply("varchar", literal(3)), symbol("json"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("json"), apply("varchar", literal(3)))).isPresent();
    }

    @Test
    void testNumberCasts()
    {
        // boolean -> number and number -> json are cast-only conversions Trino allows.
        assertThat(LIBRARY.resolveCast(symbol("boolean"), symbol("number"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("number"), symbol("json"))).isPresent();
        // Neither is an implicit coercion.
        assertThat(LIBRARY.typeSystem().coercionPlan(symbol("boolean"), symbol("number"))).isEmpty();
        assertThat(LIBRARY.typeSystem().coercionPlan(symbol("number"), symbol("json"))).isEmpty();
    }
}
