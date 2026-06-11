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
import static org.weakref.solver.Expression.anonymousField;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.function;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.row;
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
        // array(uuid) -> json: uuid has no cast to json → compositional constraint fails.
        assertThat(LIBRARY.resolveCast(apply("array", symbol("uuid")), symbol("json"))).isEmpty();
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
    void testBooleanNumericCasts()
    {
        // boolean <-> every numeric, plus char -> boolean (cast-only, not implicit).
        assertThat(LIBRARY.resolveCast(symbol("boolean"), symbol("integer"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("integer"), symbol("boolean"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("boolean"), apply("decimal", literal(10), literal(2)))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("decimal", literal(10), literal(2)), symbol("boolean"))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("char", literal(5)), symbol("boolean"))).isPresent();
        assertThat(LIBRARY.typeSystem().coercionPlan(symbol("boolean"), symbol("integer"))).isEmpty();
    }

    @Test
    void testTemporalToTemporalCasts()
    {
        // time -> timestamp (combines with current date), time precision narrowing, zoned conversions.
        assertThat(LIBRARY.resolveCast(apply("time", literal(3)), apply("timestamp", literal(6)))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("time", literal(6)), apply("time", literal(0)))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("time", literal(3)), apply("timestamp_with_time_zone", literal(3)))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("timestamp", literal(6)), apply("time", literal(3)))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("timestamp", literal(6)), apply("timestamp", literal(0)))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("timestamp_with_time_zone", literal(6)), symbol("date"))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("timestamp_with_time_zone", literal(6)), apply("time", literal(3)))).isPresent();
    }

    @Test
    void testIpAddressVarcharCastDirections()
    {
        // ipaddress -> varchar is unbounded only; ipaddress -> varchar(n) is rejected (as in Trino).
        assertThat(LIBRARY.resolveCast(symbol("ipaddress"), symbol("varchar"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("ipaddress"), apply("varchar", literal(5)))).isEmpty();
        // varchar(n) -> ipaddress accepts any length.
        assertThat(LIBRARY.resolveCast(apply("varchar", literal(5)), symbol("ipaddress"))).isPresent();
    }

    @Test
    void testNumericDecimalCastsAreUnguardedButImplicitIsNot()
    {
        // CAST(integer AS decimal(1,0)) is allowed (runtime range check) even though the value
        // may not fit — but the *implicit* coercion stays guarded and rejects it.
        assertThat(LIBRARY.resolveCast(symbol("integer"), apply("decimal", literal(1), literal(0)))).isPresent();
        assertThat(LIBRARY.typeSystem().coercionPlan(symbol("integer"), apply("decimal", literal(1), literal(0)))).isEmpty();
        // decimal narrows to the integer types; real/double widen to decimal.
        assertThat(LIBRARY.resolveCast(apply("decimal", literal(10), literal(2)), symbol("tinyint"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("real"), apply("decimal", literal(10), literal(2)))).isPresent();
    }

    @Test
    void testCharCasts()
    {
        assertThat(LIBRARY.resolveCast(apply("char", literal(3)), symbol("integer"))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("char", literal(3)), apply("timestamp", literal(3)))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("char", literal(3)), apply("char", literal(1)))).isPresent();
        // Trino has no char->decimal / char->time / char->uuid casts.
        assertThat(LIBRARY.resolveCast(apply("char", literal(3)), apply("decimal", literal(10), literal(2)))).isEmpty();
        assertThat(LIBRARY.resolveCast(apply("char", literal(3)), symbol("uuid"))).isEmpty();
    }

    @Test
    void testRemainingScalarCasts()
    {
        assertThat(LIBRARY.resolveCast(symbol("real"), symbol("tinyint"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("varbinary"), symbol("uuid"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("ipaddress"), symbol("varbinary"))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("timestamp", literal(3)), symbol("json"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("json"), symbol("number"))).isPresent();
    }

    @Test
    void testStructuralElementWiseCasts()
    {
        // array(varchar(5)) -> array(integer): element cast varchar -> integer exists.
        assertThat(LIBRARY.resolveCast(apply("array", apply("varchar", literal(5))), apply("array", symbol("integer")))).isPresent();
        // map(varchar(5), bigint) -> map(integer, varchar(5)): key and value both cast.
        assertThat(LIBRARY.resolveCast(
                apply("map", apply("varchar", literal(5)), symbol("bigint")),
                apply("map", symbol("integer"), apply("varchar", literal(5))))).isPresent();
        // array(date) -> array(uuid): no date -> uuid cast, so the container cast is rejected.
        assertThat(LIBRARY.resolveCast(apply("array", symbol("date")), apply("array", symbol("uuid")))).isEmpty();
    }

    @Test
    void testRowToJsonCast()
    {
        // row(integer, varchar(3)) -> json: every field casts to json.
        assertThat(LIBRARY.resolveCast(row(anonymousField(symbol("integer")), anonymousField(apply("varchar", literal(3)))), symbol("json"))).isPresent();
        // row(uuid) -> json: uuid has no cast to json, so the row cast is rejected.
        assertThat(LIBRARY.resolveCast(row(anonymousField(symbol("uuid"))), symbol("json"))).isEmpty();
    }

    @Test
    void testVariantCasts()
    {
        assertThat(LIBRARY.resolveCast(apply("varchar", literal(5)), symbol("variant"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("variant"), symbol("integer"))).isPresent();
        // Containers cast element-wise to/from variant; map requires a varchar key.
        assertThat(LIBRARY.resolveCast(apply("array", symbol("integer")), symbol("variant"))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("map", apply("varchar", literal(5)), symbol("bigint")), symbol("variant"))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("map", symbol("integer"), symbol("bigint")), symbol("variant"))).isEmpty();
        // variant -> varchar is unbounded only.
        assertThat(LIBRARY.resolveCast(symbol("variant"), symbol("varchar"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("variant"), apply("varchar", literal(5)))).isEmpty();
    }

    @Test
    void testSketchTypeCasts()
    {
        assertThat(LIBRARY.resolveCast(symbol("hyperloglog"), symbol("varbinary"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("varbinary"), symbol("hyperloglog"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("setdigest"), symbol("hyperloglog"))).isPresent();
        assertThat(LIBRARY.resolveCast(apply("qdigest", symbol("bigint")), symbol("varbinary"))).isPresent();
        // P4HyperLogLog -> HyperLogLog is an implicit coercion (so also a cast).
        assertThat(LIBRARY.typeSystem().coercionPlan(symbol("p4hyperloglog"), symbol("hyperloglog"))).isPresent();
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
