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
import org.weakref.solver.TypeLibrary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.symbol;

public class SpecializedTypeCastTest
{
    private static final TypeLibrary LIBRARY = TrinoPreset.library();

    @Test
    void testVarcharToUuidAndBack()
    {
        assertThat(LIBRARY.resolveCast(apply("varchar", literal(36)), symbol("uuid"))).isPresent();
        assertThat(LIBRARY.resolveCast(symbol("uuid"), apply("varchar", literal(36)))).isPresent();
    }

    @Test
    void testVarcharToIpAddressAndBack()
    {
        assertThat(LIBRARY.resolveCast(apply("varchar", literal(45)), symbol("ipaddress"))).isPresent();
        // ipaddress casts to unbounded varchar only (Trino), not varchar(n).
        assertThat(LIBRARY.resolveCast(symbol("ipaddress"), symbol("varchar"))).isPresent();
    }

    @Test
    void testJsonToArrayCompositional()
    {
        // json → array(integer): valid because json → integer exists.
        assertThat(LIBRARY.resolveCast(symbol("json"), apply("array", symbol("integer")))).isPresent();
    }

    @Test
    void testJsonToMapCompositional()
    {
        // json → map(varchar(10), integer): valid; both keys and values have inward casts from varchar/json.
        assertThat(LIBRARY.resolveCast(
                symbol("json"),
                apply("map", apply("varchar", literal(10)), symbol("integer"))))
                .isPresent();
    }

    @Test
    void testJsonToArrayRejectsIncompatibleElement()
    {
        // json → array(date): date has no inward cast from json → should fail.
        assertThat(LIBRARY.resolveCast(symbol("json"), apply("array", symbol("date")))).isEmpty();
    }

    @Test
    void testTimestampNarrowsToDate()
    {
        assertThat(LIBRARY.resolveCast(apply("timestamp", literal(3)), symbol("date"))).isPresent();
    }

    @Test
    void testTimestampWithTimeZoneNarrowsToTimestamp()
    {
        assertThat(LIBRARY.resolveCast(apply("timestamp_with_time_zone", literal(3)), apply("timestamp", literal(6))))
                .isPresent();
    }

    @Test
    void testDecimalNarrowsViaCast()
    {
        // decimal(20, 5) → decimal(10, 2): narrowing, not implicit, but cast allows it.
        assertThat(LIBRARY.resolveCast(apply("decimal", literal(20), literal(5)), apply("decimal", literal(10), literal(2))))
                .isPresent();
    }
}
