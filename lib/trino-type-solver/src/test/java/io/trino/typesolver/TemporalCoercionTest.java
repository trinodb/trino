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
package io.trino.typesolver;

import org.junit.jupiter.api.Test;

import static io.trino.typesolver.Expression.apply;
import static io.trino.typesolver.Expression.literal;
import static io.trino.typesolver.Expression.symbol;
import static org.assertj.core.api.Assertions.assertThat;

public class TemporalCoercionTest
{
    private final TypeSystem typeSystem = TrinoPreset.typeSystem();

    @Test
    void testDateCoercesToTimestamp()
    {
        assertThat(typeSystem.coercionPlan(symbol("date"), apply("timestamp", literal(3)))).isPresent();
    }

    @Test
    void testDateCoercesToTimestampWithTimeZone()
    {
        assertThat(typeSystem.coercionPlan(symbol("date"), apply("timestamp_with_time_zone", literal(3)))).isPresent();
    }

    @Test
    void testTimePrecisionWidens()
    {
        assertThat(typeSystem.coercionPlan(apply("time", literal(3)), apply("time", literal(6)))).isPresent();
    }

    @Test
    void testTimestampPrecisionWidens()
    {
        assertThat(typeSystem.coercionPlan(apply("timestamp", literal(3)), apply("timestamp", literal(6)))).isPresent();
    }

    @Test
    void testTimeCoercesToTimeWithTimeZone()
    {
        assertThat(typeSystem.coercionPlan(apply("time", literal(3)), apply("time_with_time_zone", literal(6)))).isPresent();
    }

    @Test
    void testTimestampCoercesToTimestampWithTimeZone()
    {
        assertThat(typeSystem.coercionPlan(apply("timestamp", literal(3)), apply("timestamp_with_time_zone", literal(6)))).isPresent();
    }
}
