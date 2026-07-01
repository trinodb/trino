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
package io.trino.plugin.faker;

import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

class TestIntervalValues
{
    @Test
    void testSecondsAreParsedToMicroseconds()
    {
        Type type = TESTING_TYPE_MANAGER.fromSqlType("interval second(13, 6)");
        assertThat(Literal.parse("1", type)).isEqualTo(1_000_000L);
        assertThat(Literal.parse("123.456", type)).isEqualTo(123_456_000L);
        assertThat(Literal.parse("0.000001", type)).isEqualTo(1L);
        assertThat(Literal.parse("-0.000001", type)).isEqualTo(-1L);
        assertThat(Literal.parse("1.9999996", type)).isEqualTo(2_000_000L);
        assertThat(Literal.parse("-1.9999995", type)).isEqualTo(-1_999_999L);
    }

    @Test
    void testQualifierAndFractionalPrecision()
    {
        Type millis = TESTING_TYPE_MANAGER.fromSqlType("interval second(2, 3)");
        assertThat(Literal.parse("1.234499999999", millis)).isEqualTo(1_234_000L);
        assertThat(IntervalValues.quantum(millis)).isEqualTo(1000L);
        Type days = TESTING_TYPE_MANAGER.fromSqlType("interval day(2)");
        assertThat(Literal.parse("90000", days)).isEqualTo(86_400_000_000L);
        assertThat(IntervalValues.quantum(days)).isEqualTo(86_400_000_000L);
        Type years = TESTING_TYPE_MANAGER.fromSqlType("interval year(2)");
        assertThat(Literal.parse("25", years)).isEqualTo(24L);
        assertThat(IntervalValues.quantum(years)).isEqualTo(12L);
        Type months = TESTING_TYPE_MANAGER.fromSqlType("interval month(2)");
        assertThat(Literal.parse("-25", months)).isEqualTo(-25L);
    }

    @Test
    void testLongIntervalRejected()
    {
        Type type = TESTING_TYPE_MANAGER.fromSqlType("interval second(2, 9)");
        assertTrinoExceptionThrownBy(() -> Literal.parse("1.123456789", type))
                .hasErrorCode(NOT_SUPPORTED);
        assertTrinoExceptionThrownBy(() -> IntervalValues.checkSupported(type))
                .hasErrorCode(NOT_SUPPORTED);
    }
}
