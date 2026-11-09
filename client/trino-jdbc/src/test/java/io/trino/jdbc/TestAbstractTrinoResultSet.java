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
package io.trino.jdbc;

import com.google.common.collect.ImmutableList;
import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import static io.trino.jdbc.AbstractTrinoResultSet.DEFAULT_OBJECT_REPRESENTATION;
import static io.trino.jdbc.AbstractTrinoResultSet.TYPE_CONVERSIONS;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;

public class TestAbstractTrinoResultSet
{
    @Test
    public void testByteRange()
            throws SQLException
    {
        assertNumericRange(Byte.MIN_VALUE, Byte.MAX_VALUE, "byte", result -> result.getByte(1), result -> result.getByte("value"));
    }

    @Test
    public void testShortRange()
            throws SQLException
    {
        assertNumericRange(Short.MIN_VALUE, Short.MAX_VALUE, "short", result -> result.getShort(1), result -> result.getShort("value"));
    }

    @Test
    public void testIntRange()
            throws SQLException
    {
        assertNumericRange(Integer.MIN_VALUE, Integer.MAX_VALUE, "int", result -> result.getInt(1), result -> result.getInt("value"));
    }

    @Test
    public void testLongRange()
            throws SQLException
    {
        assertNumericRange(Long.MIN_VALUE, Long.MAX_VALUE, "long", result -> result.getLong(1), result -> result.getLong("value"));
    }

    @Test
    public void testLongFloatingPointBoundaries()
            throws SQLException
    {
        for (NumericGetter getter : ImmutableList.<NumericGetter>of(result -> result.getLong(1), result -> result.getLong("value"))) {
            assertNumericValue(-0x1.0p63, Long.MIN_VALUE, getter);
            assertNumericValue(Math.nextDown(0x1.0p63), 9223372036854774784L, getter);
            assertNumericValue(-0x1.0p63f, Long.MIN_VALUE, getter);
            assertNumericValue(Math.nextDown(0x1.0p63f), 9223371487098961920L, getter);
            assertNumericOverflow(0x1.0p63, "long", getter);
            assertNumericOverflow(Math.nextDown(-0x1.0p63), "long", getter);
            assertNumericOverflow(0x1.0p63f, "long", getter);
            assertNumericOverflow(Math.nextDown(-0x1.0p63f), "long", getter);
        }
    }

    private static void assertNumericRange(long minimum, long maximum, String targetType, NumericGetter... getters)
            throws SQLException
    {
        for (NumericGetter getter : getters) {
            for (long value : new long[] {minimum, maximum, -1, 0, 1}) {
                assertNumericValue(value, value, getter);
                assertNumericValue(BigDecimal.valueOf(value), value, getter);
                assertNumericValue(Long.toString(value), value, getter);
            }
            for (BigDecimal value : ImmutableList.of(
                    BigDecimal.valueOf(minimum).subtract(BigDecimal.ONE),
                    BigDecimal.valueOf(maximum).add(BigDecimal.ONE),
                    BigDecimal.valueOf(minimum).subtract(new BigDecimal("0.1")),
                    BigDecimal.valueOf(maximum).add(new BigDecimal("0.1")))) {
                assertNumericOverflow(value, targetType, getter);
                assertNumericOverflow(value.toString(), targetType, getter);
            }
            if (minimum != Long.MIN_VALUE) {
                for (long value : new long[] {minimum - 1, maximum + 1}) {
                    assertNumericOverflow(value, targetType, getter);
                    assertNumericOverflow((double) value, targetType, getter);
                }
                assertNumericValue((double) minimum, minimum, getter);
                assertNumericValue((double) maximum, maximum, getter);
                assertNumericOverflow(minimum - 0.25, targetType, getter);
                assertNumericOverflow(maximum + 0.25, targetType, getter);
                assertNumericValue((float) minimum, minimum, getter);
                assertNumericOverflow(Math.nextDown((float) minimum), targetType, getter);
                assertNumericOverflow(Math.nextUp((float) maximum), targetType, getter);
                if (maximum == Integer.MAX_VALUE) {
                    assertNumericOverflow((float) maximum, targetType, getter);
                }
                else {
                    assertNumericValue((float) maximum, maximum, getter);
                }
            }
            for (Object value : ImmutableList.of(
                    Double.NaN,
                    Double.POSITIVE_INFINITY,
                    Double.NEGATIVE_INFINITY,
                    Float.NaN,
                    Float.POSITIVE_INFINITY,
                    Float.NEGATIVE_INFINITY,
                    "NaN",
                    "+Infinity",
                    "-Infinity")) {
                try (ResultSet result = numericResult(value)) {
                    assertThatThrownBy(() -> getter.get(result))
                            .isInstanceOf(SQLException.class)
                            .hasMessage("Value is not a number: " + value);
                }
            }
            for (Object value : ImmutableList.of(12.75, 12.75f, new BigDecimal("12.75"), "12.75")) {
                assertNumericValue(value, 12, getter);
            }
            for (Object value : ImmutableList.of(-12.75, -12.75f, new BigDecimal("-12.75"), "-12.75")) {
                assertNumericValue(value, -12, getter);
            }
            assertNumericValue(true, 1, getter);
            assertNumericValue(false, 0, getter);
            assertNumericValue(null, 0, getter);
            try (ResultSet result = numericResult("not a number")) {
                assertThatThrownBy(() -> getter.get(result))
                        .isInstanceOf(SQLException.class)
                        .hasMessage("Value is not a number: not a number");
            }
        }
    }

    private static void assertNumericValue(Object value, long expected, NumericGetter getter)
            throws SQLException
    {
        try (ResultSet result = numericResult(value)) {
            assertThat(getter.get(result)).as("conversion of %s", value).isEqualTo(expected);
            assertThat(result.wasNull()).isEqualTo(value == null);
        }
    }

    private static void assertNumericOverflow(Object value, String targetType, NumericGetter getter)
            throws SQLException
    {
        try (ResultSet result = numericResult(value)) {
            assertThatThrownBy(() -> getter.get(result))
                    .as("conversion of %s to %s", value, targetType)
                    .isInstanceOf(SQLException.class)
                    .hasMessage("Numeric overflow converting to " + targetType + ": " + value);
        }
    }

    private static ResultSet numericResult(Object value)
            throws SQLException
    {
        ResultSet result = new InMemoryTrinoResultSet(
                ImmutableList.of(new Column("value", "number", new ClientTypeSignature("number"))),
                ImmutableList.of(singletonList(value)));
        assertThat(result.next()).isTrue();
        return result;
    }

    @FunctionalInterface
    private interface NumericGetter
    {
        long get(ResultSet result)
                throws SQLException;
    }

    @Test
    public void testRepresentationImplemented()
    {
        DEFAULT_OBJECT_REPRESENTATION.forEach((sourceRawType, target) -> {
            if (target == Object.class) {
                return;
            }
            if (!TYPE_CONVERSIONS.hasConversion(sourceRawType, target)) {
                fail(String.format("No conversion registered from %s to %s", sourceRawType, target));
            }
        });
    }
}
