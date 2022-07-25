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
package io.trino.spi.statistics;

import com.google.common.primitives.Primitives;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.OptionalDouble;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.statistics.StatsUtil.toStatsRepresentation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.floatToIntBits;
import static org.testng.Assert.assertEquals;

public class TestStatsUtil
{
    @Test
    public void testToStatsRepresentation()
    {
        assertToStatsRepresentation(BIGINT, 123456L, 123456);
        assertToStatsRepresentation(INTEGER, 12345L, 12345);
        assertToStatsRepresentation(SMALLINT, 1234L, 1234);
        assertToStatsRepresentation(TINYINT, 123L, 123);
        assertToStatsRepresentation(DOUBLE, 0.1d, 0.1);
        assertToStatsRepresentation(REAL, (long) floatToIntBits(0.2f), 0.2f);
        assertToStatsRepresentation(createDecimalType(5, 2), 12345L, 123.45);
        assertToStatsRepresentation(createDecimalType(25, 5), Decimals.valueOf(new BigDecimal("12345678901234567890.12345")), 12345678901234567890.12345);
        assertToStatsRepresentation(DATE, 1L, 1);
        assertToStatsRepresentation(createTimestampType(0), 3_000_000L, 3_000_000.);
        assertToStatsRepresentation(createTimestampType(3), 3_000L, 3_000.);
        assertToStatsRepresentation(createTimestampType(6), 3L, 3.);
        assertToStatsRepresentation(createTimestampType(9), new LongTimestamp(3, 0), 3.);
        assertToStatsRepresentation(createTimestampType(12), new LongTimestamp(3, 999), 3.);
        assertToStatsRepresentation(createTimestampWithTimeZoneType(0), packDateTimeWithZone(3000, getTimeZoneKey("Europe/Warsaw")), 3000.);
        assertToStatsRepresentation(createTimestampWithTimeZoneType(3), packDateTimeWithZone(3, getTimeZoneKey("Europe/Warsaw")), 3.);
        assertToStatsRepresentation(createTimestampWithTimeZoneType(6), LongTimestampWithTimeZone.fromEpochMillisAndFraction(3, 999999999, getTimeZoneKey("Europe/Warsaw")), 3.);
        assertToStatsRepresentation(createTimestampWithTimeZoneType(9), LongTimestampWithTimeZone.fromEpochMillisAndFraction(3, 999999999, getTimeZoneKey("Europe/Warsaw")), 3.);
        assertToStatsRepresentation(createTimestampWithTimeZoneType(12), LongTimestampWithTimeZone.fromEpochMillisAndFraction(3, 999999999, getTimeZoneKey("Europe/Warsaw")), 3.);
    }

    private static void assertToStatsRepresentation(Type type, Object trinoValue, double expected)
    {
        verify(Primitives.wrap(type.getJavaType()).isInstance(trinoValue), "Incorrect class of value for %s: %s", type, trinoValue.getClass());
        assertEquals(toStatsRepresentation(type, trinoValue), OptionalDouble.of(expected));
    }
}
