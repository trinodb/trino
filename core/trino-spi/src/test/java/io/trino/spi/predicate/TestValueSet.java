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
package io.trino.spi.predicate;

import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.TIME_NANOS;
import static io.trino.spi.type.TimeType.TIME_PICOS;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MICROS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_NANOS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_PICOS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_SECONDS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_PICOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.floatToRawIntBits;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.TEN;
import static java.math.BigDecimal.ZERO;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestValueSet
{
    @Test
    public void testRejectNullOnCreate()
    {
        testRejectNullOnCreate(BOOLEAN, true, false, true);
        testRejectNullOnCreate(TINYINT, 1L, 2L, 3L);
        testRejectNullOnCreate(SMALLINT, 1L, 2L, 3L);
        testRejectNullOnCreate(INTEGER, 1L, 2L, 3L);
        testRejectNullOnCreate(BIGINT, 1L, 2L, 3L);
        testRejectNullOnCreate(REAL, (long) floatToIntBits(1.0f), (long) floatToIntBits(2.0f), (long) floatToIntBits(3.0f));
        testRejectNullOnCreate(DOUBLE, 1.0, 2.0, 3.0);
        testRejectNullOnCreate(createDecimalType(10, 2), 1L, 2L, 3L);
        testRejectNullOnCreate(createDecimalType(20, 2), Int128.valueOf(1), Int128.valueOf(2), Int128.valueOf(3));
        testRejectNullOnCreate(NUMBER, TrinoNumber.from(ZERO), TrinoNumber.from(ONE), TrinoNumber.from(TEN));
        testRejectNullOnCreate(VARCHAR, utf8Slice("abc"), utf8Slice("def"), utf8Slice("ghi"));
        testRejectNullOnCreate(createVarcharType(25), utf8Slice("abc"), utf8Slice("def"), utf8Slice("ghi"));
        testRejectNullOnCreate(VARBINARY, utf8Slice("abc"), utf8Slice("def"), utf8Slice("ghi"));
        testRejectNullOnCreate(DATE, 1L, 2L, 3L);
        testRejectNullOnCreate(TIME_SECONDS, 1_000_000_000_000L, 2_000_000_000_000L, 3_000_000_000_000L);
        testRejectNullOnCreate(TIME_MILLIS, 1_000_000_000L, 2_000_000_000L, 3_000_000_000L);
        testRejectNullOnCreate(TIME_MICROS, 1_000_000L, 2_000_000L, 3_000_000L);
        testRejectNullOnCreate(TIME_NANOS, 1_000L, 2_000L, 3_000L);
        testRejectNullOnCreate(TIME_PICOS, 1L, 2L, 3L);
        testRejectNullOnCreate(TIME_TZ_SECONDS, packDateTimeWithZone(1_000_000_000_000L, UTC_KEY), packDateTimeWithZone(2_000_000_000_000L, UTC_KEY), packDateTimeWithZone(3_000_000_000_000L, UTC_KEY));
        testRejectNullOnCreate(TIME_TZ_MILLIS, packDateTimeWithZone(1_000_000_000L, UTC_KEY), packDateTimeWithZone(2_000_000_000L, UTC_KEY), packDateTimeWithZone(3_000_000_000L, UTC_KEY));
        testRejectNullOnCreate(TIME_TZ_MICROS, packDateTimeWithZone(1_000_000L, UTC_KEY), packDateTimeWithZone(2_000_000L, UTC_KEY), packDateTimeWithZone(3_000_000L, UTC_KEY));
        testRejectNullOnCreate(TIME_TZ_NANOS, packDateTimeWithZone(1_000L, UTC_KEY), packDateTimeWithZone(2_000L, UTC_KEY), packDateTimeWithZone(3_000L, UTC_KEY));
        testRejectNullOnCreate(TIME_TZ_PICOS, new LongTimeWithTimeZone(1L, 0), new LongTimeWithTimeZone(2L, 0), new LongTimeWithTimeZone(3L, 0));
        testRejectNullOnCreate(TIMESTAMP_SECONDS, 1_000_000L, 2_000_000L, 3_000_000L);
        testRejectNullOnCreate(TIMESTAMP_MILLIS, 1_000L, 2_000L, 3_000L);
        testRejectNullOnCreate(TIMESTAMP_MICROS, 1L, 2L, 3L);
        testRejectNullOnCreate(TIMESTAMP_NANOS, new LongTimestamp(1L, 0), new LongTimestamp(2L, 0), new LongTimestamp(3L, 0));
        testRejectNullOnCreate(TIMESTAMP_PICOS, new LongTimestamp(1L, 1), new LongTimestamp(2L, 2), new LongTimestamp(3L, 3));
        testRejectNullOnCreate(TIMESTAMP_TZ_SECONDS, packDateTimeWithZone(1_000L, UTC_KEY), packDateTimeWithZone(2_000L, UTC_KEY), packDateTimeWithZone(3_000L, UTC_KEY));
        testRejectNullOnCreate(TIMESTAMP_TZ_MILLIS, packDateTimeWithZone(1L, UTC_KEY), packDateTimeWithZone(2L, UTC_KEY), packDateTimeWithZone(3L, UTC_KEY));
        testRejectNullOnCreate(TIMESTAMP_TZ_MICROS, fromEpochMillisAndFraction(1L, 0, UTC_KEY), fromEpochMillisAndFraction(2L, 0, UTC_KEY), fromEpochMillisAndFraction(3L, 0, UTC_KEY));
        testRejectNullOnCreate(TIMESTAMP_TZ_NANOS, fromEpochMillisAndFraction(1L, 1_000, UTC_KEY), fromEpochMillisAndFraction(2L, 2_000, UTC_KEY), fromEpochMillisAndFraction(3L, 3_000, UTC_KEY));
        testRejectNullOnCreate(TIMESTAMP_TZ_PICOS, fromEpochMillisAndFraction(1L, 1, UTC_KEY), fromEpochMillisAndFraction(2L, 2, UTC_KEY), fromEpochMillisAndFraction(3L, 3, UTC_KEY));
        testRejectNullOnCreate(UUID, javaUuidToTrinoUuid(new java.util.UUID(0, 1)), javaUuidToTrinoUuid(new java.util.UUID(0, 2)), javaUuidToTrinoUuid(new java.util.UUID(0, 3)));
        testRejectNullOnCreate(createCharType(5), utf8Slice("abc"), utf8Slice("def"), utf8Slice("ghi"));
        testRejectNullOnCreate(new ArrayType(BIGINT), writeNativeValue(BIGINT, 1L), writeNativeValue(BIGINT, 2L), writeNativeValue(BIGINT, 3L));
        RowType rowType = RowType.anonymous(List.of(BIGINT, VARCHAR));
        testRejectNullOnCreate(rowType, rowValue(rowType, 1L, "abc"), rowValue(rowType, 2L, "def"), rowValue(rowType, 3L, "ghi"));
        MapType mapType = new MapType(BIGINT, VARCHAR, new TypeOperators());
        testRejectNullOnCreate(mapType, mapValue(mapType, 1L, "abc"), mapValue(mapType, 2L, "def"), mapValue(mapType, 3L, "ghi"));
    }

    private static void testRejectNullOnCreate(Type type, Object exampleValueA, Object exampleValueB, Object exampleValueC)
    {
        assertThatThrownBy(() -> ValueSet.of(type, null))
                .as("ValueSet.of %s with null value", type)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("value is null");

        assertThatThrownBy(() -> ValueSet.of(type, null, exampleValueA, exampleValueB, exampleValueC))
                .as("ValueSet.of %s with null value and non-null values", type)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("value is null");

        assertThatThrownBy(() -> ValueSet.of(type, exampleValueA, exampleValueB, null, exampleValueC))
                .as("ValueSet.of %s with non-null values and a null value", type)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("value is null");

        assertThatThrownBy(() -> ValueSet.copyOf(type, asList(exampleValueA, exampleValueB, null, exampleValueC)))
                .as("ValueSet.copyOf %s with non-null values and a null value", type)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("value is null");
    }

    private static SqlRow rowValue(RowType rowType, long first, String second)
    {
        return buildRowValue(rowType, fieldBuilders -> {
            BIGINT.writeLong(fieldBuilders.get(0), first);
            VARCHAR.writeSlice(fieldBuilders.get(1), utf8Slice(second));
        });
    }

    private static SqlMap mapValue(MapType mapType, long key, String value)
    {
        return buildMapValue(mapType, 1, (keyBuilder, valueBuilder) -> {
            BIGINT.writeLong(keyBuilder, key);
            VARCHAR.writeSlice(valueBuilder, utf8Slice(value));
        });
    }

    @Test
    public void testDoubleNaNOnCreate()
    {
        // ValueSet.of with NaN
        assertThat(ValueSet.of(DOUBLE, Double.NaN).containsValue(Double.NaN))
                .isTrue();

        // a different NaN
        assertThat(ValueSet.of(DOUBLE, longBitsToDouble(0x7FF8123412341234L)).containsValue(Double.NaN))
                .isTrue();

        // ValueSet.of with others and NaN
        assertThat(ValueSet.of(DOUBLE, 42., 123., Double.NaN, 127.).containsValue(Double.NaN))
                .isTrue();

        // ValueSet.of with others and NaN first
        assertThat(ValueSet.of(DOUBLE, Double.NaN, 42., 123., 127.).containsValue(Double.NaN))
                .isTrue();

        // ValueSet.copyOf with NaN
        assertThat(ValueSet.copyOf(DOUBLE, List.of(42., 123., Double.NaN, 127.)).containsValue(Double.NaN))
                .isTrue();

        // ValueSet.copyOf with NaN first, in case the method has a special treatment for first value
        assertThat(ValueSet.copyOf(DOUBLE, List.of(Double.NaN, 42., 123., 127.)).containsValue(Double.NaN))
                .isTrue();
    }

    @Test
    public void testRealNaNOnCreate()
    {
        // ValueSet.of with NaN
        assertThat(ValueSet.of(REAL, real(Float.NaN)).containsValue(real(Float.NaN)))
                .isTrue();

        // a different NaN
        assertThat(ValueSet.of(REAL, (long) 0x7FC01234).containsValue(real(Float.NaN)))
                .isTrue();

        // ValueSet.of with others and NaN
        assertThat(ValueSet.of(REAL, real(42), real(123), real(Float.NaN), real(127)).containsValue(real(Float.NaN)))
                .isTrue();

        // ValueSet.of with others and NaN first
        assertThat(ValueSet.of(REAL, real(Float.NaN), real(42), real(123), real(127)).containsValue(real(Float.NaN)))
                .isTrue();

        // ValueSet.copyOf with NaN
        assertThat(ValueSet.copyOf(REAL, List.of(real(42), real(123), real(Float.NaN), real(127))).containsValue(real(Float.NaN)))
                .isTrue();

        // ValueSet.copyOf with NaN first, in case the method has a special treatment for first value
        assertThat(ValueSet.copyOf(REAL, List.of(real(Float.NaN), real(42), real(123), real(127))).containsValue(real(Float.NaN)))
                .isTrue();
    }

    private static long real(float value)
    {
        return floatToRawIntBits(value);
    }
}
