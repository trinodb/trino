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
package io.trino.plugin.deltalake.transactionlog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeParquetFileStatistics;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RowType;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.convertParquetToJsonStatistics;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.jsonValueToTrinoValue;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.jsonValueToTrinoValueUpperBound;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.tightBounds;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.toJsonValue;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.toJsonValueUpperBound;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeltaLakeParquetStatisticsUtils
{
    @Test
    public void testIntegerStatistics()
    {
        String columnName = "t_integer";

        PrimitiveType intType = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, columnName);
        Statistics<?> stats = Statistics.getBuilderForReading(intType)
                .withMin(getIntByteArray(-100))
                .withMax(getIntByteArray(150))
                .withNumNulls(10)
                .build();

        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, IntegerType.INTEGER))).isEqualTo(ImmutableMap.of(columnName, -100));
        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, IntegerType.INTEGER))).isEqualTo(ImmutableMap.of(columnName, 150));
    }

    @Test
    public void testStringStatistics()
    {
        String columnName = "t_string";
        PrimitiveType stringType = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, columnName);
        Statistics<?> stats = Statistics.getBuilderForReading(stringType)
                .withMin("abc".getBytes(UTF_8))
                .withMax("bac".getBytes(UTF_8))
                .withNumNulls(1)
                .build();

        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, createUnboundedVarcharType()))).isEqualTo(ImmutableMap.of(columnName, "abc"));
        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, createUnboundedVarcharType()))).isEqualTo(ImmutableMap.of(columnName, "bac"));
    }

    @Test
    public void testFloatStatistics()
    {
        String columnName = "t_float";
        PrimitiveType type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, columnName);
        Statistics<?> stats = Statistics.getBuilderForReading(type)
                .withMin(getFloatByteArray(100.0f))
                .withMax(getFloatByteArray(1000.001f))
                .withNumNulls(2)
                .build();

        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, REAL))).isEqualTo(ImmutableMap.of(columnName, 100.0f));
        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, REAL))).isEqualTo(ImmutableMap.of(columnName, 1000.001f));

        columnName = "t_double";
        type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, columnName);
        stats = Statistics.getBuilderForReading(type)
                .withMin(getDoubleByteArray(100.0))
                .withMax(getDoubleByteArray(1000.001))
                .withNumNulls(2)
                .build();

        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, DOUBLE))).isEqualTo(ImmutableMap.of(columnName, 100.0));
        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, DOUBLE))).isEqualTo(ImmutableMap.of(columnName, 1000.001));
    }

    @Test
    public void testDateStatistics()
    {
        String columnName = "t_date";
        PrimitiveType type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, columnName);
        Statistics<?> stats = Statistics.getBuilderForReading(type)
                .withMin(getIntByteArray((int) LocalDate.parse("2020-08-26").toEpochDay()))
                .withMax(getIntByteArray((int) LocalDate.parse("2020-09-17").toEpochDay()))
                .withNumNulls(2)
                .build();

        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, DATE))).isEqualTo(ImmutableMap.of(columnName, "2020-08-26"));
        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, DATE))).isEqualTo(ImmutableMap.of(columnName, "2020-09-17"));
    }

    @Test
    public void testTimestampJsonValueToTrinoValue()
    {
        assertThat(jsonValueToTrinoValue(TIMESTAMP_MICROS, "2020-08-26T01:02:03.123Z"))
                .isEqualTo(Instant.parse("2020-08-26T01:02:03.123Z").toEpochMilli() * MICROSECONDS_PER_MILLISECOND);
        assertThat(jsonValueToTrinoValue(TIMESTAMP_MICROS, "2020-08-26T01:02:03.123111Z"))
                .isEqualTo(Instant.parse("2020-08-26T01:02:03Z").getEpochSecond() * MICROSECONDS_PER_SECOND + 123111);
        assertThat(jsonValueToTrinoValue(TIMESTAMP_MICROS, "2020-08-26T01:02:03.123999Z"))
                .isEqualTo(Instant.parse("2020-08-26T01:02:03Z").getEpochSecond() * MICROSECONDS_PER_SECOND + 123999);
    }

    @Test
    public void testTimestampWithoutZoneJsonValueToTrinoValue()
    {
        long expected = Instant.parse("2020-08-26T01:02:03Z").getEpochSecond() * MICROSECONDS_PER_SECOND + 123456;
        assertThat(jsonValueToTrinoValue(TIMESTAMP_MICROS, "2020-08-26T01:02:03.123456")).isEqualTo(expected);
        assertThat(jsonValueToTrinoValue(TIMESTAMP_MICROS, "2020-08-26T01:02:03.123456Z")).isEqualTo(expected);
        assertThat(jsonValueToTrinoValue(TIMESTAMP_MICROS, "2020-08-26T02:02:03.123456+01:00")).isEqualTo(expected);
        assertThat(jsonValueToTrinoValue(TIMESTAMP_MICROS, "2020-08-25T23:02:03.123456-02:00")).isEqualTo(expected);
        assertThat(jsonValueToTrinoValue(TIMESTAMP_MICROS, "2020-08-26T01:02:03")).isEqualTo(expected - 123456);
    }

    @Test
    public void testFloatingPointJsonValueToTrinoValue()
    {
        assertThat(jsonValueToTrinoValue(DOUBLE, 1.5)).isEqualTo(1.5);
        assertThat(jsonValueToTrinoValue(DOUBLE, 2)).isEqualTo(2.0);
        assertThat(jsonValueToTrinoValue(DOUBLE, 3L)).isEqualTo(3.0);
        assertThat(jsonValueToTrinoValue(DOUBLE, "Infinity")).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(jsonValueToTrinoValue(DOUBLE, "-Infinity")).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(jsonValueToTrinoValue(DOUBLE, "NaN")).isEqualTo(Double.NaN);
        assertThatThrownBy(() -> jsonValueToTrinoValue(DOUBLE, "inf"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unexpected value for double type: inf");

        assertThat(jsonValueToTrinoValue(REAL, 1.5)).isEqualTo((long) floatToRawIntBits(1.5f));
        assertThat(jsonValueToTrinoValue(REAL, 2)).isEqualTo((long) floatToRawIntBits(2.0f));
        assertThat(jsonValueToTrinoValue(REAL, "Infinity")).isEqualTo((long) floatToRawIntBits(Float.POSITIVE_INFINITY));
        assertThat(jsonValueToTrinoValue(REAL, "-Infinity")).isEqualTo((long) floatToRawIntBits(Float.NEGATIVE_INFINITY));
        assertThat(jsonValueToTrinoValue(REAL, "NaN")).isEqualTo((long) floatToRawIntBits(Float.NaN));
        assertThatThrownBy(() -> jsonValueToTrinoValue(REAL, "nan"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unexpected value for real type: nan");
    }

    @Test
    public void testDecimalJsonValueToTrinoValue()
    {
        DecimalType shortDecimal = createDecimalType(10, 2);
        assertThat(jsonValueToTrinoValue(shortDecimal, "123.45")).isEqualTo(12345L);
        assertThat(jsonValueToTrinoValue(shortDecimal, 123.45)).isEqualTo(12345L);
        assertThat(jsonValueToTrinoValue(shortDecimal, 123)).isEqualTo(12300L);
        assertThat(jsonValueToTrinoValue(shortDecimal, 123L)).isEqualTo(12300L);

        DecimalType longDecimal = createDecimalType(38, 0);
        BigInteger bigValue = new BigInteger("123456789012345678901234567890");
        assertThat(jsonValueToTrinoValue(longDecimal, bigValue.toString())).isEqualTo(Int128.valueOf(bigValue));
        assertThat(jsonValueToTrinoValue(longDecimal, bigValue)).isEqualTo(Int128.valueOf(bigValue));
        assertThat(jsonValueToTrinoValue(longDecimal, 42)).isEqualTo(Int128.valueOf(42));
    }

    @Test
    public void testRowWithMissingFieldJsonValueToTrinoValue()
    {
        RowType rowType = RowType.rowType(field("a", IntegerType.INTEGER), field("b", createUnboundedVarcharType()));
        Map<String, Object> values = new HashMap<>();
        values.put("b", "x");

        SqlRow row = (SqlRow) jsonValueToTrinoValue(rowType, values);
        assertThat(row.getRawFieldBlock(0).isNull(row.getRawIndex())).isTrue();
        assertThat(createUnboundedVarcharType().getSlice(row.getRawFieldBlock(1), row.getRawIndex())).isEqualTo(Slices.utf8Slice("x"));
    }

    @Test
    public void testTimestampStatisticsMillisPrecision()
    {
        String columnName = "t_timestamp";
        PrimitiveType type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, columnName);
        Statistics<?> stats = Statistics.getBuilderForReading(type)
                .withMin(timestampToBytes(LocalDateTime.parse("2020-08-26T01:02:03.123456")))
                .withMax(timestampToBytes(LocalDateTime.parse("2020-08-26T01:02:03.987654")))
                .withNumNulls(2)
                .build();

        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_MICROS)))
                .isEqualTo(ImmutableMap.of(columnName, "2020-08-26T01:02:03.123Z"));
        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_MICROS)))
                .isEqualTo(ImmutableMap.of(columnName, "2020-08-26T01:02:03.988Z"));
    }

    @Test
    public void testTimestampStatisticsBeforeEpoch()
    {
        String columnName = "t_timestamp";
        PrimitiveType type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, columnName);
        Statistics<?> stats = Statistics.getBuilderForReading(type)
                .withMin(timestampToBytes(LocalDateTime.parse("1952-04-03T01:02:03.456789")))
                .withMax(timestampToBytes(LocalDateTime.parse("1969-12-31T23:59:59.999999")))
                .withNumNulls(0)
                .build();

        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_MICROS)))
                .isEqualTo(ImmutableMap.of(columnName, "1952-04-03T01:02:03.456Z"));
        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_MICROS)))
                .isEqualTo(ImmutableMap.of(columnName, "1970-01-01T00:00:00Z"));
    }

    private static byte[] timestampToBytes(LocalDateTime localDateTime)
    {
        long epochMicros = localDateTime.toEpochSecond(UTC) * MICROSECONDS_PER_SECOND
                + localDateTime.getNano() / NANOSECONDS_PER_MICROSECOND;

        Slice slice = Slices.allocate(8);
        slice.setLong(0, epochMicros);
        return slice.byteArray();
    }

    @Test
    public void testTimestampStatisticsHighPrecision()
    {
        String columnName = "t_timestamp";
        PrimitiveType type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT96, columnName);
        Statistics<?> stats = Statistics.getBuilderForReading(type)
                .withMin(toParquetEncoding(LocalDateTime.parse("2020-08-26T01:02:03.123456789")))
                .withMax(toParquetEncoding(LocalDateTime.parse("2020-08-26T01:02:03.123987654")))
                .withNumNulls(2)
                .build();

        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_MICROS)))
                .isEqualTo(ImmutableMap.of(columnName, "2020-08-26T01:02:03.123Z"));
        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_MICROS)))
                .isEqualTo(ImmutableMap.of(columnName, "2020-08-26T01:02:03.124Z"));
    }

    @Test
    public void testTimestampWithTimeZoneStatisticsHighPrecision()
    {
        String columnName = "t_timestamp";
        PrimitiveType type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT96, columnName);
        Statistics<?> stats = Statistics.getBuilderForReading(type)
                .withMin(toParquetEncoding(LocalDateTime.parse("2020-08-26T01:02:03.123456789")))
                .withMax(toParquetEncoding(LocalDateTime.parse("2020-08-26T01:02:03.123987654")))
                .withNumNulls(2)
                .build();

        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_TZ_MILLIS))).isEqualTo(ImmutableMap.of(columnName, "2020-08-26T01:02:03.123Z"));
        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_TZ_MILLIS))).isEqualTo(ImmutableMap.of(columnName, "2020-08-26T01:02:03.124Z"));
    }

    @Test
    public void testTimestampWithTimeZoneStatisticsMillisPrecision()
    {
        String columnName = "t_timestamp";
        PrimitiveType type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT96, columnName);
        Statistics<?> stats = Statistics.getBuilderForReading(type)
                .withMin(toParquetEncoding(LocalDateTime.parse("2020-08-26T01:02:03.123")))
                .withMax(toParquetEncoding(LocalDateTime.parse("2020-08-26T01:02:03.123")))
                .withNumNulls(2)
                .build();

        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_TZ_MILLIS))).isEqualTo(ImmutableMap.of(columnName, "2020-08-26T01:02:03.123Z"));
        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_TZ_MILLIS))).isEqualTo(ImmutableMap.of(columnName, "2020-08-26T01:02:03.123Z"));
    }

    @Test
    public void testTimestampWithTimeZoneUpperBound()
    {
        long epochMillis = Instant.parse("2024-01-15T10:30:00.123Z").toEpochMilli();
        LongTimestampWithTimeZone subMillisecond = LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, 456_000_000, UTC_KEY);
        assertThat(toJsonValue(TIMESTAMP_TZ_MICROS, subMillisecond)).isEqualTo("2024-01-15T10:30:00.123Z");
        assertThat(toJsonValueUpperBound(TIMESTAMP_TZ_MICROS, subMillisecond)).isEqualTo("2024-01-15T10:30:00.124Z");
        assertThat(toJsonValueUpperBound(TIMESTAMP_TZ_MICROS, LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, 0, UTC_KEY))).isEqualTo("2024-01-15T10:30:00.123Z");
    }

    @Test
    public void testTimestampWithTimeZoneStatisticsInt64Micros()
    {
        String columnName = "t_timestamp";
        Statistics<?> stats = Statistics.getBuilderForReading(int64TimestampType(columnName, true, LogicalTypeAnnotation.TimeUnit.MICROS))
                .withMin(timestampToBytes(LocalDateTime.parse("2020-08-26T01:02:03.123456")))
                .withMax(timestampToBytes(LocalDateTime.parse("2020-08-26T01:02:03.987654")))
                .withNumNulls(2)
                .build();

        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_TZ_MICROS)))
                .isEqualTo(ImmutableMap.of(columnName, "2020-08-26T01:02:03.123Z"));
        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_TZ_MICROS)))
                .isEqualTo(ImmutableMap.of(columnName, "2020-08-26T01:02:03.988Z"));
    }

    @Test
    public void testTimestampWithTimeZoneStatisticsInt64Millis()
    {
        String columnName = "t_timestamp";
        Statistics<?> stats = Statistics.getBuilderForReading(int64TimestampType(columnName, false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .withMin(longToBytes(Instant.parse("2020-08-26T01:02:03.123Z").toEpochMilli()))
                .withMax(longToBytes(Instant.parse("2020-08-26T01:02:03.124Z").toEpochMilli()))
                .withNumNulls(2)
                .build();

        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_TZ_MICROS)))
                .isEqualTo(ImmutableMap.of(columnName, "2020-08-26T01:02:03.123Z"));
        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_TZ_MICROS)))
                .isEqualTo(ImmutableMap.of(columnName, "2020-08-26T01:02:03.124Z"));
    }

    @Test
    public void testTimestampWithTimeZoneStatisticsInt64Nanos()
    {
        String columnName = "t_timestamp";
        Statistics<?> stats = Statistics.getBuilderForReading(int64TimestampType(columnName, true, LogicalTypeAnnotation.TimeUnit.NANOS))
                .withMin(longToBytes(1_598_403_723_123_456_789L))
                .withMax(longToBytes(1_598_403_723_987_654_321L))
                .withNumNulls(2)
                .build();

        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_TZ_MICROS)))
                .isEqualTo(ImmutableMap.of(columnName, "2020-08-26T01:02:03.123Z"));
        assertThat(DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_TZ_MICROS)))
                .isEqualTo(ImmutableMap.of(columnName, "2020-08-26T01:02:03.988Z"));
    }

    private static PrimitiveType int64TimestampType(String columnName, boolean isAdjustedToUTC, LogicalTypeAnnotation.TimeUnit unit)
    {
        return Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(isAdjustedToUTC, unit))
                .named(columnName);
    }

    private static byte[] longToBytes(long value)
    {
        Slice slice = Slices.allocate(8);
        slice.setLong(0, value);
        return slice.byteArray();
    }

    @Test
    public void testTimestampUpperBound()
    {
        long subMillisecond = Instant.parse("2024-01-15T10:30:00Z").getEpochSecond() * MICROSECONDS_PER_SECOND + 123_456;
        assertThat(toJsonValue(TIMESTAMP_MICROS, subMillisecond)).isEqualTo("2024-01-15T10:30:00.123Z");
        assertThat(toJsonValueUpperBound(TIMESTAMP_MICROS, subMillisecond)).isEqualTo("2024-01-15T10:30:00.124Z");
        assertThat(toJsonValueUpperBound(TIMESTAMP_MICROS, subMillisecond - 456)).isEqualTo("2024-01-15T10:30:00.123Z");

        assertThat(jsonValueToTrinoValue(TIMESTAMP_TZ_MICROS, "2024-01-15T10:30:00.123456Z")).isEqualTo(LongTimestampWithTimeZone.fromEpochMillisAndFraction(Instant.parse("2024-01-15T10:30:00.123Z").toEpochMilli(), 456_000_000, UTC_KEY));
        assertThat(jsonValueToTrinoValueUpperBound(TIMESTAMP_TZ_MICROS, "2024-01-15T10:30:00.123456Z")).isEqualTo(LongTimestampWithTimeZone.fromEpochMillisAndFraction(Instant.parse("2024-01-15T10:30:00.124Z").toEpochMilli(), 0, UTC_KEY));
        assertThat(jsonValueToTrinoValueUpperBound(TIMESTAMP_TZ_MICROS, "2024-01-15T10:30:00.123Z")).isEqualTo(LongTimestampWithTimeZone.fromEpochMillisAndFraction(Instant.parse("2024-01-15T10:30:00.123Z").toEpochMilli(), 0, UTC_KEY));
    }

    @Test
    public void testNestedTimestampUpperBound()
    {
        RowType microsRow = RowType.rowType(field("ts", TIMESTAMP_MICROS));
        long subMillisecond = Instant.parse("2024-01-15T10:30:00Z").getEpochSecond() * MICROSECONDS_PER_SECOND + 123_456;
        SqlRow row = buildRowValue(microsRow, fields -> TIMESTAMP_MICROS.writeLong(fields.get(0), subMillisecond));
        assertThat(toJsonValueUpperBound(microsRow, row)).isEqualTo(ImmutableMap.of("ts", "2024-01-15T10:30:00.124Z"));
    }

    @Test
    public void testConvertParquetToJsonStatisticsRoundsMaximumUp()
    {
        long subMillisecond = Instant.parse("2024-01-15T10:30:00Z").getEpochSecond() * MICROSECONDS_PER_SECOND + 123_456;
        DeltaLakeParquetFileStatistics statistics = new DeltaLakeParquetFileStatistics(
                Optional.of(1L),
                Optional.of(ImmutableMap.of("ts", subMillisecond)),
                Optional.of(ImmutableMap.of("ts", subMillisecond)),
                Optional.of(ImmutableMap.of("ts", 0L)));

        DeltaLakeJsonFileStatistics jsonStatistics = convertParquetToJsonStatistics(ImmutableMap.of("ts", TIMESTAMP_MICROS), statistics);
        assertThat(jsonStatistics.getMinValues()).contains(ImmutableMap.of("ts", "2024-01-15T10:30:00.123Z"));
        assertThat(jsonStatistics.getMaxValues()).contains(ImmutableMap.of("ts", "2024-01-15T10:30:00.124Z"));
    }

    @Test
    public void testTightBounds()
    {
        assertThat(tightBounds(ImmutableList.of(IntegerType.INTEGER, TIMESTAMP_TZ_MILLIS))).isEmpty();
        assertThat(tightBounds(ImmutableList.of(IntegerType.INTEGER, TIMESTAMP_MICROS))).contains(false);
        assertThat(tightBounds(ImmutableList.of(TIMESTAMP_TZ_MICROS))).contains(false);
    }

    private static byte[] toParquetEncoding(LocalDateTime time)
    {
        long timeOfDayNanos = (long) time.getNano() + (time.toEpochSecond(UTC) - time.toLocalDate().atStartOfDay().toEpochSecond(UTC)) * 1_000_000_000;

        Slice slice = Slices.allocate(12);
        slice.setLong(0, timeOfDayNanos);
        slice.setInt(8, millisToJulianDay(time.toInstant(UTC).toEpochMilli()));
        return slice.byteArray();
    }

    static final int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;

    private static int millisToJulianDay(long timestamp)
    {
        return toIntExact(MILLISECONDS.toDays(timestamp) + JULIAN_EPOCH_OFFSET_DAYS);
    }

    static byte[] getIntByteArray(int i)
    {
        return ByteBuffer.allocate(4).order(LITTLE_ENDIAN).putInt(i).array();
    }

    static byte[] getFloatByteArray(float f)
    {
        return ByteBuffer.allocate(4).order(LITTLE_ENDIAN).putFloat(f).array();
    }

    static byte[] getDoubleByteArray(double d)
    {
        return ByteBuffer.allocate(8).order(LITTLE_ENDIAN).putDouble(d).array();
    }
}
