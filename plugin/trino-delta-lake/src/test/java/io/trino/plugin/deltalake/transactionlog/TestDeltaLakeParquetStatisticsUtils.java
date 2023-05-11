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

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.type.DoubleType;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

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

        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, INTEGER)),
                ImmutableMap.of(columnName, -100));
        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, INTEGER)),
                ImmutableMap.of(columnName, 150));
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

        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, createUnboundedVarcharType())),
                ImmutableMap.of(columnName, "abc"));
        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, createUnboundedVarcharType())),
                ImmutableMap.of(columnName, "bac"));
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

        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, REAL)),
                ImmutableMap.of(columnName, 100.0f));
        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, REAL)),
                ImmutableMap.of(columnName, 1000.001f));

        columnName = "t_double";
        type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, columnName);
        stats = Statistics.getBuilderForReading(type)
                .withMin(getDoubleByteArray(100.0))
                .withMax(getDoubleByteArray(1000.001))
                .withNumNulls(2)
                .build();

        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, DoubleType.DOUBLE)),
                ImmutableMap.of(columnName, 100.0));
        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, DoubleType.DOUBLE)),
                ImmutableMap.of(columnName, 1000.001));
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

        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, DATE)),
                ImmutableMap.of(columnName, "2020-08-26"));
        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, DATE)),
                ImmutableMap.of(columnName, "2020-09-17"));
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

        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_TZ_MILLIS)),
                ImmutableMap.of(columnName, "2020-08-26T01:02:03.123Z"));
        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_TZ_MILLIS)),
                ImmutableMap.of(columnName, "2020-08-26T01:02:03.124Z"));
    }

    @Test
    public void testTimestampStatisticsMillisPrecision()
    {
        String columnName = "t_timestamp";
        PrimitiveType type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT96, columnName);
        Statistics<?> stats = Statistics.getBuilderForReading(type)
                .withMin(toParquetEncoding(LocalDateTime.parse("2020-08-26T01:02:03.123")))
                .withMax(toParquetEncoding(LocalDateTime.parse("2020-08-26T01:02:03.123")))
                .withNumNulls(2)
                .build();

        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_TZ_MILLIS)),
                ImmutableMap.of(columnName, "2020-08-26T01:02:03.123Z"));
        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, TIMESTAMP_TZ_MILLIS)),
                ImmutableMap.of(columnName, "2020-08-26T01:02:03.123Z"));
    }

    @Test
    public void testNestedFieldStatistics()
    {
        String stringColumn = "t_grandparent.t_parent.t_string";
        PrimitiveType stringType = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, stringColumn);
        Statistics<?> stringColumnStats = Statistics.getBuilderForReading(stringType)
                .withMin("abc".getBytes(UTF_8))
                .withMax("bac".getBytes(UTF_8))
                .withNumNulls(1)
                .build();

        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(stringColumn, Optional.of(stringColumnStats)), ImmutableMap.of(stringColumn, createUnboundedVarcharType())),
                ImmutableMap.of("t_grandparent", ImmutableMap.of("t_parent", ImmutableMap.of("t_string", "abc"))));
        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(stringColumn, Optional.of(stringColumnStats)), ImmutableMap.of(stringColumn, createUnboundedVarcharType())),
                ImmutableMap.of("t_grandparent", ImmutableMap.of("t_parent", ImmutableMap.of("t_string", "bac"))));

        String booleanColumn = "t_grandparent.t_parent.t_boolean";
        PrimitiveType booleanType = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BOOLEAN, booleanColumn);
        Statistics<?> booleanColumnStats = Statistics.getBuilderForReading(booleanType)
                .withMin(getBooleanByteArray(false))
                .withMax(getBooleanByteArray(true))
                .withNumNulls(1)
                .build();

        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(booleanColumn, Optional.of(booleanColumnStats)), ImmutableMap.of(booleanColumn, BOOLEAN)),
                ImmutableMap.of("t_grandparent", ImmutableMap.of("t_parent", ImmutableMap.of())));
        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(booleanColumn, Optional.of(booleanColumnStats)), ImmutableMap.of(booleanColumn, BOOLEAN)),
                ImmutableMap.of("t_grandparent", ImmutableMap.of("t_parent", ImmutableMap.of())));

        String nullColumn = "t_parent.t_null";
        PrimitiveType nullColumnType = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, nullColumn);
        Statistics<?> nullColumnStats = Statistics.getBuilderForReading(nullColumnType)
                .withMin(null)
                .withMax(null)
                .withNumNulls(1)
                .build();

        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(nullColumn, Optional.of(nullColumnStats)), ImmutableMap.of(nullColumn, INTEGER)),
                ImmutableMap.of("t_parent", ImmutableMap.of()));
        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(nullColumn, Optional.of(nullColumnStats)), ImmutableMap.of(nullColumn, INTEGER)),
                ImmutableMap.of("t_parent", ImmutableMap.of()));
    }

    @Test
    public void testMultipleNestedFieldStatistics()
    {
        String stringColumn = "t_grandparent.t_parent.t_string";
        PrimitiveType stringType = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, stringColumn);
        Statistics<?> stringColumnStats = Statistics.getBuilderForReading(stringType)
                .withMin("abc".getBytes(UTF_8))
                .withMax("bac".getBytes(UTF_8))
                .withNumNulls(1)
                .build();

        String booleanColumn = "t_grandparent.t_parent.t_boolean";
        PrimitiveType booleanType = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BOOLEAN, booleanColumn);
        Statistics<?> booleanColumnStats = Statistics.getBuilderForReading(booleanType)
                .withMin(getBooleanByteArray(false))
                .withMax(getBooleanByteArray(true))
                .withNumNulls(1)
                .build();

        String timestampColumn = "t_grandparent.t_timestamp";
        PrimitiveType timestampType = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT96, timestampColumn);
        Statistics<?> timestampColumnStats = Statistics.getBuilderForReading(timestampType)
                .withMin(toParquetEncoding(LocalDateTime.parse("2020-08-26T01:02:03.123456789")))
                .withMax(toParquetEncoding(LocalDateTime.parse("2020-08-26T01:02:03.123987654")))
                .withNumNulls(2)
                .build();

        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMin(
                        ImmutableMap.of(stringColumn, Optional.of(stringColumnStats), booleanColumn, Optional.of(booleanColumnStats), timestampColumn, Optional.of(timestampColumnStats)),
                        ImmutableMap.of(stringColumn, createUnboundedVarcharType(), booleanColumn, BOOLEAN, timestampColumn, TIMESTAMP_TZ_MILLIS)),
                ImmutableMap.of("t_grandparent", ImmutableMap.of("t_parent", ImmutableMap.of("t_string", "abc"), "t_timestamp", "2020-08-26T01:02:03.123Z")));
        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMax(
                        ImmutableMap.of(stringColumn, Optional.of(stringColumnStats), booleanColumn, Optional.of(booleanColumnStats), timestampColumn, Optional.of(timestampColumnStats)),
                        ImmutableMap.of(stringColumn, createUnboundedVarcharType(), booleanColumn, BOOLEAN, timestampColumn, TIMESTAMP_TZ_MILLIS)),
                ImmutableMap.of("t_grandparent", ImmutableMap.of("t_parent", ImmutableMap.of("t_string", "bac"), "t_timestamp", "2020-08-26T01:02:03.124Z")));
    }

    @Test
    public void testPopulateNestedStats()
    {
        Map<String, Optional<Object>> allStats = new HashMap<>();
        allStats.put("base1", Optional.of(2));
        allStats.put("base2", Optional.of("base2Value"));
        allStats.put("base3", Optional.of(100L));
        allStats.put("base4", Optional.empty()); // should get discard
        allStats.put("base5", null); // should get discard
        allStats.put("base6.f1", Optional.empty()); // should get discard
        allStats.put("base6.f2", Optional.of(99.99));
        allStats.put("base6.f3", Optional.of("2020-08-26T01:02:03.123Z"));
        allStats.put("base7.level1.f4", null); // should get discard
        allStats.put("base7.level1.f5", Optional.empty()); // should get discard
        allStats.put("base7.level1.f6", Optional.of("base5Level1F6Value"));

        assertEquals(
                DeltaLakeParquetStatisticsUtils.convertNestedMapKeys(allStats),
                ImmutableMap.of(
                        "base1", 2,
                        "base2", "base2Value",
                        "base3", 100L,
                        "base6", ImmutableMap.of("f2", 99.99, "f3", "2020-08-26T01:02:03.123Z"),
                        "base7", ImmutableMap.of("level1", ImmutableMap.of("f6", "base5Level1F6Value"))));
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

    static byte[] getBooleanByteArray(boolean b)
    {
        return ByteBuffer.allocate(1).put((byte) (b ? 1 : 0)).array();
    }
}
