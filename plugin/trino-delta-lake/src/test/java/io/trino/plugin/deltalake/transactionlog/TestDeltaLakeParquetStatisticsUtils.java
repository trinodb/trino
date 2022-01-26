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
import io.trino.spi.type.IntegerType;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;

import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
                DeltaLakeParquetStatisticsUtils.jsonEncodeMin(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, IntegerType.INTEGER)),
                ImmutableMap.of(columnName, -100));
        assertEquals(
                DeltaLakeParquetStatisticsUtils.jsonEncodeMax(ImmutableMap.of(columnName, Optional.of(stats)), ImmutableMap.of(columnName, IntegerType.INTEGER)),
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
