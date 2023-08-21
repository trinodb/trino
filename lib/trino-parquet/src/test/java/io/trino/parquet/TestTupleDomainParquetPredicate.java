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
package io.trino.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.math.LongMath;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.predicate.DictionaryDescriptor;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.LittleEndianDataOutputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static io.trino.parquet.ParquetTimestampUtils.JULIAN_EPOCH_OFFSET_DAYS;
import static io.trino.parquet.predicate.TupleDomainParquetPredicate.getDomain;
import static io.trino.spi.predicate.Domain.all;
import static io.trino.spi.predicate.Domain.create;
import static io.trino.spi.predicate.Domain.notNull;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.NaN;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoField.MICRO_OF_SECOND;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTupleDomainParquetPredicate
{
    private static final ParquetDataSourceId ID = new ParquetDataSourceId("testFile");

    @Test
    public void testBoolean()
            throws ParquetCorruptionException
    {
        ColumnDescriptor columnDescriptor = createColumnDescriptor(PrimitiveTypeName.BOOLEAN, "BooleanColumn");
        assertEquals(getDomain(columnDescriptor, BOOLEAN, 0L, null, ID, UTC), all(BOOLEAN));

        assertEquals(getDomain(columnDescriptor, BOOLEAN, 10, booleanColumnStats(true, true), ID, UTC), singleValue(BOOLEAN, true));
        assertEquals(getDomain(columnDescriptor, BOOLEAN, 10, booleanColumnStats(false, false), ID, UTC), singleValue(BOOLEAN, false));

        assertEquals(getDomain(columnDescriptor, BOOLEAN, 20, booleanColumnStats(false, true), ID, UTC), all(BOOLEAN));
    }

    private static BooleanStatistics booleanColumnStats(boolean minimum, boolean maximum)
    {
        return (BooleanStatistics) Statistics.getBuilderForReading(Types.optional(PrimitiveTypeName.BOOLEAN).named("BooleanColumn"))
                .withMin(BytesUtils.booleanToBytes(minimum))
                .withMax(BytesUtils.booleanToBytes(maximum))
                .withNumNulls(0)
                .build();
    }

    @Test
    public void testBigint()
            throws ParquetCorruptionException
    {
        ColumnDescriptor columnDescriptor = createColumnDescriptor(INT64, "BigintColumn");
        assertEquals(getDomain(columnDescriptor, BIGINT, 0, null, ID, UTC), all(BIGINT));

        assertEquals(getDomain(columnDescriptor, BIGINT, 10, longColumnStats(100L, 100L), ID, UTC), singleValue(BIGINT, 100L));

        assertEquals(getDomain(columnDescriptor, BIGINT, 10, longColumnStats(0L, 100L), ID, UTC), create(ValueSet.ofRanges(range(BIGINT, 0L, true, 100L, true)), false));

        assertEquals(getDomain(columnDescriptor, BIGINT, 20, longOnlyNullsStats(10), ID, UTC), Domain.all(BIGINT));
        assertEquals(getDomain(columnDescriptor, BIGINT, 20, longOnlyNullsStats(20), ID, UTC), Domain.onlyNull(BIGINT));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(columnDescriptor, BIGINT, 10, longColumnStats(100L, 10L), ID, UTC))
                .withMessage("Corrupted statistics for column \"[] required int64 BigintColumn\" in Parquet file \"testFile\": [min: 100, max: 10, num_nulls: 0]");
    }

    @Test
    public void testInteger()
            throws ParquetCorruptionException
    {
        ColumnDescriptor columnDescriptor = createColumnDescriptor(INT32, "IntegerColumn");
        assertEquals(getDomain(columnDescriptor, INTEGER, 0, null, ID, UTC), all(INTEGER));

        assertEquals(getDomain(columnDescriptor, INTEGER, 10, longColumnStats(100, 100), ID, UTC), singleValue(INTEGER, 100L));

        assertEquals(getDomain(columnDescriptor, INTEGER, 10, longColumnStats(0, 100), ID, UTC), create(ValueSet.ofRanges(range(INTEGER, 0L, true, 100L, true)), false));

        assertEquals(getDomain(columnDescriptor, INTEGER, 20, longColumnStats(0, 2147483648L), ID, UTC), notNull(INTEGER));

        assertEquals(getDomain(columnDescriptor, INTEGER, 20, longOnlyNullsStats(10), ID, UTC), create(ValueSet.all(INTEGER), true));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(columnDescriptor, INTEGER, 10, longColumnStats(2147483648L, 10), ID, UTC))
                .withMessage("Corrupted statistics for column \"[] required int32 IntegerColumn\" in Parquet file \"testFile\": [min: 2147483648, max: 10, num_nulls: 0]");
    }

    @Test
    public void testSmallint()
            throws ParquetCorruptionException
    {
        ColumnDescriptor columnDescriptor = createColumnDescriptor(INT32, "SmallintColumn");
        assertEquals(getDomain(columnDescriptor, SMALLINT, 0, null, ID, UTC), all(SMALLINT));

        assertEquals(getDomain(columnDescriptor, SMALLINT, 10, longColumnStats(100, 100), ID, UTC), singleValue(SMALLINT, 100L));

        assertEquals(getDomain(columnDescriptor, SMALLINT, 10, longColumnStats(0, 100), ID, UTC), create(ValueSet.ofRanges(range(SMALLINT, 0L, true, 100L, true)), false));

        assertEquals(getDomain(columnDescriptor, SMALLINT, 20, longColumnStats(0, 2147483648L), ID, UTC), notNull(SMALLINT));

        assertEquals(getDomain(columnDescriptor, SMALLINT, 20, longOnlyNullsStats(10), ID, UTC), create(ValueSet.all(SMALLINT), true));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(columnDescriptor, SMALLINT, 10, longColumnStats(2147483648L, 10), ID, UTC))
                .withMessage("Corrupted statistics for column \"[] required int32 SmallintColumn\" in Parquet file \"testFile\": [min: 2147483648, max: 10, num_nulls: 0]");
    }

    @Test
    public void testTinyint()
            throws ParquetCorruptionException
    {
        ColumnDescriptor columnDescriptor = createColumnDescriptor(INT32, "TinyintColumn");
        assertEquals(getDomain(columnDescriptor, TINYINT, 0, null, ID, UTC), all(TINYINT));

        assertEquals(getDomain(columnDescriptor, TINYINT, 10, longColumnStats(100, 100), ID, UTC), singleValue(TINYINT, 100L));

        assertEquals(getDomain(columnDescriptor, TINYINT, 10, longColumnStats(0, 100), ID, UTC), create(ValueSet.ofRanges(range(TINYINT, 0L, true, 100L, true)), false));

        assertEquals(getDomain(columnDescriptor, TINYINT, 20, longColumnStats(0, 2147483648L), ID, UTC), notNull(TINYINT));

        assertEquals(getDomain(columnDescriptor, TINYINT, 20, longOnlyNullsStats(10), ID, UTC), create(ValueSet.all(TINYINT), true));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(columnDescriptor, TINYINT, 10, longColumnStats(2147483648L, 10), ID, UTC))
                .withMessage("Corrupted statistics for column \"[] required int32 TinyintColumn\" in Parquet file \"testFile\": [min: 2147483648, max: 10, num_nulls: 0]");
    }

    @Test
    public void testShortDecimal()
            throws Exception
    {
        ColumnDescriptor columnDescriptor = createColumnDescriptor(INT32, "ShortDecimalColumn");
        Type type = createDecimalType(5, 2);
        assertEquals(getDomain(columnDescriptor, type, 0, null, ID, UTC), all(type));

        assertEquals(getDomain(columnDescriptor, type, 10, longColumnStats(10012L, 10012L), ID, UTC), singleValue(type, 10012L));
        // Test that statistics overflowing the size of the type are not used
        assertEquals(getDomain(columnDescriptor, type, 10, longColumnStats(100012L, 100012L), ID, UTC), notNull(type));

        assertEquals(getDomain(columnDescriptor, type, 10, longColumnStats(0L, 100L), ID, UTC), create(ValueSet.ofRanges(range(type, 0L, true, 100L, true)), false));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(columnDescriptor, type, 10, longColumnStats(100L, 10L), ID, UTC))
                .withMessage("Corrupted statistics for column \"[] required int32 ShortDecimalColumn\" in Parquet file \"testFile\": [min: 100, max: 10, num_nulls: 0]");
    }

    @Test
    public void testShortDecimalWithNoScale()
            throws Exception
    {
        ColumnDescriptor columnDescriptor = createColumnDescriptor(INT32, "ShortDecimalColumnWithNoScale");
        Type type = createDecimalType(5, 0);
        assertEquals(getDomain(columnDescriptor, type, 0, null, ID, UTC), all(type));

        assertEquals(getDomain(columnDescriptor, type, 10, longColumnStats(100L, 100L), ID, UTC), singleValue(type, 100L));
        // Test that statistics overflowing the size of the type are not used
        assertEquals(getDomain(columnDescriptor, type, 10, longColumnStats(123456L, 123456), ID, UTC), notNull(type));

        assertEquals(getDomain(columnDescriptor, type, 10, longColumnStats(0L, 100L), ID, UTC), create(ValueSet.ofRanges(range(type, 0L, true, 100L, true)), false));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(columnDescriptor, type, 10, longColumnStats(100L, 10L), ID, UTC))
                .withMessage("Corrupted statistics for column \"[] required int32 ShortDecimalColumnWithNoScale\" in Parquet file \"testFile\": [min: 100, max: 10, num_nulls: 0]");
    }

    @Test
    public void testLongDecimal()
            throws Exception
    {
        ColumnDescriptor columnDescriptor = createColumnDescriptor(FIXED_LEN_BYTE_ARRAY, "LongDecimalColumn");
        DecimalType type = createDecimalType(20, 5);
        BigInteger maximum = new BigInteger("12345678901234512345");

        Int128 zero = Int128.ZERO;
        Int128 hundred = Int128.valueOf(100L);
        Int128 max = Int128.valueOf(maximum);

        assertEquals(getDomain(columnDescriptor, type, 0, null, ID, UTC), all(type));
        assertEquals(getDomain(columnDescriptor, type, 10, binaryColumnStats(maximum, maximum), ID, UTC), singleValue(type, max));

        assertEquals(
                getDomain(columnDescriptor, type, 10, binaryColumnStats(0L, 100L), ID, UTC),
                create(ValueSet.ofRanges(range(type, zero, true, hundred, true)), false));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(columnDescriptor, type, 10, binaryColumnStats(100L, 10L), ID, UTC))
                .withMessage("Corrupted statistics for column \"[] required fixed_len_byte_array(0) LongDecimalColumn\" in Parquet file \"testFile\": [min: 0x64, max: 0x0A, num_nulls: 0]");
    }

    @Test
    public void testLongDecimalWithNoScale()
            throws Exception
    {
        ColumnDescriptor columnDescriptor = createColumnDescriptor(FIXED_LEN_BYTE_ARRAY, "LongDecimalColumnWithNoScale");
        DecimalType type = createDecimalType(20, 0);
        Int128 zero = Int128.ZERO;
        Int128 hundred = Int128.valueOf(100L);
        assertEquals(getDomain(columnDescriptor, type, 0, null, ID, UTC), all(type));

        assertEquals(getDomain(columnDescriptor, type, 10, binaryColumnStats(100L, 100L), ID, UTC), singleValue(type, hundred));

        assertEquals(getDomain(columnDescriptor, type, 10, binaryColumnStats(0L, 100L), ID, UTC), create(ValueSet.ofRanges(range(type, zero, true, hundred, true)), false));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(columnDescriptor, type, 10, binaryColumnStats(100L, 10L), ID, UTC))
                .withMessage("Corrupted statistics for column \"[] required fixed_len_byte_array(0) LongDecimalColumnWithNoScale\" in Parquet file \"testFile\": [min: 0x64, max: 0x0A, num_nulls: 0]");
    }

    @Test
    public void testDouble()
            throws Exception
    {
        ColumnDescriptor columnDescriptor = createColumnDescriptor(PrimitiveTypeName.DOUBLE, "DoubleColumn");
        assertEquals(getDomain(columnDescriptor, DOUBLE, 0, null, ID, UTC), all(DOUBLE));

        assertEquals(getDomain(columnDescriptor, DOUBLE, 10, doubleColumnStats(42.24, 42.24), ID, UTC), singleValue(DOUBLE, 42.24));

        assertEquals(getDomain(columnDescriptor, DOUBLE, 10, doubleColumnStats(3.3, 42.24), ID, UTC), create(ValueSet.ofRanges(range(DOUBLE, 3.3, true, 42.24, true)), false));

        assertEquals(getDomain(columnDescriptor, DOUBLE, 10, doubleColumnStats(NaN, NaN), ID, UTC), Domain.notNull(DOUBLE));

        assertEquals(getDomain(columnDescriptor, DOUBLE, 10, doubleColumnStats(NaN, NaN, true), ID, UTC), Domain.all(DOUBLE));

        assertEquals(getDomain(columnDescriptor, DOUBLE, 10, doubleColumnStats(3.3, NaN), ID, UTC), Domain.notNull(DOUBLE));

        assertEquals(getDomain(columnDescriptor, DOUBLE, 10, doubleColumnStats(3.3, NaN, true), ID, UTC), Domain.all(DOUBLE));

        assertEquals(getDomain(DOUBLE, doubleDictionaryDescriptor(NaN)), Domain.all(DOUBLE));

        assertEquals(getDomain(DOUBLE, doubleDictionaryDescriptor(3.3, NaN)), Domain.all(DOUBLE));

        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(columnDescriptor, DOUBLE, 10, doubleColumnStats(42.24, 3.3), ID, UTC))
                .withMessage("Corrupted statistics for column \"[] required double DoubleColumn\" in Parquet file \"testFile\": [min: 42.24, max: 3.3, num_nulls: 0]");
    }

    @Test
    public void testString()
            throws ParquetCorruptionException
    {
        ColumnDescriptor columnDescriptor = createColumnDescriptor(BINARY, "StringColumn");
        assertEquals(getDomain(columnDescriptor, createUnboundedVarcharType(), 0, null, ID, UTC), all(createUnboundedVarcharType()));

        assertEquals(getDomain(columnDescriptor, createUnboundedVarcharType(), 10, stringColumnStats("taco", "taco"), ID, UTC), singleValue(createUnboundedVarcharType(), utf8Slice("taco")));

        assertEquals(getDomain(columnDescriptor, createUnboundedVarcharType(), 10, stringColumnStats("apple", "taco"), ID, UTC), create(ValueSet.ofRanges(range(createUnboundedVarcharType(), utf8Slice("apple"), true, utf8Slice("taco"), true)), false));

        assertEquals(getDomain(columnDescriptor, createUnboundedVarcharType(), 10, stringColumnStats("中国", "美利坚"), ID, UTC), create(ValueSet.ofRanges(range(createUnboundedVarcharType(), utf8Slice("中国"), true, utf8Slice("美利坚"), true)), false));

        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(columnDescriptor, createUnboundedVarcharType(), 10, stringColumnStats("taco", "apple"), ID, UTC))
                .withMessage("Corrupted statistics for column \"[] required binary StringColumn\" in Parquet file \"testFile\": [min: 0x7461636F, max: 0x6170706C65, num_nulls: 0]");
    }

    private static BinaryStatistics stringColumnStats(String minimum, String maximum)
    {
        return (BinaryStatistics) Statistics.getBuilderForReading(Types.optional(BINARY).named("StringColumn"))
                .withMin(minimum.getBytes(UTF_8))
                .withMax(maximum.getBytes(UTF_8))
                .withNumNulls(0)
                .build();
    }

    @Test
    public void testFloat()
            throws Exception
    {
        ColumnDescriptor columnDescriptor = createColumnDescriptor(FLOAT, "FloatColumn");
        assertEquals(getDomain(columnDescriptor, REAL, 0, null, ID, UTC), all(REAL));

        float minimum = 4.3f;
        float maximum = 40.3f;

        assertEquals(getDomain(columnDescriptor, REAL, 10, floatColumnStats(minimum, minimum), ID, UTC), singleValue(REAL, (long) floatToRawIntBits(minimum)));

        assertEquals(
                getDomain(columnDescriptor, REAL, 10, floatColumnStats(minimum, maximum), ID, UTC),
                create(ValueSet.ofRanges(range(REAL, (long) floatToRawIntBits(minimum), true, (long) floatToRawIntBits(maximum), true)), false));

        assertEquals(getDomain(columnDescriptor, REAL, 10, floatColumnStats(NaN, NaN), ID, UTC), Domain.notNull(REAL));

        assertEquals(getDomain(columnDescriptor, REAL, 10, floatColumnStats(NaN, NaN, true), ID, UTC), Domain.all(REAL));

        assertEquals(getDomain(columnDescriptor, REAL, 10, floatColumnStats(minimum, NaN), ID, UTC), Domain.notNull(REAL));

        assertEquals(getDomain(columnDescriptor, REAL, 10, floatColumnStats(minimum, NaN, true), ID, UTC), Domain.all(REAL));

        assertEquals(getDomain(REAL, floatDictionaryDescriptor(NaN)), Domain.all(REAL));

        assertEquals(getDomain(REAL, floatDictionaryDescriptor(minimum, NaN)), Domain.all(REAL));

        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(columnDescriptor, REAL, 10, floatColumnStats(maximum, minimum), ID, UTC))
                .withMessage("Corrupted statistics for column \"[] required float FloatColumn\" in Parquet file \"testFile\": [min: 40.3, max: 4.3, num_nulls: 0]");
    }

    @Test
    public void testDate()
            throws ParquetCorruptionException
    {
        ColumnDescriptor columnDescriptor = createColumnDescriptor(INT32, "DateColumn");
        assertEquals(getDomain(columnDescriptor, DATE, 0, null, ID, UTC), all(DATE));
        assertEquals(getDomain(columnDescriptor, DATE, 10, intColumnStats(100, 100), ID, UTC), singleValue(DATE, 100L));
        assertEquals(getDomain(columnDescriptor, DATE, 10, intColumnStats(0, 100), ID, UTC), create(ValueSet.ofRanges(range(DATE, 0L, true, 100L, true)), false));

        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(columnDescriptor, DATE, 10, intColumnStats(200, 100), ID, UTC))
                .withMessage("Corrupted statistics for column \"[] required int32 DateColumn\" in Parquet file \"testFile\": [min: 200, max: 100, num_nulls: 0]");
    }

    @DataProvider
    public Object[][] timestampPrecision()
    {
        LocalDateTime baseTime = LocalDateTime.of(1970, 1, 19, 10, 28, 52, 123456789);
        return new Object[][] {
                {3, baseTime, baseTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli() * MICROSECONDS_PER_MILLISECOND},
                // note the rounding of micros
                {6, baseTime, baseTime.atZone(ZoneOffset.UTC).toInstant().getEpochSecond() * MICROSECONDS_PER_SECOND + 123457},
                {9, baseTime, longTimestamp(9, baseTime)}
        };
    }

    @Test(dataProvider = "timestampPrecision")
    public void testTimestampInt96(int precision, LocalDateTime baseTime, Object baseDomainValue)
            throws ParquetCorruptionException
    {
        ColumnDescriptor columnDescriptor = createColumnDescriptor(INT96, "TimestampColumn");
        TimestampType timestampType = createTimestampType(precision);
        assertEquals(getDomain(columnDescriptor, timestampType, 0, null, ID, UTC), all(timestampType));
        assertEquals(getDomain(columnDescriptor, timestampType, 10, timestampColumnStats(baseTime, baseTime), ID, UTC), singleValue(timestampType, baseDomainValue));
        // INT96 binary ranges ignored when min <> max
        assertEquals(
                getDomain(columnDescriptor, timestampType, 10, timestampColumnStats(baseTime.minusSeconds(10), baseTime), ID, UTC),
                create(ValueSet.all(timestampType), false));
    }

    @Test(dataProvider = "testTimestampInt64DataProvider")
    public void testTimestampInt64(TimeUnit timeUnit, int precision, LocalDateTime baseTime, Object baseDomainValue)
            throws ParquetCorruptionException
    {
        int parquetPrecision;
        switch (timeUnit) {
            case MILLIS:
                parquetPrecision = 3;
                break;
            case MICROS:
                parquetPrecision = 6;
                break;
            case NANOS:
                parquetPrecision = 9;
                break;
            default:
                throw new IllegalArgumentException("Unknown Parquet TimeUnit " + timeUnit);
        }

        PrimitiveType type = Types.required(INT64)
                .as(LogicalTypeAnnotation.timestampType(false, timeUnit))
                .named("TimestampColumn");

        ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[]{}, type, 0, 0);
        TimestampType timestampType = createTimestampType(precision);
        assertEquals(getDomain(columnDescriptor, timestampType, 0, null, ID, UTC), all(timestampType));
        LocalDateTime maxTime = baseTime.plus(Duration.ofMillis(50));

        Object maxDomainValue;
        if (baseDomainValue instanceof Long value) {
            maxDomainValue = value + 50 * MICROSECONDS_PER_MILLISECOND;
        }
        else if (baseDomainValue instanceof LongTimestamp longTimestamp) {
            maxDomainValue = new LongTimestamp(longTimestamp.getEpochMicros() + 50 * MICROSECONDS_PER_MILLISECOND, longTimestamp.getPicosOfMicro());
        }
        else {
            throw new IllegalArgumentException("Unknown Timestamp domain type " + baseDomainValue);
        }

        long minValue = toEpochWithPrecision(baseTime, parquetPrecision);
        long maxValue = toEpochWithPrecision(maxTime, parquetPrecision);
        assertEquals(getDomain(columnDescriptor, timestampType, 10, longColumnStats(minValue, minValue), ID, UTC), singleValue(timestampType, baseDomainValue));
        assertEquals(getDomain(columnDescriptor, timestampType, 10, longColumnStats(minValue, maxValue), ID, UTC),
                Domain.create(ValueSet.ofRanges(range(timestampType, baseDomainValue, true, maxDomainValue, true)), false));
    }

    @DataProvider
    public Object[][] testTimestampInt64DataProvider()
    {
        LocalDateTime baseTime = LocalDateTime.of(1970, 1, 19, 10, 28, 52, 123456789);
        Object millisExpectedValue = baseTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli() * MICROSECONDS_PER_MILLISECOND;
        // note the rounding of micros
        Object microsExpectedValue = baseTime.atZone(ZoneOffset.UTC).toInstant().getEpochSecond() * MICROSECONDS_PER_SECOND + 123457;
        Object nanosExpectedValue = longTimestamp(9, baseTime);

        Object nanosTruncatedToMillisExpectedValue = longTimestamp(
                9,
                LocalDateTime.of(1970, 1, 19, 10, 28, 52, 123000000));
        Object nanosTruncatedToMicrosExpectedValue = longTimestamp(
                9,
                LocalDateTime.of(1970, 1, 19, 10, 28, 52, 123457000));
        return new Object[][] {
                {TimeUnit.MILLIS, 3, baseTime, millisExpectedValue},
                {TimeUnit.MICROS, 3, baseTime, millisExpectedValue},
                {TimeUnit.NANOS, 3, baseTime, millisExpectedValue},
                {TimeUnit.MILLIS, 6, baseTime, millisExpectedValue},
                {TimeUnit.MICROS, 6, baseTime, microsExpectedValue},
                {TimeUnit.NANOS, 6, baseTime, microsExpectedValue},
                {TimeUnit.MILLIS, 9, baseTime, nanosTruncatedToMillisExpectedValue},
                {TimeUnit.MICROS, 9, baseTime, nanosTruncatedToMicrosExpectedValue},
                {TimeUnit.NANOS, 9, baseTime, nanosExpectedValue},
        };
    }

    private static long toEpochWithPrecision(LocalDateTime time, int precision)
    {
        long scaledEpochSeconds = time.toEpochSecond(ZoneOffset.UTC) * (long) Math.pow(10, precision);
        long fractionOfSecond = LongMath.divide(time.getNano(), (long) Math.pow(10, 9 - precision), RoundingMode.HALF_UP);
        return scaledEpochSeconds + fractionOfSecond;
    }

    private static BinaryStatistics timestampColumnStats(LocalDateTime minimum, LocalDateTime maximum)
    {
        return (BinaryStatistics) Statistics.getBuilderForReading(Types.optional(BINARY).named("TimestampColumn"))
                .withMin(toParquetEncoding(minimum))
                .withMax(toParquetEncoding(maximum))
                .withNumNulls(0)
                .build();
    }

    private static byte[] toParquetEncoding(LocalDateTime timestamp)
    {
        long startOfDay = timestamp.toLocalDate().atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        long timeOfDayNanos = (long) ((timestamp.atZone(ZoneOffset.UTC).toInstant().toEpochMilli() - startOfDay) * Math.pow(10, 6)) + timestamp.getNano() % NANOSECONDS_PER_MILLISECOND;

        Slice slice = Slices.allocate(12);
        slice.setLong(0, timeOfDayNanos);
        slice.setInt(8, millisToJulianDay(timestamp.atZone(ZoneOffset.UTC).toInstant().toEpochMilli()));
        return slice.byteArray();
    }

    private static int millisToJulianDay(long timestamp)
    {
        return toIntExact(MILLISECONDS.toDays(timestamp) + JULIAN_EPOCH_OFFSET_DAYS);
    }

    @Test
    public void testVarcharMatchesWithStatistics()
            throws ParquetCorruptionException
    {
        String value = "Test";
        ColumnDescriptor column = createColumnDescriptor(BINARY, "VarcharColumn");
        TupleDomain<ColumnDescriptor> effectivePredicate = getEffectivePredicate(column, createVarcharType(255), utf8Slice(value));
        TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(effectivePredicate, singletonList(column), UTC);
        PrimitiveType type = column.getPrimitiveType();
        Statistics<?> stats = Statistics.getBuilderForReading(type)
                .withMin(value.getBytes(UTF_8))
                .withMax(value.getBytes(UTF_8))
                .withNumNulls(1L)
                .build();
        assertThat(parquetPredicate.getIndexLookupCandidates(ImmutableMap.of(column, 2L), ImmutableMap.of(column, stats), ID))
                .isEqualTo(Optional.of(ImmutableList.of(column)));
    }

    @Test(dataProvider = "typeForParquetInt32")
    public void testIntegerMatchesWithStatistics(Type typeForParquetInt32)
            throws ParquetCorruptionException
    {
        ColumnDescriptor column = createColumnDescriptor(INT32, "Test column");
        TupleDomain<ColumnDescriptor> effectivePredicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                column,
                Domain.create(ValueSet.of(typeForParquetInt32, 42L, 43L, 44L, 112L), false)));
        TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(effectivePredicate, singletonList(column), UTC);

        assertThat(parquetPredicate.getIndexLookupCandidates(ImmutableMap.of(column, 2L), ImmutableMap.of(column, intColumnStats(32, 42)), ID))
                .isEqualTo(Optional.of(ImmutableList.of(column)));
        assertThat(parquetPredicate.getIndexLookupCandidates(ImmutableMap.of(column, 2L), ImmutableMap.of(column, intColumnStats(30, 40)), ID)).isEmpty();
        assertThat(parquetPredicate.getIndexLookupCandidates(ImmutableMap.of(column, 2L), ImmutableMap.of(column, intColumnStats(1024, 0x10000 + 42)), ID).isPresent())
                .isEqualTo(typeForParquetInt32 != INTEGER); // stats invalid for smallint/tinyint
    }

    @DataProvider
    public Object[][] typeForParquetInt32()
    {
        return new Object[][] {
                {INTEGER},
                {SMALLINT},
                {TINYINT},
        };
    }

    @Test
    public void testBigintMatchesWithStatistics()
            throws ParquetCorruptionException
    {
        ColumnDescriptor column = new ColumnDescriptor(new String[] {"path"}, Types.optional(INT64).named("Test column"), 0, 0);
        TupleDomain<ColumnDescriptor> effectivePredicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                column,
                Domain.create(ValueSet.of(BIGINT, 42L, 43L, 44L, 404L), false)));
        TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(effectivePredicate, singletonList(column), UTC);

        assertThat(parquetPredicate.getIndexLookupCandidates(ImmutableMap.of(column, 2L), ImmutableMap.of(column, longColumnStats(32, 42)), ID))
                .isEqualTo(Optional.of(ImmutableList.of(column)));
        assertThat(parquetPredicate.getIndexLookupCandidates(ImmutableMap.of(column, 2L), ImmutableMap.of(column, longColumnStats(30, 40)), ID)).isEmpty();
        assertThat(parquetPredicate.getIndexLookupCandidates(ImmutableMap.of(column, 2L), ImmutableMap.of(column, longColumnStats(1024, 0x10000 + 42)), ID)).isEmpty();
    }

    @Test
    public void testVarcharMatchesWithDictionaryDescriptor()
    {
        ColumnDescriptor column = new ColumnDescriptor(new String[] {"path"}, Types.optional(BINARY).named("Test column"), 0, 0);
        TupleDomain<ColumnDescriptor> effectivePredicate = getEffectivePredicate(column, createVarcharType(255), EMPTY_SLICE);
        TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(effectivePredicate, singletonList(column), UTC);
        DictionaryPage page = new DictionaryPage(Slices.wrappedBuffer(new byte[] {0, 0, 0, 0}), 1, PLAIN_DICTIONARY);
        assertTrue(parquetPredicate.matches(new DictionaryDescriptor(column, true, Optional.of(page))));
        assertTrue(parquetPredicate.matches(new DictionaryDescriptor(column, false, Optional.of(page))));

        effectivePredicate = withColumnDomains(ImmutableMap.of(
                column,
                singleValue(createVarcharType(255), Slices.utf8Slice("abc"), true)));
        parquetPredicate = new TupleDomainParquetPredicate(effectivePredicate, singletonList(column), UTC);
        assertTrue(parquetPredicate.matches(new DictionaryDescriptor(column, true, Optional.of(page))));
        assertFalse(parquetPredicate.matches(new DictionaryDescriptor(column, false, Optional.of(page))));
    }

    @Test
    public void testEmptyDictionary()
    {
        ColumnDescriptor column = new ColumnDescriptor(new String[] {"path"}, Types.optional(BINARY).named("Test column"), 0, 0);
        ColumnDescriptor descriptor = new ColumnDescriptor(column.getPath(), column.getPrimitiveType(), 0, 0);
        VarcharType type = createVarcharType(255);

        DictionaryPage dictionary = new DictionaryPage(EMPTY_SLICE, 0, PLAIN_DICTIONARY);
        TupleDomainParquetPredicate predicate;

        // only non-nulls allowed
        predicate = new TupleDomainParquetPredicate(
                withColumnDomains(singletonMap(descriptor, notNull(type))),
                singletonList(column),
                UTC);
        assertFalse(predicate.matches(new DictionaryDescriptor(column, true, Optional.of(dictionary))));
        assertFalse(predicate.matches(new DictionaryDescriptor(column, false, Optional.of(dictionary))));

        // only nulls allowed
        predicate = new TupleDomainParquetPredicate(
                withColumnDomains(singletonMap(descriptor, onlyNull(type))),
                singletonList(column),
                UTC);
        assertTrue(predicate.matches(new DictionaryDescriptor(column, true, Optional.of(dictionary))));
        assertFalse(predicate.matches(new DictionaryDescriptor(column, false, Optional.of(dictionary))));

        // mixed non-nulls and nulls allowed
        predicate = new TupleDomainParquetPredicate(
                withColumnDomains(singletonMap(descriptor, singleValue(type, EMPTY_SLICE, true))),
                singletonList(column),
                UTC);
        assertTrue(predicate.matches(new DictionaryDescriptor(column, true, Optional.of(dictionary))));
        assertFalse(predicate.matches(new DictionaryDescriptor(column, false, Optional.of(dictionary))));
    }

    @Test
    public void testIndexLookupCandidates()
            throws ParquetCorruptionException
    {
        ColumnDescriptor columnA = new ColumnDescriptor(new String[] {"pathA"}, Types.optional(INT64).named("Test column A"), 0, 0);
        ColumnDescriptor columnB = new ColumnDescriptor(new String[] {"pathB"}, Types.optional(INT64).named("Test column B"), 0, 0);
        TupleDomain<ColumnDescriptor> effectivePredicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                columnA,
                Domain.create(ValueSet.of(BIGINT, 42L, 43L, 44L, 404L), false),
                columnB,
                Domain.create(ValueSet.ofRanges(range(BIGINT, 42L, true, 404L, true)), false)));

        TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(effectivePredicate, singletonList(columnA), UTC);
        assertThat(parquetPredicate.getIndexLookupCandidates(
                ImmutableMap.of(columnA, 2L, columnB, 2L),
                ImmutableMap.of(columnA, longColumnStats(32, 42), columnB, longColumnStats(42, 500)), ID))
                .isEqualTo(Optional.of(ImmutableList.of(columnA)));

        parquetPredicate = new TupleDomainParquetPredicate(effectivePredicate, ImmutableList.of(columnA, columnB), UTC);
        // column stats missing on columnB
        assertThat(parquetPredicate.getIndexLookupCandidates(ImmutableMap.of(columnA, 2L), ImmutableMap.of(columnA, longColumnStats(32, 42)), ID))
                .isEqualTo(Optional.of(ImmutableList.of(columnA, columnB)));

        // All possible values for columnB are covered by effectivePredicate
        assertThat(parquetPredicate.getIndexLookupCandidates(
                ImmutableMap.of(columnA, 2L, columnB, 2L),
                ImmutableMap.of(columnA, longColumnStats(32, 42), columnB, longColumnStats(50, 400)), ID))
                .isEqualTo(Optional.of(ImmutableList.of(columnA)));

        assertThat(parquetPredicate.getIndexLookupCandidates(
                ImmutableMap.of(columnA, 2L, columnB, 2L),
                ImmutableMap.of(columnA, longColumnStats(32, 42), columnB, longColumnStats(42, 500)), ID))
                .isEqualTo(Optional.of(ImmutableList.of(columnA, columnB)));
    }

    @Test
    public void testColumnIndexWithNullPages()
            throws Exception
    {
        ColumnIndex columnIndex = ColumnIndexBuilder.build(
                Types.required(INT64).named("test_int64"),
                BoundaryOrder.UNORDERED,
                asList(true, false, true, false, true, false),
                asList(1L, 2L, 3L, 4L, 5L, 6L),
                toByteBufferList(null, 2L, null, 4L, null, 9L),
                toByteBufferList(null, 3L, null, 15L, null, 10L));
        ColumnDescriptor column = new ColumnDescriptor(new String[] {"path"}, Types.optional(INT64).named("Test column"), 0, 0);
        assertThat(getDomain(BIGINT, 200, columnIndex, new ParquetDataSourceId("test"), column, UTC))
                .isEqualTo(Domain.create(
                        ValueSet.ofRanges(
                                range(BIGINT, 2L, true, 3L, true),
                                range(BIGINT, 4L, true, 15L, true),
                                range(BIGINT, 9L, true, 10L, true)),
                        true));
    }

    @Test
    public void testColumnIndexWithNoNullsCount()
            throws Exception
    {
        ColumnIndex columnIndex = ColumnIndexBuilder.build(
                Types.required(INT64).named("test_int64"),
                BoundaryOrder.UNORDERED,
                asList(false, false, false),
                null,
                toByteBufferList(2L, 4L, 9L),
                toByteBufferList(3L, 15L, 10L));
        ColumnDescriptor column = new ColumnDescriptor(new String[] {"path"}, Types.optional(INT64).named("Test column"), 0, 0);
        assertThat(getDomain(BIGINT, 200, columnIndex, new ParquetDataSourceId("test"), column, UTC))
                .isEqualTo(Domain.create(
                        ValueSet.ofRanges(
                                range(BIGINT, 2L, true, 3L, true),
                                range(BIGINT, 4L, true, 15L, true),
                                range(BIGINT, 9L, true, 10L, true)),
                        true));
    }

    private ColumnDescriptor createColumnDescriptor(PrimitiveTypeName typeName, String columnName)
    {
        return new ColumnDescriptor(new String[]{}, new PrimitiveType(REQUIRED, typeName, columnName), 0, 0);
    }

    private TupleDomain<ColumnDescriptor> getEffectivePredicate(ColumnDescriptor column, VarcharType type, Slice value)
    {
        ColumnDescriptor predicateColumn = new ColumnDescriptor(column.getPath(), column.getPrimitiveType(), 0, 0);
        Domain predicateDomain = singleValue(type, value);
        Map<ColumnDescriptor, Domain> predicateColumns = singletonMap(predicateColumn, predicateDomain);
        return withColumnDomains(predicateColumns);
    }

    private static FloatStatistics floatColumnStats(float minimum, float maximum)
    {
        return floatColumnStats(minimum, maximum, false);
    }

    private static FloatStatistics floatColumnStats(float minimum, float maximum, boolean hasNulls)
    {
        return (FloatStatistics) Statistics.getBuilderForReading(Types.optional(FLOAT).named("FloatColumn"))
                .withMin(BytesUtils.longToBytes(Float.floatToRawIntBits(minimum)))
                .withMax(BytesUtils.longToBytes(Float.floatToRawIntBits(maximum)))
                .withNumNulls(hasNulls ? 1 : 0)
                .build();
    }

    private static DoubleStatistics doubleColumnStats(double minimum, double maximum)
    {
        return doubleColumnStats(minimum, maximum, false);
    }

    private static DoubleStatistics doubleColumnStats(double minimum, double maximum, boolean hasNulls)
    {
        return (DoubleStatistics) Statistics.getBuilderForReading(Types.optional(PrimitiveTypeName.DOUBLE).named("DoubleColumn"))
                .withMin(BytesUtils.longToBytes(Double.doubleToLongBits(minimum)))
                .withMax(BytesUtils.longToBytes(Double.doubleToLongBits(maximum)))
                .withNumNulls(hasNulls ? 1 : 0)
                .build();
    }

    private static IntStatistics intColumnStats(int minimum, int maximum)
    {
        return (IntStatistics) Statistics.getBuilderForReading(Types.optional(INT32).named("IntColumn"))
                .withMin(BytesUtils.intToBytes(minimum))
                .withMax(BytesUtils.intToBytes(maximum))
                .withNumNulls(0)
                .build();
    }

    private static LongStatistics longColumnStats(long minimum, long maximum)
    {
        return (LongStatistics) Statistics.getBuilderForReading(Types.optional(INT64).named("LongColumn"))
                .withMin(BytesUtils.longToBytes(minimum))
                .withMax(BytesUtils.longToBytes(maximum))
                .withNumNulls(0)
                .build();
    }

    private static BinaryStatistics binaryColumnStats(long minimum, long maximum)
    {
        return binaryColumnStats(BigInteger.valueOf(minimum), BigInteger.valueOf(maximum));
    }

    private static BinaryStatistics binaryColumnStats(BigInteger minimum, BigInteger maximum)
    {
        return (BinaryStatistics) Statistics.getBuilderForReading(Types.optional(BINARY).named("BinaryColumn"))
                .withMin(minimum.toByteArray())
                .withMax(maximum.toByteArray())
                .withNumNulls(0)
                .build();
    }

    private static LongStatistics longOnlyNullsStats(long numNulls)
    {
        return (LongStatistics) Statistics.getBuilderForReading(Types.optional(INT64).named("LongColumn"))
                .withNumNulls(numNulls)
                .build();
    }

    private DictionaryDescriptor floatDictionaryDescriptor(float... values)
            throws Exception
    {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(buf)) {
            for (float val : values) {
                out.writeFloat(val);
            }
        }
        return new DictionaryDescriptor(
                new ColumnDescriptor(new String[] {"dummy"}, new PrimitiveType(OPTIONAL, FLOAT, 0, "FloatColumn"), 1, 1),
                true,
                Optional.of(new DictionaryPage(Slices.wrappedBuffer(buf.toByteArray()), values.length, PLAIN_DICTIONARY)));
    }

    private DictionaryDescriptor doubleDictionaryDescriptor(double... values)
            throws Exception
    {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(buf)) {
            for (double val : values) {
                out.writeDouble(val);
            }
        }
        return new DictionaryDescriptor(
                new ColumnDescriptor(new String[] {"dummy"}, new PrimitiveType(OPTIONAL, PrimitiveTypeName.DOUBLE, 0, "DoubleColumn"), 1, 1),
                true,
                Optional.of(new DictionaryPage(Slices.wrappedBuffer(buf.toByteArray()), values.length, PLAIN_DICTIONARY)));
    }

    private static LongTimestamp longTimestamp(long precision, LocalDateTime start)
    {
        checkArgument(precision > MAX_SHORT_PRECISION && precision <= TimestampType.MAX_PRECISION, "Precision is out of range");
        return new LongTimestamp(
                start.atZone(ZoneOffset.UTC).toInstant().getEpochSecond() * MICROSECONDS_PER_SECOND + start.getLong(MICRO_OF_SECOND),
                toIntExact(round((start.getNano() % PICOSECONDS_PER_NANOSECOND) * (long) PICOSECONDS_PER_NANOSECOND, toIntExact(TimestampType.MAX_PRECISION - precision))));
    }

    private static List<ByteBuffer> toByteBufferList(Long... values)
    {
        List<ByteBuffer> buffers = new ArrayList<>(values.length);
        for (Long value : values) {
            if (value == null) {
                buffers.add(ByteBuffer.allocate(0));
            }
            else {
                buffers.add(ByteBuffer.wrap(BytesUtils.longToBytes(value)));
            }
        }
        return buffers;
    }
}
