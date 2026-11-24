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
package io.trino.plugin.iceberg.aggregation;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.AggregationMetrics;
import io.trino.operator.aggregation.Aggregator;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.iceberg.types.Conversions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.iceberg.IcebergTypes.convertTrinoValueToIceberg;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergTypeForNewColumn;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergThetaSketchForStats
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution(new IcebergPlugin());

    @Test
    void testBigint()
    {
        assertHashedAsIcebergValue(BIGINT, ImmutableList.of(0L, 1L, -1L, 42L, 1L << 40, Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Test
    void testInteger()
    {
        assertHashedAsIcebergValue(INTEGER, ImmutableList.of(0L, 1L, -1L, 42L, 1L << 20, (long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE));
    }

    @Test
    void testDate()
    {
        assertHashedAsIcebergValue(DATE, ImmutableList.of(0L, 1L, -1L, 20_000L, -700_000L, 2_900_000L));
    }

    @Test
    void testReal()
    {
        assertHashedAsIcebergValue(REAL, ImmutableList.of(
                (long) floatToRawIntBits(0.0f),
                (long) floatToRawIntBits(-0.0f),
                (long) floatToRawIntBits(1.5f),
                (long) floatToRawIntBits(-123.456f),
                (long) floatToRawIntBits(Float.NaN),
                (long) floatToRawIntBits(Float.NEGATIVE_INFINITY),
                (long) floatToRawIntBits(Float.MAX_VALUE)));
    }

    @Test
    void testDouble()
    {
        assertHashedAsIcebergValue(DOUBLE, ImmutableList.of(0.0, -0.0, 1.5, -123.456, Double.NaN, Double.POSITIVE_INFINITY, Double.MIN_VALUE));
    }

    @Test
    void testBoolean()
    {
        assertHashedAsIcebergValue(BOOLEAN, ImmutableList.of(true));
        assertHashedAsIcebergValue(BOOLEAN, ImmutableList.of(false));
        assertHashedAsIcebergValue(BOOLEAN, ImmutableList.of(true, false));
    }

    @Test
    void testVarchar()
    {
        assertHashedAsIcebergValue(VARCHAR, ImmutableList.of(
                Slices.utf8Slice(""),
                Slices.utf8Slice("a"),
                Slices.utf8Slice("some longer value, longer than sixteen bytes"),
                Slices.utf8Slice("zażółć gęślą jaźń"),
                Slices.utf8Slice("😀 中文")));
    }

    @Test
    void testBoundedVarchar()
    {
        assertHashedAsIcebergValue(createVarcharType(10), ImmutableList.of(
                Slices.utf8Slice(""),
                Slices.utf8Slice("a"),
                Slices.utf8Slice("zażółć")));
    }

    @Test
    void testVarbinary()
    {
        assertHashedAsIcebergValue(VARBINARY, ImmutableList.of(
                Slices.EMPTY_SLICE,
                Slices.wrappedBuffer((byte) 0),
                Slices.wrappedBuffer((byte) 0xFF, (byte) 0x00, (byte) 0x80),
                Slices.utf8Slice("some longer value, longer than sixteen bytes")));
    }

    @Test
    void testShortDecimal()
    {
        ImmutableList.Builder<Object> values = ImmutableList.builder();
        values.add(0L, 1L, -1L, 999_999_999_999_999_999L, -999_999_999_999_999_999L);
        // values around the boundaries where the minimal two's complement representation grows by a byte
        for (int bits = 7; bits < 63; bits += 8) {
            long boundary = 1L << bits;
            values.add(boundary - 1, boundary, boundary + 1, -boundary + 1, -boundary, -boundary - 1);
        }
        assertHashedAsIcebergValue(createDecimalType(18, 4), values.build());
        assertHashedAsIcebergValue(createDecimalType(5, 0), ImmutableList.of(0L, 127L, 128L, -128L, -129L, 99_999L, -99_999L));
    }

    @Test
    void testLongDecimal()
    {
        assertHashedAsIcebergValue(createDecimalType(38, 10), ImmutableList.of(
                Int128.ZERO,
                Int128.valueOf(1),
                Int128.valueOf(-1),
                Int128.valueOf(Long.MAX_VALUE),
                Int128.valueOf("12345678901234567890123456789012345678"),
                Int128.valueOf("-12345678901234567890123456789012345678")));
    }

    @Test
    void testTime()
    {
        assertHashedAsIcebergValue(TIME_MICROS, ImmutableList.of(
                0L,
                1L * PICOSECONDS_PER_MICROSECOND,
                12_345_678_901L * PICOSECONDS_PER_MICROSECOND,
                86_399_999_999L * PICOSECONDS_PER_MICROSECOND));
    }

    @Test
    void testTimestamp()
    {
        assertHashedAsIcebergValue(TIMESTAMP_MICROS, ImmutableList.of(0L, 1L, -1L, 1_700_000_000_123_456L, -62_135_596_800_000_000L));
    }

    @Test
    void testTimestampWithTimeZone()
    {
        assertHashedAsIcebergValue(TIMESTAMP_TZ_MICROS, ImmutableList.of(
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(0, 0, UTC_KEY),
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(1, 1_000_000, UTC_KEY),
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(-1, 999_000_000, UTC_KEY),
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(1_700_000_000_123L, 456_000_000, UTC_KEY)));
    }

    @Test
    void testTimestampNanos()
    {
        assertHashedAsIcebergValue(TIMESTAMP_NANOS, ImmutableList.of(
                new LongTimestamp(0, 0),
                new LongTimestamp(1, 1_000),
                new LongTimestamp(-1, 999_000),
                new LongTimestamp(1_700_000_000_123_456L, 789_000)));
    }

    @Test
    void testTimestampWithTimeZoneNanos()
    {
        assertHashedAsIcebergValue(TIMESTAMP_TZ_NANOS, ImmutableList.of(
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(0, 0, UTC_KEY),
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(1, 1_000, UTC_KEY),
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(-1, 999_999_000, UTC_KEY),
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(1_700_000_000_123L, 456_789_000, UTC_KEY)));
    }

    @Test
    void testUuid()
    {
        assertHashedAsIcebergValue(UUID, ImmutableList.of(
                javaUuidToTrinoUuid(java.util.UUID.fromString("00000000-0000-0000-0000-000000000000")),
                javaUuidToTrinoUuid(java.util.UUID.fromString("406caec7-68b9-4778-81b2-a12ece70c8b1"))));
    }

    private static void assertHashedAsIcebergValue(Type type, List<?> values)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, values.size() + 1);
        for (Object value : values) {
            writeNativeValue(type, blockBuilder, value);
        }
        // nulls are not counted
        blockBuilder.appendNull();
        ValueBlock block = blockBuilder.buildValueBlock();

        UpdatableThetaSketch expected = UpdatableThetaSketch.builder()
                .setFamily(Family.ALPHA)
                .build();
        org.apache.iceberg.types.Type icebergType = toIcebergTypeForNewColumn(type, new AtomicInteger(1));
        int expectedDistinctValues = 0;
        for (int position = 0; position < values.size(); position++) {
            Object icebergValue = convertTrinoValueToIceberg(type, readNativeValue(type, block, position));
            ByteBuffer serializedValue = Conversions.toByteBuffer(icebergType, icebergValue);
            // the sketch ignores empty values
            if (serializedValue.hasRemaining()) {
                expectedDistinctValues++;
            }
            expected.update(serializedValue);
        }
        assertThat(expected.getEstimate()).isEqualTo(expectedDistinctValues);

        // resolve the function like the engine does, as there is an input function per type
        Aggregator aggregator = FUNCTION_RESOLUTION.getAggregateFunction(IcebergThetaSketchForStats.NAME, fromTypes(type))
                .createAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.empty())
                .createAggregator(new AggregationMetrics());
        aggregator.processPage(new Page(block));
        BlockBuilder output = VARBINARY.createBlockBuilder(null, 1);
        aggregator.evaluate(output);

        assertThat(DataSketchStateSerializer.deserialize(output.build(), 0).toByteArray()).isEqualTo(expected.compact().toByteArray());
    }
}
