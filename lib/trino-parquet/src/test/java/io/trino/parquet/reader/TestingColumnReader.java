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
package io.trino.parquet.reader;

import com.google.common.collect.ImmutableMap;
import com.google.common.math.IntMath;
import com.google.common.math.LongMath;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetEncoding;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.Fixed12Block;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.parquet.ParquetEncoding.DELTA_BINARY_PACKED;
import static io.trino.parquet.ParquetEncoding.DELTA_BYTE_ARRAY;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.ParquetTestUtils.toTrinoDictionaryPage;
import static io.trino.parquet.ParquetTypeUtils.paddingBigInteger;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.TIME_NANOS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_PICOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.toDataProvider;
import static java.lang.Math.max;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.NANO_OF_DAY;
import static java.time.temporal.JulianFields.JULIAN_DAY;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.assertj.core.api.Assertions.assertThat;

public class TestingColumnReader
{
    private static final Map<Type, Class<? extends Block>> BLOCK_CLASSES = ImmutableMap.<Type, Class<? extends Block>>builder()
            .put(BooleanType.BOOLEAN, ByteArrayBlock.class)
            .put(BIGINT, LongArrayBlock.class)
            .put(INTEGER, IntArrayBlock.class)
            .put(SMALLINT, ShortArrayBlock.class)
            .put(TINYINT, ByteArrayBlock.class)
            .put(REAL, IntArrayBlock.class)
            .put(DoubleType.DOUBLE, LongArrayBlock.class)
            .put(VARCHAR, VariableWidthBlock.class)
            .put(createDecimalType(8, 0), LongArrayBlock.class)
            .put(createDecimalType(38, 2), Int128ArrayBlock.class)
            .put(TIME_MILLIS, LongArrayBlock.class)
            .put(TIMESTAMP_MILLIS, LongArrayBlock.class)
            .put(TIMESTAMP_TZ_MILLIS, LongArrayBlock.class)
            .put(TIMESTAMP_TZ_NANOS, Fixed12Block.class)
            .put(TIMESTAMP_PICOS, Fixed12Block.class)
            .put(UUID, Int128ArrayBlock.class)
            .buildOrThrow();

    private static final Writer<Number> WRITE_BOOLEAN = (writer, values) -> {
        Number[] result = new Number[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                boolean value = values[i] % 2 == 1;
                writer.writeBoolean(value);
                result[i] = (byte) (value ? 1 : 0);
            }
        }
        return result;
    };
    private static final Writer<Number> WRITE_BYTE = (writer, values) -> {
        Number[] result = new Number[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                writer.writeInteger(values[i]);
                result[i] = values[i].byteValue();
            }
        }
        return result;
    };
    private static final Writer<Number> WRITE_SHORT = (writer, values) -> {
        Number[] result = new Number[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                writer.writeInteger(values[i]);
                result[i] = values[i].shortValue();
            }
        }
        return result;
    };
    private static final Writer<Number> WRITE_INT = (writer, values) -> {
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                // 1001 work well for testing timestamp related types
                values[i] *= 1001;
                writer.writeInteger(values[i]);
            }
        }
        return values;
    };
    private static final Writer<Number> WRITE_LONG = (writer, values) -> {
        Number[] result = new Number[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                // 1001001 work well for testing timestamp related types
                long value = values[i].longValue();
                writer.writeLong(value);
                result[i] = value;
            }
        }
        return result;
    };
    public static final Writer<Number> WRITE_LONG_TIMESTAMP = (writer, values) -> {
        Number[] result = new Number[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                // 1001001 work well for testing timestamp related types
                long value = values[i].longValue() * 1001001;
                writer.writeLong(value);
                result[i] = value;
            }
        }
        return result;
    };
    private static final Writer<Float> WRITE_FLOAT = (writer, values) -> {
        Float[] result = new Float[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                writer.writeFloat(values[i]);
                result[i] = values[i].floatValue();
            }
        }
        return result;
    };
    private static final Writer<Double> WRITE_DOUBLE = (writer, values) -> {
        Double[] result = new Double[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                writer.writeDouble(values[i]);
                result[i] = values[i].doubleValue();
            }
        }
        return result;
    };
    private static final Writer<Number> WRITE_SHORT_DECIMAL = (writer, values) -> {
        for (Integer value : values) {
            if (value != null) {
                writer.writeBytes(Binary.fromConstantByteArray(Longs.toByteArray(value.longValue())));
            }
        }
        return values;
    };
    private static final Writer<String> WRITE_BINARY = (writer, values) -> {
        String[] result = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                String stringValue = Integer.toString(values[i]);
                writer.writeBytes(Binary.fromCharSequence(stringValue));
                result[i] = stringValue;
            }
        }
        return result;
    };
    private static final Writer<Number> WRITE_BINARY_DECIMAL = (writer, values) -> {
        Number[] result = new Number[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                writer.writeBytes(Binary.fromConstantByteArray(BigInteger.valueOf(values[i]).toByteArray()));
                result[i] = values[i];
            }
        }
        return result;
    };
    private static final Writer<Number> WRITE_BINARY_LONG_DECIMAL = (writer, values) -> {
        Number[] result = new Number[values.length * 2];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                writer.writeBytes(Binary.fromConstantByteArray(BigInteger.valueOf(values[i]).toByteArray()));
                result[2 * i] = 0;
                result[2 * i + 1] = values[i];
            }
        }
        return result;
    };
    private static final Writer<DecodedTimestamp> WRITE_INT96 = (writer, values) -> {
        DecodedTimestamp[] result = new DecodedTimestamp[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                // 1001001 work well for testing timestamp related types
                long seconds = values[i].longValue() * 1001001;
                int nanos = (int) (values[i].longValue() * 1111);
                writer.writeBytes(encodeInt96Timestamp(seconds, nanos));
                result[i] = new DecodedTimestamp(seconds, nanos);
            }
        }
        return result;
    };
    private static final Writer<Number> WRITE_UUID = (writer, values) -> {
        Number[] result = new Long[values.length * 2];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                byte[] uuid = new byte[16];
                byte value = values[i].byteValue();
                for (int j = 0; j < 16; j++) {
                    uuid[j] = (byte) (value + j);
                }

                result[2 * i] = Long.reverseBytes(Longs.fromByteArray(Arrays.copyOfRange(uuid, 0, 8)));
                result[2 * i + 1] = Long.reverseBytes(Longs.fromByteArray(Arrays.copyOfRange(uuid, 8, 16)));
                writer.writeBytes(Binary.fromConstantByteArray(uuid));
            }
        }
        return result;
    };

    private static Writer<Number> writeLongDecimal(int typeLength)
    {
        return (writer, values) -> {
            Number[] result = new Number[values.length * 2];
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    writer.writeBytes(Binary.fromConstantByteArray(paddingBigInteger(BigInteger.valueOf(values[i]), typeLength)));
                    Int128 longDecimal = Int128.valueOf(values[i]);
                    result[2 * i] = longDecimal.getHigh();
                    result[2 * i + 1] = longDecimal.getLow();
                }
            }
            return result;
        };
    }

    private static Writer<String> writeFixedWidthBinary(int typeLength)
    {
        return (writer, values) -> {
            String[] result = new String[values.length];
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    byte[] bytes = Arrays.copyOf(Integer.toString(values[i]).getBytes(UTF_8), typeLength);
                    writer.writeBytes(Binary.fromConstantByteArray(bytes));
                    result[i] = new String(bytes, UTF_8);
                }
            }
            return result;
        };
    }

    private static final Assertion<Number> ASSERT_BOOLEAN = (values, block, offset, blockOffset) -> assertThat(BooleanType.BOOLEAN.getBoolean(block, blockOffset)).isEqualTo(values[offset].byteValue() != 0);
    private static final Assertion<Number> ASSERT_BYTE = (values, block, offset, blockOffset) -> assertThat(TINYINT.getByte(block, blockOffset)).isEqualTo(values[offset].byteValue());
    private static final Assertion<Number> ASSERT_SHORT = (values, block, offset, blockOffset) -> assertThat(SMALLINT.getShort(block, blockOffset)).isEqualTo(values[offset].shortValue());
    private static final Assertion<Number> ASSERT_INT = (values, block, offset, blockOffset) -> assertThat(INTEGER.getInt(block, blockOffset)).isEqualTo(values[offset].intValue());
    private static final Assertion<Float> ASSERT_FLOAT = (values, block, offset, blockOffset) -> assertThat(REAL.getFloat(block, blockOffset)).isEqualTo(values[offset].floatValue());
    private static final Assertion<Number> ASSERT_LONG = (values, block, offset, blockOffset) -> assertThat(BIGINT.getLong(block, blockOffset)).isEqualTo(values[offset].longValue());
    private static final Assertion<Double> ASSERT_DOUBLE = (values, block, offset, blockOffset) -> assertThat(DoubleType.DOUBLE.getDouble(block, blockOffset)).isEqualTo(values[offset].doubleValue());
    private static final Assertion<Float> ASSERT_DOUBLE_STORED_AS_FLOAT = (values, block, offset, blockOffset) ->
            assertThat(DoubleType.DOUBLE.getDouble(block, blockOffset)).isEqualTo(values[offset].floatValue());

    private record Int128Assertion(Type type)
            implements Assertion<Number>
    {
        private Int128Assertion
        {
            checkArgument(type.getJavaType().equals(Int128.class));
        }

        @Override
        public void assertPosition(Number[] expected, Block block, int index, int blockIndex)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void assertBlock(Number[] expected, Block block, int offset, int blockOffset, int length)
        {
            for (int i = 0; i < length; i++) {
                if (block.isNull(blockOffset + i)) {
                    assertThat(expected[2 * (offset + i)]).isNull();
                    assertThat(expected[2 * (offset + i) + 1]).isNull();
                }
                else {
                    Int128 value = (Int128) type.getObject(block, blockOffset + i);
                    assertThat(value.getHigh()).isEqualTo(expected[2 * (offset + i)].longValue());
                    assertThat(value.getLow()).isEqualTo(expected[2 * (offset + i) + 1].longValue());
                }
            }
        }

        @Override
        public void assertBlock(Number[] expected, Block block)
        {
            assertThat(expected.length).isEqualTo(block.getPositionCount() * 2);
            assertBlock(expected, block, 0, 0, block.getPositionCount());
        }
    }

    private static Assertion<Number> assertShortDecimal(DecimalType decimalType)
    {
        long multiplier = longTenToNth(decimalType.getScale());
        return (expected, block, index, blockIndex) -> assertThat(decimalType.getLong(block, blockIndex)).isEqualTo(expected[index].longValue() * multiplier);
    }

    private static Assertion<Number> assertLongDecimal(DecimalType decimalType)
    {
        long multiplier = longTenToNth(decimalType.getScale());
        return (expected, block, index, blockIndex) -> assertThat(decimalType.getObject(block, blockIndex)).isEqualTo(Int128.valueOf(0, expected[index].longValue() * multiplier));
    }

    private static Assertion<Number> assertLongToShortRescaled(DecimalType decimalType)
    {
        long multiplier = longTenToNth(decimalType.getScale());
        return new Assertion<>()
        {
            @Override
            public void assertBlock(Number[] expected, Block block, int offset, int blockOffset, int length)
            {
                for (int i = 0; i < length; i++) {
                    if (block.isNull(blockOffset + i)) {
                        assertThat(expected[2 * (offset + i)]).isNull();
                        assertThat(expected[2 * (offset + i) + 1]).isNull();
                    }
                    else {
                        assertPosition(expected, block, offset + i, blockOffset + i);
                    }
                }
            }

            @Override
            public void assertPosition(Number[] expected, Block block, int index, int blockIndex)
            {
                assertThat(decimalType.getLong(block, blockIndex)).isEqualTo(expected[2 * index + 1].longValue() * multiplier);
            }

            @Override
            public void assertBlock(Number[] expected, Block block)
            {
                assertThat(expected.length).isEqualTo(block.getPositionCount() * 2);
                assertBlock(expected, block, 0, 0, block.getPositionCount());
            }
        };
    }

    private static Assertion<Number> assertUuid()
    {
        return new Assertion<>()
        {
            @Override
            public void assertBlock(Number[] expected, Block block, int offset, int blockOffset, int length)
            {
                for (int i = 0; i < length; i++) {
                    if (block.isNull(blockOffset + i)) {
                        assertThat(expected[2 * (offset + i)]).isNull();
                        assertThat(expected[2 * (offset + i) + 1]).isNull();
                    }
                    else {
                        assertPosition(expected, block, offset + i, blockOffset + i);
                    }
                }
            }

            @Override
            public void assertPosition(Number[] expected, Block block, int index, int blockIndex)
            {
                Slice uuid = UUID.getSlice(block, blockIndex);
                assertThat(uuid.getLong(0)).isEqualTo(expected[2 * index].longValue());
                assertThat(uuid.getLong(8)).isEqualTo(expected[2 * index + 1].longValue());
            }

            @Override
            public void assertBlock(Number[] expected, Block block)
            {
                assertThat(expected.length).isEqualTo(block.getPositionCount() * 2);
                assertBlock(expected, block, 0, 0, block.getPositionCount());
            }
        };
    }

    private static Assertion<Number> assertLongRescaled(DecimalType decimalType)
    {
        long multiplier = longTenToNth(decimalType.getScale());
        return new Assertion<>()
        {
            @Override
            public void assertBlock(Number[] expected, Block block, int offset, int blockOffset, int length)
            {
                for (int i = 0; i < length; i++) {
                    if (block.isNull(blockOffset + i)) {
                        assertThat(expected[2 * (offset + i)]).isNull();
                        assertThat(expected[2 * (offset + i) + 1]).isNull();
                    }
                    else {
                        assertPosition(expected, block, offset + i, blockOffset + i);
                    }
                }
            }

            @Override
            public void assertPosition(Number[] expected, Block block, int index, int blockIndex)
            {
                assertThat(decimalType.getObject(block, blockIndex)).isEqualTo(Int128.valueOf(0, expected[2 * index + 1].longValue() * multiplier));
            }

            @Override
            public void assertBlock(Number[] expected, Block block)
            {
                assertThat(expected.length).isEqualTo(block.getPositionCount() * 2);
                assertBlock(expected, block, 0, 0, block.getPositionCount());
            }
        };
    }

    private static final Assertion<String> ASSERT_BINARY = (values, block, offset, blockOffset) -> {
        assertThat(VARBINARY.getSlice(block, blockOffset).getBytes()).isEqualTo(values[offset].getBytes(UTF_8));
    };

    private static Assertion<Number> assertTime(TimeType timeType, int precision)
    {
        return assertTime(timeType, precision, -precision);
    }

    private static Assertion<Number> assertTime(TimeType timeType, int precision, int rounding)
    {
        long multiplier = LongMath.pow(10, max(0, precision));
        return (values, block, offset, blockOffset) -> {
            long value = values[offset].longValue();
            if (rounding > 0 | precision < 0) {
                value = Timestamps.round(value, rounding) / LongMath.pow(10, -precision);
            }
            assertThat(timeType.getLong(block, blockOffset)).isEqualTo(value * multiplier);
        };
    }

    private static Assertion<DecodedTimestamp> assertTimestampMicros(int rounding)
    {
        return (values, block, offset, blockOffset) -> {
            long epochSeconds = values[offset].epochSeconds();
            int nanos = values[offset].nanosOfSecond();
            long epochNanos = epochSeconds * Timestamps.NANOSECONDS_PER_SECOND + nanos;
            long expectedMicros = Timestamps.round(epochNanos, rounding) / 1000;

            assertThat(TIMESTAMP_MICROS.getLong(block, blockOffset)).isEqualTo(expectedMicros);
        };
    }

    private static Assertion<DecodedTimestamp> assertTimestampNanos()
    {
        return (values, block, offset, blockOffset) -> {
            long epochSeconds = values[offset].epochSeconds();
            int nanos = values[offset].nanosOfSecond();
            long epochNanos = epochSeconds * Timestamps.NANOSECONDS_PER_SECOND + nanos;

            LongTimestamp timestamp = (LongTimestamp) TIMESTAMP_NANOS.getObject(block, blockOffset);
            long actualEpochMicros = timestamp.getEpochMicros();
            long actualNanos = timestamp.getPicosOfMicro() / 1000;
            long actualEpochNanos = actualEpochMicros * 1000 + actualNanos;
            assertThat(actualEpochNanos).isEqualTo(epochNanos);
        };
    }

    private static Assertion<DecodedTimestamp> assertTimestampWithTimeZoneMillis()
    {
        return (values, block, offset, blockOffset) -> {
            long epochSeconds = values[offset].epochSeconds();
            int nanos = values[offset].nanosOfSecond();
            long epochNanos = epochSeconds * Timestamps.NANOSECONDS_PER_SECOND + nanos;
            long expectedMillis = Timestamps.round(epochNanos, 6) / 1000000;

            long packed = TIMESTAMP_TZ_MILLIS.getLong(block, blockOffset);
            long blockMillis = unpackMillisUtc(packed);
            TimeZoneKey timeZoneKey = unpackZoneKey(packed);
            assertThat(timeZoneKey).isEqualTo(UTC_KEY);

            assertThat(blockMillis).isEqualTo(expectedMillis);
        };
    }

    public static Assertion<Number> assertTimestampNanos(int precision)
    {
        int multiplier = IntMath.pow(10, precision);
        return (values, block, offset, blockOffset) ->
        {
            LongTimestamp value = (LongTimestamp) TIMESTAMP_NANOS.getObject(block, blockOffset);
            // We ignore higher bits of long timestamp as we only test on small numbers
            long longValue = (value.getEpochMicros() * 1_000_000) + value.getPicosOfMicro();
            assertThat(longValue).isEqualTo(values[offset].longValue() * multiplier);
        };
    }

    private static Assertion<Number> assertTimestampWithTimeZoneMillis(int precision)
    {
        return assertTimestampWithTimeZoneMillis(precision, -precision);
    }

    private static Assertion<Number> assertTimestampWithTimeZoneMillis(int precision, int rounding)
    {
        long multiplier = LongMath.pow(10, max(0, precision));
        return (values, block, offset, blockOffset) ->
        {
            long packed = TIMESTAMP_TZ_MILLIS.getLong(block, blockOffset);
            long millisUtc = unpackMillisUtc(packed);
            TimeZoneKey timeZoneKey = unpackZoneKey(packed);
            long value = values[offset].longValue();
            if (rounding > 0 | precision < 0) {
                value = Timestamps.round(value, rounding) / LongMath.pow(10, -precision);
            }
            assertThat(timeZoneKey).isEqualTo(UTC_KEY);
            assertThat(millisUtc).isEqualTo(value * multiplier);
        };
    }

    private static Assertion<Number> assertLongTimestampWithTimeZoneNanos(int precision)
    {
        long multiplier = LongMath.pow(10, precision);
        return (values, block, offset, blockOffset) ->
        {
            LongTimestampWithTimeZone packed = (LongTimestampWithTimeZone) TIMESTAMP_TZ_NANOS.getObject(block, blockOffset);
            long longValue = (packed.getEpochMillis() * 1_000_000_000) + packed.getPicosOfMilli();
            TimeZoneKey timeZoneKey = getTimeZoneKey(packed.getTimeZoneKey());
            long value = values[offset].longValue();
            assertThat(timeZoneKey).isEqualTo(UTC_KEY);
            assertThat(longValue).isEqualTo(value * multiplier);
        };
    }

    private static Assertion<DecodedTimestamp> assertInt96LongTimestampWithTimeZone(int precision)
    {
        TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(precision);
        return (values, block, offset, blockOffset) ->
        {
            long epochSeconds = values[offset].epochSeconds();
            int nanos = values[offset].nanosOfSecond();
            long epochNanos = epochSeconds * Timestamps.NANOSECONDS_PER_SECOND + nanos;
            if (precision < 9) {
                epochNanos = round(epochNanos, 9 - precision);
            }

            LongTimestampWithTimeZone packed = (LongTimestampWithTimeZone) type.getObject(block, blockOffset);
            long picos = (packed.getEpochMillis() * PICOSECONDS_PER_MILLISECOND) + packed.getPicosOfMilli();
            TimeZoneKey timeZoneKey = getTimeZoneKey(packed.getTimeZoneKey());
            assertThat(timeZoneKey).isEqualTo(UTC_KEY);
            assertThat(picos).isEqualTo(epochNanos * PICOSECONDS_PER_NANOSECOND);
        };
    }

    private TestingColumnReader() {}

    public enum DataPageVersion
    {
        V1, V2
    }

    public static DictionaryPage getDictionaryPage(DictionaryValuesWriter dictionaryWriter)
    {
        org.apache.parquet.column.page.DictionaryPage apacheDictionaryPage = dictionaryWriter.toDictPageAndClose();
        return toTrinoDictionaryPage(apacheDictionaryPage);
    }

    public static Object[][] readersWithPageVersions()
    {
        return cartesianProduct(
                Stream.of(DataPageVersion.V1, DataPageVersion.V2).collect(toDataProvider()),
                Stream.of(TestingColumnReader.columnReaders()).collect(toDataProvider()));
    }

    public static Object[][] dictionaryReadersWithPageVersions()
    {
        return cartesianProduct(
                Stream.of(DataPageVersion.V1, DataPageVersion.V2).collect(toDataProvider()),
                Arrays.stream(TestingColumnReader.columnReaders())
                        .filter(ColumnReaderFormat::hasDictionarySupport)
                        .collect(toDataProvider()));
    }

    public static Binary encodeInt96Timestamp(long epochSeconds, int nanos)
    {
        LocalDateTime javaTime = LocalDateTime.ofEpochSecond(epochSeconds, nanos, UTC);
        Slice slice = Slices.allocate(12);
        slice.setLong(0, NANO_OF_DAY.getFrom(javaTime));
        slice.setInt(8, (int) JULIAN_DAY.getFrom(javaTime));

        return Binary.fromConstantByteArray(slice.getBytes());
    }

    /**
     * These types should correspond to the column readers in {@link ColumnReaderFactory} class.
     * This method is a de-facto definition of all supported types and coercions of the optimized Parquet Reader.
     * All supported types should be added here.
     */
    private static ColumnReaderFormat<?>[] columnReaders()
    {
        return new ColumnReaderFormat[] {
                new ColumnReaderFormat<>(BOOLEAN, BooleanType.BOOLEAN, PLAIN, RLE, WRITE_BOOLEAN, ASSERT_BOOLEAN),
                new ColumnReaderFormat<>(FLOAT, REAL, PLAIN, PLAIN, WRITE_FLOAT, ASSERT_FLOAT),
                // FLOAT parquet primitive type can be read as a DOUBLE or REAL type in Trino
                new ColumnReaderFormat<>(FLOAT, DoubleType.DOUBLE, PLAIN, PLAIN, WRITE_FLOAT, ASSERT_DOUBLE_STORED_AS_FLOAT),
                new ColumnReaderFormat<>(DOUBLE, DoubleType.DOUBLE, PLAIN, PLAIN, WRITE_DOUBLE, ASSERT_DOUBLE),
                new ColumnReaderFormat<>(INT32, decimalType(0, 8), createDecimalType(8), PLAIN, DELTA_BINARY_PACKED, WRITE_INT, ASSERT_LONG),
                // INT32 can be read as a ShortDecimalType in Trino without decimal logical type annotation as well
                new ColumnReaderFormat<>(INT32, createDecimalType(8, 0), PLAIN, DELTA_BINARY_PACKED, WRITE_INT, ASSERT_LONG),
                new ColumnReaderFormat<>(INT32, createDecimalType(8, 2), PLAIN, DELTA_BINARY_PACKED, WRITE_INT, assertShortDecimal(createDecimalType(8, 2))),
                new ColumnReaderFormat<>(INT32, BIGINT, PLAIN, DELTA_BINARY_PACKED, WRITE_INT, ASSERT_LONG),
                new ColumnReaderFormat<>(INT32, INTEGER, PLAIN, DELTA_BINARY_PACKED, WRITE_INT, ASSERT_INT),
                new ColumnReaderFormat<>(INT32, SMALLINT, PLAIN, DELTA_BINARY_PACKED, WRITE_SHORT, ASSERT_SHORT),
                new ColumnReaderFormat<>(INT32, TINYINT, PLAIN, DELTA_BINARY_PACKED, WRITE_BYTE, ASSERT_BYTE),
                new ColumnReaderFormat<>(BINARY, VARCHAR, PLAIN, DELTA_BYTE_ARRAY, WRITE_BINARY, ASSERT_BINARY),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, null, VARCHAR, PLAIN, DELTA_BYTE_ARRAY, writeFixedWidthBinary(8), ASSERT_BINARY),
                new ColumnReaderFormat<>(INT64, decimalType(0, 16), createDecimalType(16), PLAIN, DELTA_BINARY_PACKED, WRITE_LONG, ASSERT_LONG),
                new ColumnReaderFormat<>(INT64, BIGINT, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG, ASSERT_LONG),
                new ColumnReaderFormat<>(INT64, INTEGER, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG, ASSERT_INT),
                new ColumnReaderFormat<>(INT64, SMALLINT, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG, ASSERT_SHORT),
                new ColumnReaderFormat<>(INT64, TINYINT, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG, ASSERT_BYTE),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 2), createDecimalType(2), PLAIN, DELTA_BYTE_ARRAY, WRITE_SHORT_DECIMAL, ASSERT_LONG),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 16, decimalType(2, 38), createDecimalType(38, 2), PLAIN, DELTA_BYTE_ARRAY, writeLongDecimal(16), new Int128Assertion(createDecimalType(38, 2))),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 16, uuidType(), UUID, PLAIN, DELTA_BYTE_ARRAY, WRITE_UUID, assertUuid()),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 16, null, UUID, PLAIN, DELTA_BYTE_ARRAY, WRITE_UUID, assertUuid()),
                // Trino type precision is irrelevant since the data is always stored as picoseconds
                new ColumnReaderFormat<>(INT32, timeType(false, MILLIS), TIME_MILLIS, PLAIN, DELTA_BINARY_PACKED, WRITE_INT, assertTime(TIME_MICROS, 9)),
                new ColumnReaderFormat<>(INT64, timeType(false, MICROS), TIME_MICROS, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG, assertTime(TIME_MICROS, 6)),
                // Reading a column TimeLogicalTypeAnnotation as a BIGINT
                new ColumnReaderFormat<>(INT64, timeType(false, MICROS), BIGINT, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG, ASSERT_LONG),
                // Short decimals
                new ColumnReaderFormat<>(INT32, decimalType(0, 8), createDecimalType(8), PLAIN, DELTA_BINARY_PACKED, WRITE_INT, ASSERT_LONG),
                // INT32 values can be read as zero scale decimals provided the precision is at least 10 to accommodate the largest possible integer
                new ColumnReaderFormat<>(INT32, createDecimalType(10), PLAIN, DELTA_BINARY_PACKED, WRITE_INT, ASSERT_LONG),
                new ColumnReaderFormat<>(INT32, decimalType(0, 8), BIGINT, PLAIN, DELTA_BINARY_PACKED, WRITE_INT, ASSERT_LONG),
                new ColumnReaderFormat<>(INT32, decimalType(0, 8), INTEGER, PLAIN, DELTA_BINARY_PACKED, WRITE_INT, ASSERT_INT),
                new ColumnReaderFormat<>(INT32, decimalType(0, 8), SMALLINT, PLAIN, DELTA_BINARY_PACKED, WRITE_SHORT, ASSERT_SHORT),
                new ColumnReaderFormat<>(INT32, decimalType(0, 8), TINYINT, PLAIN, DELTA_BINARY_PACKED, WRITE_BYTE, ASSERT_BYTE),
                new ColumnReaderFormat<>(INT64, decimalType(0, 8), createDecimalType(8), PLAIN, DELTA_BINARY_PACKED, WRITE_LONG, ASSERT_LONG),
                new ColumnReaderFormat<>(INT64, decimalType(0, 8), BIGINT, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG, ASSERT_LONG),
                new ColumnReaderFormat<>(INT64, decimalType(0, 8), INTEGER, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG, ASSERT_INT),
                new ColumnReaderFormat<>(INT64, decimalType(0, 8), SMALLINT, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG, ASSERT_SHORT),
                new ColumnReaderFormat<>(INT64, decimalType(0, 8), TINYINT, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG, ASSERT_BYTE),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 2), createDecimalType(2), PLAIN, DELTA_BYTE_ARRAY, WRITE_SHORT_DECIMAL, ASSERT_LONG),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 2), BIGINT, PLAIN, DELTA_BYTE_ARRAY, WRITE_SHORT_DECIMAL, ASSERT_LONG),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 2), INTEGER, PLAIN, DELTA_BYTE_ARRAY, WRITE_SHORT_DECIMAL, ASSERT_INT),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 2), SMALLINT, PLAIN, DELTA_BYTE_ARRAY, WRITE_SHORT_DECIMAL, ASSERT_SHORT),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 2), TINYINT, PLAIN, DELTA_BYTE_ARRAY, WRITE_SHORT_DECIMAL, ASSERT_BYTE),
                new ColumnReaderFormat<>(BINARY, decimalType(0, 8), createDecimalType(8), PLAIN, DELTA_BYTE_ARRAY, WRITE_BINARY_DECIMAL, ASSERT_LONG),
                new ColumnReaderFormat<>(BINARY, decimalType(0, 8), BIGINT, PLAIN, DELTA_BYTE_ARRAY, WRITE_BINARY_DECIMAL, ASSERT_LONG),
                new ColumnReaderFormat<>(BINARY, decimalType(0, 8), INTEGER, PLAIN, DELTA_BYTE_ARRAY, WRITE_BINARY_DECIMAL, ASSERT_INT),
                new ColumnReaderFormat<>(BINARY, decimalType(0, 8), SMALLINT, PLAIN, DELTA_BYTE_ARRAY, WRITE_BINARY_DECIMAL, ASSERT_SHORT),
                new ColumnReaderFormat<>(BINARY, decimalType(0, 8), TINYINT, PLAIN, DELTA_BYTE_ARRAY, WRITE_BINARY_DECIMAL, ASSERT_BYTE),
                // Long decimals
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 16, decimalType(2, 38), createDecimalType(38, 2), PLAIN, DELTA_BYTE_ARRAY, writeLongDecimal(16), new Int128Assertion(createDecimalType(38, 2))),
                new ColumnReaderFormat<>(BINARY, 16, decimalType(2, 38), createDecimalType(38, 2), PLAIN, DELTA_BYTE_ARRAY, WRITE_BINARY_LONG_DECIMAL, new Int128Assertion(createDecimalType(38, 2))),
                // Rescaled decimals
                new ColumnReaderFormat<>(INT32, decimalType(0, 7), createDecimalType(8, 1), PLAIN, DELTA_BINARY_PACKED, WRITE_INT, assertShortDecimal(createDecimalType(8, 1))),
                new ColumnReaderFormat<>(INT64, decimalType(0, 7), createDecimalType(8, 2), PLAIN, DELTA_BINARY_PACKED, WRITE_LONG, assertShortDecimal(createDecimalType(8, 2))),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 7), createDecimalType(8, 3), PLAIN, DELTA_BYTE_ARRAY, WRITE_SHORT_DECIMAL, assertShortDecimal(createDecimalType(8, 3))),
                new ColumnReaderFormat<>(INT32, decimalType(0, 7), createDecimalType(30, 1), PLAIN, DELTA_BINARY_PACKED, WRITE_INT, assertLongDecimal(createDecimalType(30, 1))),
                new ColumnReaderFormat<>(INT64, decimalType(0, 7), createDecimalType(30, 2), PLAIN, DELTA_BINARY_PACKED, WRITE_LONG, assertLongDecimal(createDecimalType(30, 2))),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 7), createDecimalType(30, 3), PLAIN, DELTA_BYTE_ARRAY, WRITE_SHORT_DECIMAL, assertLongDecimal(createDecimalType(30, 3))),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 16, decimalType(0, 38), createDecimalType(8, 1), PLAIN, DELTA_BYTE_ARRAY, writeLongDecimal(16), assertLongToShortRescaled(createDecimalType(8, 1))),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 16, decimalType(0, 38), createDecimalType(37, 2), PLAIN, DELTA_BYTE_ARRAY, writeLongDecimal(16), assertLongRescaled(createDecimalType(37, 2))),
                // Timestamps.
                //  The `precision` and `rounding` arguments at the end of every assertion may be difficult to understand. They are a direct
                // consequence of various Trino timestamp representations.
                new ColumnReaderFormat<>(INT64, timestampType(false, MILLIS), TIMESTAMP_MICROS, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, assertTime(TIME_MILLIS, 3)),
                new ColumnReaderFormat<>(INT64, timestampType(false, MILLIS), TIMESTAMP_PICOS, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, assertTimestampNanos(9)),
                new ColumnReaderFormat<>(INT64, timestampType(false, MILLIS), TIMESTAMP_TZ_MILLIS, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, assertTimestampWithTimeZoneMillis(0)),
                new ColumnReaderFormat<>(INT64, timestampType(false, MICROS), TIMESTAMP_MILLIS, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, assertTime(TIME_MICROS, 0, 3)),
                new ColumnReaderFormat<>(INT64, timestampType(false, MICROS), TIMESTAMP_MICROS, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, assertTime(TIME_MICROS, 0)),
                new ColumnReaderFormat<>(INT64, timestampType(false, MICROS), TIMESTAMP_NANOS, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, assertTimestampNanos(6)),
                new ColumnReaderFormat<>(INT64, timestampType(false, MICROS), TIMESTAMP_TZ_MILLIS, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, assertTimestampWithTimeZoneMillis(-3, 3)),
                new ColumnReaderFormat<>(INT64, timestampType(false, MICROS), TIMESTAMP_TZ_NANOS, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, assertLongTimestampWithTimeZoneNanos(6)),
                new ColumnReaderFormat<>(INT64, timestampType(false, NANOS), TIMESTAMP_MILLIS, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, assertTime(TIME_NANOS, -3, 6)),
                new ColumnReaderFormat<>(INT64, timestampType(false, NANOS), TIMESTAMP_MICROS, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, assertTime(TIME_NANOS, -3)),
                new ColumnReaderFormat<>(INT64, timestampType(false, NANOS), TIMESTAMP_NANOS, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, assertTimestampNanos(3)),
                new ColumnReaderFormat<>(INT64, timestampType(false, NANOS), TIMESTAMP_TZ_MILLIS, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, assertTimestampWithTimeZoneMillis(-6, 3)),
                new ColumnReaderFormat<>(INT64, timestampType(false, NANOS), TIMESTAMP_TZ_NANOS, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, assertLongTimestampWithTimeZoneNanos(3)),
                new ColumnReaderFormat<>(INT96, 12, null, TIMESTAMP_MILLIS, PLAIN, PLAIN, WRITE_INT96, assertTimestampMicros(6)),
                new ColumnReaderFormat<>(INT96, 12, null, TIMESTAMP_MICROS, PLAIN, PLAIN, WRITE_INT96, assertTimestampMicros(3)),
                new ColumnReaderFormat<>(INT96, 12, null, TIMESTAMP_NANOS, PLAIN, PLAIN, WRITE_INT96, assertTimestampNanos()),
                new ColumnReaderFormat<>(INT96, 12, null, TIMESTAMP_PICOS, PLAIN, PLAIN, WRITE_INT96, assertTimestampNanos()),
                new ColumnReaderFormat<>(INT96, 12, null, TIMESTAMP_TZ_MILLIS, PLAIN, PLAIN, WRITE_INT96, assertTimestampWithTimeZoneMillis()),
                new ColumnReaderFormat<>(INT96, 12, null, TIMESTAMP_TZ_MICROS, PLAIN, PLAIN, WRITE_INT96, assertInt96LongTimestampWithTimeZone(6)),
                new ColumnReaderFormat<>(INT96, 12, null, TIMESTAMP_TZ_NANOS, PLAIN, PLAIN, WRITE_INT96, assertInt96LongTimestampWithTimeZone(9)),
                new ColumnReaderFormat<>(INT96, 12, null, TIMESTAMP_TZ_PICOS, PLAIN, PLAIN, WRITE_INT96, assertInt96LongTimestampWithTimeZone(12)),
                // timestamps read as bigint
                new ColumnReaderFormat<>(INT64, timestampType(false, MILLIS), BIGINT, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, ASSERT_LONG),
                new ColumnReaderFormat<>(INT64, timestampType(false, MICROS), BIGINT, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, ASSERT_LONG),
                new ColumnReaderFormat<>(INT64, timestampType(false, NANOS), BIGINT, PLAIN, DELTA_BINARY_PACKED, WRITE_LONG_TIMESTAMP, ASSERT_LONG),
        };
    }

    // Simple helper interface that writes given data into the parquet writer
    // and returns buffer that can be used for asserting the results
    // The values are always int, and they should be somehow "mapped" to the resulting
    // type e.g. 1 -> "1" for varchar type
    private interface Writer<BUFFER>
    {
        BUFFER[] write(ValuesWriter parquetWriter, Integer[] values);

        default BUFFER[] resetAndWrite(ValuesWriter parquetWriter, Integer[] values)
        {
            parquetWriter.reset();
            return write(parquetWriter, values);
        }
    }

    private interface Assertion<BUFFER>
    {
        void assertPosition(BUFFER[] expected, Block block, int index, int blockIndex);

        default void assertBlock(BUFFER[] expected, Block block, int offset, int blockOffset, int length)
        {
            for (int i = 0; i < length; i++) {
                if (block.isNull(blockOffset + i)) {
                    assertThat(expected[offset + i]).isNull();
                }
                else {
                    assertPosition(expected, block, offset + i, blockOffset + i);
                }
            }
        }

        default void assertBlock(BUFFER[] expected, Block block)
        {
            assertThat(expected.length).isEqualTo(block.getPositionCount());
            assertBlock(expected, block, 0, 0, block.getPositionCount());
        }
    }

    public static class ColumnReaderFormat<T>
            implements Assertion<T>, Writer<T>
    {
        private final PrimitiveTypeName typeName;
        private final int typeLengthInBytes;
        @Nullable
        private final LogicalTypeAnnotation logicalTypeAnnotation;
        private final Type trinoType;
        private final ParquetEncoding v1Encoding;
        private final ParquetEncoding v2Encoding;
        private final Writer<T> writerFunction;
        private final Assertion<T> assertion;

        public ColumnReaderFormat(
                PrimitiveTypeName typeName,
                Type trinoType,
                ParquetEncoding v1Encoding,
                ParquetEncoding v2Encoding,
                Writer<T> writerFunction,
                Assertion<T> assertion)
        {
            this(typeName, -1, null, trinoType, v1Encoding, v2Encoding, writerFunction, assertion);
        }

        public ColumnReaderFormat(
                PrimitiveTypeName typeName,
                @Nullable LogicalTypeAnnotation logicalTypeAnnotation,
                Type trinoType,
                ParquetEncoding v1Encoding,
                ParquetEncoding v2Encoding,
                Writer<T> writerFunction,
                Assertion<T> assertion)
        {
            this(typeName, -1, logicalTypeAnnotation, trinoType, v1Encoding, v2Encoding, writerFunction, assertion);
        }

        public ColumnReaderFormat(
                PrimitiveTypeName typeName,
                int typeLengthInBytes,
                @Nullable LogicalTypeAnnotation logicalTypeAnnotation,
                Type trinoType,
                ParquetEncoding v1Encoding,
                ParquetEncoding v2Encoding,
                Writer<T> writerFunction,
                Assertion<T> assertion)
        {
            this.typeName = requireNonNull(typeName, "typeName is null");
            this.typeLengthInBytes = typeLengthInBytes;
            this.logicalTypeAnnotation = logicalTypeAnnotation;
            this.trinoType = requireNonNull(trinoType, "trinoType is null");
            this.v1Encoding = requireNonNull(v1Encoding, "v1Encoding is null");
            this.v2Encoding = requireNonNull(v2Encoding, "v2Encoding is null");
            this.writerFunction = requireNonNull(writerFunction, "writerFunction is null");
            this.assertion = requireNonNull(assertion, "assertion is null");
        }

        public PrimitiveTypeName getTypeName()
        {
            return typeName;
        }

        public boolean hasDictionarySupport()
        {
            return typeName != PrimitiveTypeName.BOOLEAN;
        }

        public int getTypeLengthInBytes()
        {
            return typeLengthInBytes;
        }

        @Nullable
        public LogicalTypeAnnotation getLogicalTypeAnnotation()
        {
            return logicalTypeAnnotation;
        }

        public Type getTrinoType()
        {
            return trinoType;
        }

        public ValuesWriter getValuesWriter(DataPageVersion version)
        {
            ParquetEncoding encoding = version == DataPageVersion.V2 ? v2Encoding : v1Encoding;
            return TestingValuesWriters.getValuesWriter(encoding, typeName, typeLengthInBytes < 0 ? OptionalInt.empty() : OptionalInt.of(typeLengthInBytes));
        }

        public DictionaryValuesWriter getDictionaryWriter()
        {
            return (DictionaryValuesWriter) TestingValuesWriters.getValuesWriter(
                    RLE_DICTIONARY,
                    typeName,
                    typeLengthInBytes < 0 ? OptionalInt.empty() : OptionalInt.of(typeLengthInBytes));
        }

        @Override
        public T[] write(ValuesWriter parquetWriter, Integer[] values)
        {
            return writerFunction.write(parquetWriter, values);
        }

        @Override
        public void assertPosition(T[] expected, Block block, int index, int blockIndex)
        {
            assertion.assertPosition(expected, block, index, blockIndex);
        }

        @Override
        public void assertBlock(T[] expected, Block block)
        {
            Class<? extends Block> blockClass = BLOCK_CLASSES.entrySet().stream()
                    .filter(entry -> entry.getKey().getClass().isAssignableFrom(trinoType.getClass()))
                    .map(Entry::getValue)
                    .collect(onlyElement());
            if (block.getClass() != RunLengthEncodedBlock.class && block.getClass() != DictionaryBlock.class) {
                assertThat(block).isInstanceOf(blockClass);
            }
            assertion.assertBlock(expected, block);
        }

        @Override
        public void assertBlock(T[] expected, Block block, int offset, int blockOffset, int length)
        {
            assertion.assertBlock(expected, block, offset, blockOffset, length);
        }

        @Override
        public String toString()
        {
            return "ColumnReaderFormat{" +
                    "typeName=" + typeName +
                    ", typeLengthInBytes=" + typeLengthInBytes +
                    ", logicalTypeAnnotation=" + logicalTypeAnnotation +
                    ", trinoType=" + trinoType +
                    '}';
        }
    }
}
