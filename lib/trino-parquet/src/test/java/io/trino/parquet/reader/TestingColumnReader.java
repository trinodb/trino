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
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.testng.annotations.DataProvider;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.parquet.ParquetTypeUtils.getParquetEncoding;
import static io.trino.parquet.ParquetTypeUtils.paddingBigInteger;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.toDataProvider;
import static java.lang.Float.intBitsToFloat;
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
            .put(DecimalType.createDecimalType(8, 0), LongArrayBlock.class)
            .put(DecimalType.createDecimalType(38, 2), Int128ArrayBlock.class)
            .put(TIME_MILLIS, LongArrayBlock.class)
            .put(TIMESTAMP_MILLIS, LongArrayBlock.class)
            .put(TIMESTAMP_TZ_MILLIS, LongArrayBlock.class)
            .put(TIMESTAMP_TZ_NANOS, Fixed12Block.class)
            .put(TIMESTAMP_PICOS, Fixed12Block.class)
            .put(UUID, Int128ArrayBlock.class)
            .buildOrThrow();

    private static final IntFunction<DictionaryValuesWriter> DICTIONARY_INT_WRITER =
            length -> new PlainIntegerDictionaryValuesWriter(Integer.MAX_VALUE, Encoding.RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
    public static final IntFunction<DictionaryValuesWriter> DICTIONARY_LONG_WRITER =
            length -> new PlainLongDictionaryValuesWriter(Integer.MAX_VALUE, Encoding.RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
    private static final IntFunction<DictionaryValuesWriter> DICTIONARY_FIXED_LENGTH_WRITER =
            length -> new PlainFixedLenArrayDictionaryValuesWriter(Integer.MAX_VALUE, length, Encoding.RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
    private static final IntFunction<DictionaryValuesWriter> DICTIONARY_FLOAT_WRITER =
            length -> new PlainFloatDictionaryValuesWriter(Integer.MAX_VALUE, Encoding.RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
    private static final IntFunction<DictionaryValuesWriter> DICTIONARY_DOUBLE_WRITER =
            length -> new PlainDoubleDictionaryValuesWriter(Integer.MAX_VALUE, Encoding.RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
    private static final IntFunction<DictionaryValuesWriter> DICTIONARY_BINARY_WRITER =
            length -> new PlainBinaryDictionaryValuesWriter(Integer.MAX_VALUE, Encoding.RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());

    private static final IntFunction<ValuesWriter> BOOLEAN_WRITER = length -> new BooleanPlainValuesWriter();
    private static final IntFunction<ValuesWriter> FIXED_LENGTH_WRITER =
            length -> new FixedLenByteArrayPlainValuesWriter(length, 1024, 1024, HeapByteBufferAllocator.getInstance());
    public static final IntFunction<ValuesWriter> PLAIN_WRITER =
            length -> new PlainValuesWriter(1024, 1024, HeapByteBufferAllocator.getInstance());
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

    private static final Assertion<Number> ASSERT_BYTE = (values, block, offset, blockOffset) -> assertThat(block.getByte(blockOffset, 0)).isEqualTo(values[offset].byteValue());
    private static final Assertion<Number> ASSERT_SHORT = (values, block, offset, blockOffset) -> assertThat(block.getShort(blockOffset, 0)).isEqualTo(values[offset].shortValue());
    private static final Assertion<Number> ASSERT_INT = (values, block, offset, blockOffset) -> assertThat(block.getInt(blockOffset, 0)).isEqualTo(values[offset].intValue());
    private static final Assertion<Float> ASSERT_FLOAT = (values, block, offset, blockOffset) -> assertThat(intBitsToFloat(block.getInt(blockOffset, 0))).isEqualTo(values[offset].floatValue());
    private static final Assertion<Number> ASSERT_LONG = (values, block, offset, blockOffset) -> assertThat(block.getLong(blockOffset, 0)).isEqualTo(values[offset].longValue());
    private static final Assertion<Double> ASSERT_DOUBLE = (values, block, offset, blockOffset) -> assertThat(Double.longBitsToDouble(block.getLong(blockOffset, 0))).isEqualTo(values[offset].doubleValue());
    private static final Assertion<Float> ASSERT_DOUBLE_STORED_AS_FLOAT = (values, block, offset, blockOffset) ->
            assertThat(Double.longBitsToDouble(block.getLong(blockOffset, 0))).isEqualTo(values[offset].floatValue());
    private static final Assertion<Number> ASSERT_INT_128 = new Assertion<>()
    {
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
                    assertThat(block.getLong(blockOffset + i, 0)).isEqualTo(expected[2 * (offset + i)].longValue());
                    assertThat(block.getLong(blockOffset + i, 8)).isEqualTo(expected[2 * (offset + i) + 1].longValue());
                }
            }
        }

        @Override
        public void assertBlock(Number[] expected, Block block)
        {
            assertThat(expected.length).isEqualTo(block.getPositionCount() * 2);
            assertBlock(expected, block, 0, 0, block.getPositionCount());
        }
    };

    private static Assertion<Number> assertRescaled(int scale)
    {
        return (expected, block, index, blockIndex) -> {
            long multiplier = longTenToNth(scale);
            assertThat(block.getLong(blockIndex, 0)).isEqualTo(expected[index].longValue() * multiplier);
        };
    }

    private static Assertion<Number> assertShortToLongRescaled(int scale)
    {
        long multiplier = longTenToNth(scale);
        return (expected, block, index, blockIndex) -> {
            assertThat(block.getLong(blockIndex, 0)).isEqualTo(0);
            assertThat(block.getLong(blockIndex, 8)).isEqualTo(expected[index].longValue() * multiplier);
        };
    }

    private static Assertion<Number> assertLongToShortRescaled(int scale)
    {
        long multiplier = longTenToNth(scale);
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
                assertThat(block.getLong(blockIndex, 0)).isEqualTo(expected[2 * index + 1].longValue() * multiplier);
            }

            @Override
            public void assertBlock(Number[] expected, Block block)
            {
                assertThat(expected.length).isEqualTo(block.getPositionCount() * 2);
                assertBlock(expected, block, 0, 0, block.getPositionCount());
            }
        };
    }

    private static Assertion<Number> assertLongRescaled(int scale)
    {
        long multiplier = longTenToNth(scale);
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
                assertThat(block.getLong(blockIndex, 0)).isEqualTo(0);
                assertThat(block.getLong(blockIndex, 8)).isEqualTo(expected[2 * index + 1].longValue() * multiplier);
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
        int positionLength = block.getSliceLength(blockOffset);
        assertThat(block.getSlice(blockOffset, 0, positionLength).getBytes()).isEqualTo(values[offset].getBytes(UTF_8));
    };

    private static Assertion<Number> assertTime(int precision)
    {
        return assertTime(precision, -precision);
    }

    private static Assertion<Number> assertTime(int precision, int rounding)
    {
        long multiplier = LongMath.pow(10, max(0, precision));
        return (values, block, offset, blockOffset) -> {
            long value = values[offset].longValue();
            if (rounding > 0 | precision < 0) {
                value = Timestamps.round(value, rounding) / LongMath.pow(10, -precision);
            }
            assertThat(block.getLong(blockOffset, 0)).isEqualTo(value * multiplier);
        };
    }

    private static Assertion<DecodedTimestamp> assertInt96Short(int rounding)
    {
        return (values, block, offset, blockOffset) -> {
            long epochSeconds = values[offset].epochSeconds();
            int nanos = values[offset].nanosOfSecond();
            long epochNanos = epochSeconds * Timestamps.NANOSECONDS_PER_SECOND + nanos;
            long expectedMicros = Timestamps.round(epochNanos, rounding) / 1000;

            assertThat(block.getLong(blockOffset, 0)).isEqualTo(expectedMicros);
        };
    }

    private static Assertion<DecodedTimestamp> assertInt96Long()
    {
        return (values, block, offset, blockOffset) -> {
            long epochSeconds = values[offset].epochSeconds();
            int nanos = values[offset].nanosOfSecond();
            long epochNanos = epochSeconds * Timestamps.NANOSECONDS_PER_SECOND + nanos;

            long actualEpochMicros = block.getLong(blockOffset, 0);
            long actualNanos = block.getInt(blockOffset, 8) / 1000;
            long actualEpochNanos = actualEpochMicros * 1000 + actualNanos;
            assertThat(actualEpochNanos).isEqualTo(epochNanos);
        };
    }

    private static Assertion<DecodedTimestamp> assertInt96ShortWithTimezone()
    {
        return (values, block, offset, blockOffset) -> {
            long epochSeconds = values[offset].epochSeconds();
            int nanos = values[offset].nanosOfSecond();
            long epochNanos = epochSeconds * Timestamps.NANOSECONDS_PER_SECOND + nanos;
            long expectedMillis = Timestamps.round(epochNanos, 6) / 1000000;

            long packed = block.getLong(blockOffset, 0);
            long blockMillis = unpackMillisUtc(packed);
            TimeZoneKey timeZoneKey = unpackZoneKey(packed);
            assertThat(timeZoneKey).isEqualTo(UTC_KEY);

            assertThat(blockMillis).isEqualTo(expectedMillis);
        };
    }

    public static Assertion<Number> assertLongTimestamp(int precision)
    {
        int multiplier = IntMath.pow(10, precision);
        return (values, block, offset, blockOffset) ->
        {
            LongTimestamp value = (LongTimestamp) TIMESTAMP_PICOS.getObject(block, blockOffset);
            // We ignore higher bits of long timestamp as we only test on small numbers
            long longValue = (value.getEpochMicros() * 1_000_000) + value.getPicosOfMicro();
            assertThat(longValue).isEqualTo(values[offset].longValue() * multiplier);
        };
    }

    private static Assertion<Number> assertTimestampWithTimeZone(int precision)
    {
        return assertTimestampWithTimeZone(precision, -precision);
    }

    private static Assertion<Number> assertTimestampWithTimeZone(int precision, int rounding)
    {
        long multiplier = LongMath.pow(10, max(0, precision));
        return (values, block, offset, blockOffset) ->
        {
            long packed = block.getLong(blockOffset, 0);
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

    private static Assertion<Number> assertLongTimestampWithTimeZone(int precision)
    {
        long multiplier = LongMath.pow(10, precision);
        return (values, block, offset, blockOffset) ->
        {
            LongTimestampWithTimeZone packed = (LongTimestampWithTimeZone) TIMESTAMP_TZ_NANOS.getObject(block, blockOffset);
            long longValue = (packed.getEpochMillis() * 1_000_000_000) + packed.getPicosOfMilli();
            TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey(packed.getTimeZoneKey());
            long value = values[offset].longValue();
            assertThat(timeZoneKey).isEqualTo(UTC_KEY);
            assertThat(longValue).isEqualTo(value * multiplier);
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

    public static DictionaryPage toTrinoDictionaryPage(org.apache.parquet.column.page.DictionaryPage dictionary)
    {
        try {
            return new DictionaryPage(
                    Slices.wrappedBuffer(dictionary.getBytes().toByteArray()),
                    dictionary.getDictionarySize(),
                    getParquetEncoding(dictionary.getEncoding()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DataProvider(name = "readersWithPageVersions")
    public static Object[][] readersWithPageVersions()
    {
        return cartesianProduct(
                Stream.of(DataPageVersion.V1, DataPageVersion.V2).collect(toDataProvider()),
                Stream.of(TestingColumnReader.columnReaders()).collect(toDataProvider()));
    }

    @DataProvider(name = "dictionaryReadersWithPageVersions")
    public static Object[][] dictionaryReadersWithPageVersions()
    {
        return cartesianProduct(
                Stream.of(DataPageVersion.V1, DataPageVersion.V2).collect(toDataProvider()),
                Arrays.stream(TestingColumnReader.columnReaders())
                        .filter(reader -> reader.dictionaryWriterProvider != null)
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
                new ColumnReaderFormat<>(BOOLEAN, BooleanType.BOOLEAN, BOOLEAN_WRITER, null, WRITE_BOOLEAN, ASSERT_BYTE),
                new ColumnReaderFormat<>(FLOAT, REAL, PLAIN_WRITER, DICTIONARY_FLOAT_WRITER, WRITE_FLOAT, ASSERT_FLOAT),
                // FLOAT parquet primitive type can be read as a DOUBLE or REAL type in Trino
                new ColumnReaderFormat<>(FLOAT, DoubleType.DOUBLE, PLAIN_WRITER, DICTIONARY_FLOAT_WRITER, WRITE_FLOAT, ASSERT_DOUBLE_STORED_AS_FLOAT),
                new ColumnReaderFormat<>(DOUBLE, DoubleType.DOUBLE, PLAIN_WRITER, DICTIONARY_DOUBLE_WRITER, WRITE_DOUBLE, ASSERT_DOUBLE),
                new ColumnReaderFormat<>(INT32, decimalType(0, 8), createDecimalType(8), PLAIN_WRITER, DICTIONARY_INT_WRITER, WRITE_INT, ASSERT_INT),
                // INT32 can be read as a ShortDecimalType in Trino without decimal logical type annotation as well
                new ColumnReaderFormat<>(INT32, createDecimalType(8, 0), PLAIN_WRITER, DICTIONARY_INT_WRITER, WRITE_INT, ASSERT_INT),
                new ColumnReaderFormat<>(INT32, BIGINT, PLAIN_WRITER, DICTIONARY_INT_WRITER, WRITE_INT, ASSERT_LONG),
                new ColumnReaderFormat<>(INT32, INTEGER, PLAIN_WRITER, DICTIONARY_INT_WRITER, WRITE_INT, ASSERT_INT),
                new ColumnReaderFormat<>(INT32, SMALLINT, PLAIN_WRITER, DICTIONARY_INT_WRITER, WRITE_SHORT, ASSERT_SHORT),
                new ColumnReaderFormat<>(INT32, TINYINT, PLAIN_WRITER, DICTIONARY_INT_WRITER, WRITE_BYTE, ASSERT_BYTE),
                new ColumnReaderFormat<>(BINARY, VARCHAR, PLAIN_WRITER, DICTIONARY_BINARY_WRITER, WRITE_BINARY, ASSERT_BINARY),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, null, VARCHAR, FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, writeFixedWidthBinary(8), ASSERT_BINARY),
                new ColumnReaderFormat<>(INT64, decimalType(0, 16), createDecimalType(16), PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG, ASSERT_LONG),
                new ColumnReaderFormat<>(INT64, BIGINT, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG, ASSERT_LONG),
                new ColumnReaderFormat<>(INT64, INTEGER, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG, ASSERT_INT),
                new ColumnReaderFormat<>(INT64, SMALLINT, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG, ASSERT_SHORT),
                new ColumnReaderFormat<>(INT64, TINYINT, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG, ASSERT_BYTE),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 2), createDecimalType(2), FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_SHORT_DECIMAL, ASSERT_LONG),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 16, decimalType(2, 38), createDecimalType(38, 2), FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, writeLongDecimal(16), ASSERT_INT_128),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 16, uuidType(), UUID, FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_UUID, ASSERT_INT_128),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 16, null, UUID, FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_UUID, ASSERT_INT_128),
                // Trino type precision is irrelevant since the data is always stored as picoseconds
                new ColumnReaderFormat<>(INT64, timeType(false, MICROS), TimeType.TIME_MICROS, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG, assertTime(6)),
                // Reading a column TimeLogicalTypeAnnotation as a BIGINT
                new ColumnReaderFormat<>(INT64, timeType(false, MICROS), BIGINT, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG, ASSERT_LONG),
                // Short decimals
                new ColumnReaderFormat<>(INT32, decimalType(0, 8), createDecimalType(8), PLAIN_WRITER, DICTIONARY_INT_WRITER, WRITE_INT, ASSERT_INT),
                // INT32 values can be read as zero scale decimals provided the precision is at least 10 to accommodate the largest possible integer
                new ColumnReaderFormat<>(INT32, createDecimalType(10), PLAIN_WRITER, DICTIONARY_INT_WRITER, WRITE_INT, ASSERT_INT),
                new ColumnReaderFormat<>(INT32, decimalType(0, 8), BIGINT, PLAIN_WRITER, DICTIONARY_INT_WRITER, WRITE_INT, ASSERT_LONG),
                new ColumnReaderFormat<>(INT32, decimalType(0, 8), INTEGER, PLAIN_WRITER, DICTIONARY_INT_WRITER, WRITE_INT, ASSERT_INT),
                new ColumnReaderFormat<>(INT32, decimalType(0, 8), SMALLINT, PLAIN_WRITER, DICTIONARY_INT_WRITER, WRITE_SHORT, ASSERT_SHORT),
                new ColumnReaderFormat<>(INT32, decimalType(0, 8), TINYINT, PLAIN_WRITER, DICTIONARY_INT_WRITER, WRITE_BYTE, ASSERT_BYTE),
                new ColumnReaderFormat<>(INT64, decimalType(0, 8), createDecimalType(8), PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG, ASSERT_LONG),
                new ColumnReaderFormat<>(INT64, decimalType(0, 8), BIGINT, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG, ASSERT_LONG),
                new ColumnReaderFormat<>(INT64, decimalType(0, 8), INTEGER, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG, ASSERT_INT),
                new ColumnReaderFormat<>(INT64, decimalType(0, 8), SMALLINT, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG, ASSERT_SHORT),
                new ColumnReaderFormat<>(INT64, decimalType(0, 8), TINYINT, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG, ASSERT_BYTE),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 2), createDecimalType(2), FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_SHORT_DECIMAL, ASSERT_LONG),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 2), BIGINT, FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_SHORT_DECIMAL, ASSERT_LONG),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 2), INTEGER, FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_SHORT_DECIMAL, ASSERT_INT),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 2), SMALLINT, FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_SHORT_DECIMAL, ASSERT_SHORT),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 2), TINYINT, FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_SHORT_DECIMAL, ASSERT_BYTE),
                new ColumnReaderFormat<>(BINARY, decimalType(0, 8), createDecimalType(8), PLAIN_WRITER, DICTIONARY_BINARY_WRITER, WRITE_BINARY_DECIMAL, ASSERT_LONG),
                new ColumnReaderFormat<>(BINARY, decimalType(0, 8), BIGINT, PLAIN_WRITER, DICTIONARY_BINARY_WRITER, WRITE_BINARY_DECIMAL, ASSERT_LONG),
                new ColumnReaderFormat<>(BINARY, decimalType(0, 8), INTEGER, PLAIN_WRITER, DICTIONARY_BINARY_WRITER, WRITE_BINARY_DECIMAL, ASSERT_INT),
                new ColumnReaderFormat<>(BINARY, decimalType(0, 8), SMALLINT, PLAIN_WRITER, DICTIONARY_BINARY_WRITER, WRITE_BINARY_DECIMAL, ASSERT_SHORT),
                new ColumnReaderFormat<>(BINARY, decimalType(0, 8), TINYINT, PLAIN_WRITER, DICTIONARY_BINARY_WRITER, WRITE_BINARY_DECIMAL, ASSERT_BYTE),
                // Long decimals
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 16, decimalType(2, 38), createDecimalType(38, 2), FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, writeLongDecimal(16), ASSERT_INT_128),
                new ColumnReaderFormat<>(BINARY, 16, decimalType(2, 38), createDecimalType(38, 2), PLAIN_WRITER, DICTIONARY_BINARY_WRITER, WRITE_BINARY_LONG_DECIMAL, ASSERT_INT_128),
                // Rescaled decimals
                new ColumnReaderFormat<>(INT32, decimalType(0, 7), createDecimalType(8, 1), PLAIN_WRITER, DICTIONARY_INT_WRITER, WRITE_INT, assertRescaled(1)),
                new ColumnReaderFormat<>(INT64, decimalType(0, 7), createDecimalType(8, 2), PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG, assertRescaled(2)),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 7), createDecimalType(8, 3), FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_SHORT_DECIMAL, assertRescaled(3)),
                new ColumnReaderFormat<>(INT32, decimalType(0, 7), createDecimalType(30, 1), PLAIN_WRITER, DICTIONARY_INT_WRITER, WRITE_INT, assertShortToLongRescaled(1)),
                new ColumnReaderFormat<>(INT64, decimalType(0, 7), createDecimalType(30, 2), PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG, assertShortToLongRescaled(2)),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 8, decimalType(0, 7), createDecimalType(30, 3), FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_SHORT_DECIMAL, assertShortToLongRescaled(3)),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 16, decimalType(0, 38), createDecimalType(8, 1), FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, writeLongDecimal(16), assertLongToShortRescaled(1)),
                new ColumnReaderFormat<>(FIXED_LEN_BYTE_ARRAY, 16, decimalType(0, 38), createDecimalType(37, 2), FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, writeLongDecimal(16), assertLongRescaled(2)),
                // Timestamps.
                //  The `precision` and `rounding` arguments at the end of every assertion may be difficult to understand. They are a direct
                // consequence of various Trino timestamp representations.
                new ColumnReaderFormat<>(INT64, timestampType(false, MILLIS), TIMESTAMP_MICROS, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG_TIMESTAMP, assertTime(3)),
                new ColumnReaderFormat<>(INT64, timestampType(false, MILLIS), TIMESTAMP_PICOS, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG_TIMESTAMP, assertLongTimestamp(9)),
                new ColumnReaderFormat<>(INT64, timestampType(false, MILLIS), TIMESTAMP_TZ_MILLIS, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG_TIMESTAMP, assertTimestampWithTimeZone(0)),
                new ColumnReaderFormat<>(INT64, timestampType(false, MICROS), TIMESTAMP_MILLIS, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG_TIMESTAMP, assertTime(0, 3)),
                new ColumnReaderFormat<>(INT64, timestampType(false, MICROS), TIMESTAMP_MICROS, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG_TIMESTAMP, assertTime(0)),
                new ColumnReaderFormat<>(INT64, timestampType(false, MICROS), TIMESTAMP_NANOS, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG_TIMESTAMP, assertLongTimestamp(6)),
                new ColumnReaderFormat<>(INT64, timestampType(false, MICROS), TIMESTAMP_TZ_MILLIS, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG_TIMESTAMP, assertTimestampWithTimeZone(-3, 3)),
                new ColumnReaderFormat<>(INT64, timestampType(false, MICROS), TIMESTAMP_TZ_NANOS, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG_TIMESTAMP, assertLongTimestampWithTimeZone(6)),
                new ColumnReaderFormat<>(INT64, timestampType(false, NANOS), TIMESTAMP_MILLIS, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG_TIMESTAMP, assertTime(-3, 6)),
                new ColumnReaderFormat<>(INT64, timestampType(false, NANOS), TIMESTAMP_MICROS, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG_TIMESTAMP, assertTime(-3)),
                new ColumnReaderFormat<>(INT64, timestampType(false, NANOS), TIMESTAMP_NANOS, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG_TIMESTAMP, assertLongTimestamp(3)),
                new ColumnReaderFormat<>(INT96, 12, null, TIMESTAMP_MILLIS, FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_INT96, assertInt96Short(6)),
                new ColumnReaderFormat<>(INT96, 12, null, TIMESTAMP_MICROS, FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_INT96, assertInt96Short(3)),
                new ColumnReaderFormat<>(INT96, 12, null, TIMESTAMP_NANOS, FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_INT96, assertInt96Long()),
                new ColumnReaderFormat<>(INT96, 12, null, TIMESTAMP_PICOS, FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_INT96, assertInt96Long()),
                new ColumnReaderFormat<>(INT96, 12, null, TIMESTAMP_TZ_MILLIS, FIXED_LENGTH_WRITER, DICTIONARY_FIXED_LENGTH_WRITER, WRITE_INT96, assertInt96ShortWithTimezone()),
                // timestamps read as bigint
                new ColumnReaderFormat<>(INT64, timestampType(false, MILLIS), BIGINT, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG_TIMESTAMP, ASSERT_LONG),
                new ColumnReaderFormat<>(INT64, timestampType(false, MICROS), BIGINT, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG_TIMESTAMP, ASSERT_LONG),
                new ColumnReaderFormat<>(INT64, timestampType(false, NANOS), BIGINT, PLAIN_WRITER, DICTIONARY_LONG_WRITER, WRITE_LONG_TIMESTAMP, ASSERT_LONG)};
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
        private final IntFunction<ValuesWriter> plainWriterProvider;
        @Nullable
        private final IntFunction<DictionaryValuesWriter> dictionaryWriterProvider;
        private final Writer<T> writerFunction;
        private final Assertion<T> assertion;

        public ColumnReaderFormat(
                PrimitiveTypeName typeName,
                Type trinoType,
                IntFunction<ValuesWriter> plainWriterProvider,
                @Nullable IntFunction<DictionaryValuesWriter> dictionaryWriterProvider,
                Writer<T> writerFunction,
                Assertion<T> assertion)
        {
            this(typeName, null, trinoType, plainWriterProvider, dictionaryWriterProvider, writerFunction, assertion);
        }

        public ColumnReaderFormat(
                PrimitiveTypeName typeName,
                @Nullable LogicalTypeAnnotation logicalTypeAnnotation,
                Type trinoType,
                IntFunction<ValuesWriter> plainWriterProvider,
                @Nullable IntFunction<DictionaryValuesWriter> dictionaryWriterProvider,
                Writer<T> writerFunction,
                Assertion<T> assertion)
        {
            this(typeName, -1, logicalTypeAnnotation, trinoType, plainWriterProvider, dictionaryWriterProvider, writerFunction, assertion);
        }

        public ColumnReaderFormat(
                PrimitiveTypeName typeName,
                int typeLengthInBytes,
                @Nullable LogicalTypeAnnotation logicalTypeAnnotation,
                Type trinoType,
                IntFunction<ValuesWriter> plainWriterProvider,
                @Nullable IntFunction<DictionaryValuesWriter> dictionaryWriterProvider,
                Writer<T> writerFunction,
                Assertion<T> assertion)
        {
            this.typeName = requireNonNull(typeName, "typeName is null");
            this.typeLengthInBytes = typeLengthInBytes;
            this.logicalTypeAnnotation = logicalTypeAnnotation;
            this.trinoType = requireNonNull(trinoType, "trinoType is null");
            this.plainWriterProvider = requireNonNull(plainWriterProvider, "plainWriterSupplier is null");
            this.dictionaryWriterProvider = dictionaryWriterProvider;
            this.writerFunction = requireNonNull(writerFunction, "writerFunction is null");
            this.assertion = requireNonNull(assertion, "assertion is null");
        }

        public PrimitiveTypeName getTypeName()
        {
            return typeName;
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

        public ValuesWriter getPlainWriter()
        {
            return plainWriterProvider.apply(typeLengthInBytes);
        }

        public DictionaryValuesWriter getDictionaryWriter()
        {
            return requireNonNull(dictionaryWriterProvider, "dictionaryWriterProvider is null").apply(typeLengthInBytes);
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
