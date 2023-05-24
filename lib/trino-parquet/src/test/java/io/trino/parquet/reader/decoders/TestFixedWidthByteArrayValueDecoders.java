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
package io.trino.parquet.reader.decoders;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.parquet.reader.flat.BinaryBuffer;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.UuidType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.util.List;
import java.util.OptionalInt;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetEncoding.DELTA_BYTE_ARRAY;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.ParquetTypeUtils.checkBytesFitInShortDecimal;
import static io.trino.parquet.ParquetTypeUtils.getShortDecimalValue;
import static io.trino.parquet.ParquetTypeUtils.paddingBigInteger;
import static io.trino.parquet.reader.TestData.longToBytes;
import static io.trino.parquet.reader.TestData.maxPrecision;
import static io.trino.parquet.reader.TestData.randomBinaryData;
import static io.trino.parquet.reader.TestData.randomUtf8;
import static io.trino.parquet.reader.TestData.unscaledRandomShortDecimalSupplier;
import static io.trino.parquet.reader.flat.BinaryColumnAdapter.BINARY_ADAPTER;
import static io.trino.parquet.reader.flat.Int128ColumnAdapter.INT128_ADAPTER;
import static io.trino.parquet.reader.flat.LongColumnAdapter.LONG_ADAPTER;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.DataProviders.concat;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestFixedWidthByteArrayValueDecoders
        extends AbstractValueDecodersTest
{
    private static final List<ParquetEncoding> ENCODINGS = ImmutableList.of(PLAIN, RLE_DICTIONARY, DELTA_BYTE_ARRAY);

    private static final BiConsumer<BinaryBuffer, BinaryBuffer> BINARY_ASSERT = (actual, expected) -> {
        assertThat(actual.getOffsets()).containsExactly(expected.getOffsets());
        assertThat(actual.asSlice()).isEqualTo(expected.asSlice());
    };

    @Override
    protected Object[][] tests()
    {
        return concat(
                generateShortDecimalTests(PLAIN),
                generateShortDecimalTests(DELTA_BYTE_ARRAY),
                generateShortDecimalTests(RLE_DICTIONARY),
                generateLongDecimalTests(PLAIN),
                generateLongDecimalTests(DELTA_BYTE_ARRAY),
                generateLongDecimalTests(RLE_DICTIONARY),
                generateUuidTests(),
                generateVarbinaryTests(),
                generateVarcharTests());
    }

    private static Object[][] generateShortDecimalTests(ParquetEncoding encoding)
    {
        return IntStream.range(1, 17)
                .mapToObj(typeLength -> {
                    int precision = maxPrecision(min(typeLength, 8));
                    return new Object[] {
                            createShortDecimalTestType(typeLength, precision),
                            encoding,
                            createShortDecimalInputDataProvider(typeLength, precision)};
                })
                .toArray(Object[][]::new);
    }

    private static Object[][] generateLongDecimalTests(ParquetEncoding encoding)
    {
        return IntStream.range(9, 17)
                .mapToObj(typeLength -> new Object[] {
                        createLongDecimalTestType(typeLength),
                        encoding,
                        createLongDecimalInputDataProvider(typeLength)})
                .toArray(Object[][]::new);
    }

    private static Object[][] generateUuidTests()
    {
        return ENCODINGS.stream()
                .map(encoding -> new Object[] {
                        createUuidTestType(),
                        encoding,
                        new UuidInputProvider()})
                .toArray(Object[][]::new);
    }

    private static Object[][] generateVarbinaryTests()
    {
        int typeLength = 7;
        return ENCODINGS.stream()
                .map(encoding -> new Object[] {
                        createVarbinaryTestType(typeLength),
                        encoding,
                        createRandomBinaryInputProvider(typeLength)})
                .toArray(Object[][]::new);
    }

    private static Object[][] generateVarcharTests()
    {
        int typeLength = 13;
        return ENCODINGS.stream()
                .map(encoding -> new Object[] {
                        createVarcharTestType(typeLength),
                        encoding,
                        createRandomUtf8BinaryInputProvider(typeLength)})
                .toArray(Object[][]::new);
    }

    private static TestType<long[]> createShortDecimalTestType(int typeLength, int precision)
    {
        DecimalType decimalType = DecimalType.createDecimalType(precision, 2);
        PrimitiveField primitiveField = createField(FIXED_LEN_BYTE_ARRAY, OptionalInt.of(typeLength), decimalType);
        return new TestType<>(
                primitiveField,
                ValueDecoders::getFixedWidthShortDecimalDecoder,
                valuesReader -> new ShortDecimalApacheParquetValueDecoder(valuesReader, primitiveField.getDescriptor()),
                LONG_ADAPTER,
                (actual, expected) -> assertThat(actual).isEqualTo(expected));
    }

    private static TestType<long[]> createLongDecimalTestType(int typeLength)
    {
        int precision = maxPrecision(typeLength);
        DecimalType decimalType = DecimalType.createDecimalType(precision, 2);
        return new TestType<>(
                createField(FIXED_LEN_BYTE_ARRAY, OptionalInt.of(typeLength), decimalType),
                ValueDecoders::getFixedWidthLongDecimalDecoder,
                LongDecimalApacheParquetValueDecoder::new,
                INT128_ADAPTER,
                (actual, expected) -> assertThat(actual).isEqualTo(expected));
    }

    private static TestType<long[]> createUuidTestType()
    {
        return new TestType<>(
                createField(FIXED_LEN_BYTE_ARRAY, OptionalInt.of(16), UuidType.UUID),
                ValueDecoders::getUuidDecoder,
                UuidApacheParquetValueDecoder::new,
                INT128_ADAPTER,
                (actual, expected) -> assertThat(actual).isEqualTo(expected));
    }

    private static TestType<BinaryBuffer> createVarbinaryTestType(int typeLength)
    {
        return new TestType<>(
                createField(FIXED_LEN_BYTE_ARRAY, OptionalInt.of(typeLength), VARBINARY),
                ValueDecoders::getFixedWidthBinaryDecoder,
                BinaryApacheParquetValueDecoder::new,
                BINARY_ADAPTER,
                BINARY_ASSERT);
    }

    private static TestType<BinaryBuffer> createVarcharTestType(int typeLength)
    {
        return new TestType<>(
                createField(FIXED_LEN_BYTE_ARRAY, OptionalInt.of(typeLength), VARCHAR),
                ValueDecoders::getFixedWidthBinaryDecoder,
                BinaryApacheParquetValueDecoder::new,
                BINARY_ADAPTER,
                BINARY_ASSERT);
    }

    private static InputDataProvider createShortDecimalInputDataProvider(int typeLength, int precision)
    {
        return new InputDataProvider() {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                long[] values = unscaledRandomShortDecimalSupplier(min(typeLength * Byte.SIZE, 64), precision).apply(dataSize);
                byte[][] bytes = new byte[dataSize][];
                for (int i = 0; i < dataSize; i++) {
                    bytes[i] = longToBytes(values[i], typeLength);
                }
                return writeBytes(valuesWriter, bytes);
            }

            @Override
            public String toString()
            {
                return "fixed_len_byte_array(" + typeLength + ")";
            }
        };
    }

    private static InputDataProvider createLongDecimalInputDataProvider(int typeLength)
    {
        return new InputDataProvider() {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                byte[][] bytes = new byte[dataSize][];
                for (int i = 0; i < dataSize; i++) {
                    bytes[i] = paddingBigInteger(
                            new BigInteger(Math.min(typeLength * Byte.SIZE - 1, 126), random),
                            typeLength);
                }
                return writeBytes(valuesWriter, bytes);
            }

            @Override
            public String toString()
            {
                return "fixed_len_byte_array(" + typeLength + ")";
            }
        };
    }

    private static class UuidInputProvider
            implements InputDataProvider
    {
        @Override
        public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
        {
            byte[][] bytes = new byte[dataSize][];
            for (int i = 0; i < dataSize; i++) {
                UUID uuid = UUID.randomUUID();
                bytes[i] = Slices.wrappedLongArray(
                                uuid.getMostSignificantBits(),
                                uuid.getLeastSignificantBits())
                        .getBytes();
            }
            return writeBytes(valuesWriter, bytes);
        }

        @Override
        public String toString()
        {
            return "uuid";
        }
    }

    private static InputDataProvider createRandomBinaryInputProvider(int length)
    {
        return new InputDataProvider()
        {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                return writeBytes(valuesWriter, randomBinaryData(dataSize, length, length));
            }

            @Override
            public String toString()
            {
                return "random(" + length + ")";
            }
        };
    }

    private static InputDataProvider createRandomUtf8BinaryInputProvider(int length)
    {
        return new InputDataProvider()
        {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                return writeBytes(valuesWriter, randomUtf8(dataSize, length));
            }

            @Override
            public String toString()
            {
                return "random utf8(" + length + ")";
            }
        };
    }

    private static DataBuffer writeBytes(ValuesWriter valuesWriter, byte[][] input)
    {
        for (byte[] value : input) {
            valuesWriter.writeBytes(Binary.fromConstantByteArray(value));
        }

        return getWrittenBuffer(valuesWriter);
    }

    private static final class ShortDecimalApacheParquetValueDecoder
            implements ValueDecoder<long[]>
    {
        private final ValuesReader delegate;
        private final ColumnDescriptor descriptor;
        private final int typeLength;

        private ShortDecimalApacheParquetValueDecoder(ValuesReader delegate, ColumnDescriptor descriptor)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            LogicalTypeAnnotation logicalTypeAnnotation = descriptor.getPrimitiveType().getLogicalTypeAnnotation();
            checkArgument(
                    logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation
                            && decimalAnnotation.getPrecision() <= Decimals.MAX_SHORT_PRECISION,
                    "Column %s is not a short decimal",
                    descriptor);
            this.typeLength = descriptor.getPrimitiveType().getTypeLength();
            checkArgument(typeLength > 0 && typeLength <= 16, "Expected column %s to have type length in range (1-16)", descriptor);
            this.descriptor = descriptor;
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            int bytesOffset = 0;
            int bytesLength = typeLength;
            if (typeLength > Long.BYTES) {
                bytesOffset = typeLength - Long.BYTES;
                bytesLength = Long.BYTES;
            }
            for (int i = offset; i < offset + length; i++) {
                byte[] bytes = delegate.readBytes().getBytes();
                checkBytesFitInShortDecimal(bytes, 0, bytesOffset, descriptor);
                values[i] = getShortDecimalValue(bytes, bytesOffset, bytesLength);
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    private static final class LongDecimalApacheParquetValueDecoder
            implements ValueDecoder<long[]>
    {
        private final ValuesReader delegate;

        private LongDecimalApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            int endOffset = (offset + length) * 2;
            for (int currentOutputOffset = offset * 2; currentOutputOffset < endOffset; currentOutputOffset += 2) {
                Int128 value = Int128.fromBigEndian(delegate.readBytes().getBytes());
                values[currentOutputOffset] = value.getHigh();
                values[currentOutputOffset + 1] = value.getLow();
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    private static final class UuidApacheParquetValueDecoder
            implements ValueDecoder<long[]>
    {
        private static final VarHandle LONG_ARRAY_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

        private final ValuesReader delegate;

        private UuidApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            int endOffset = (offset + length) * 2;
            for (int currentOutputOffset = offset * 2; currentOutputOffset < endOffset; currentOutputOffset += 2) {
                byte[] data = delegate.readBytes().getBytes();
                values[currentOutputOffset] = (long) LONG_ARRAY_HANDLE.get(data, 0);
                values[currentOutputOffset + 1] = (long) LONG_ARRAY_HANDLE.get(data, Long.BYTES);
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }
}
