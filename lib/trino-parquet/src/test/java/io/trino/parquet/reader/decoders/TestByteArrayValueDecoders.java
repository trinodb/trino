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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.parquet.reader.flat.BinaryBuffer;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.VarcharType;
import io.trino.spi.type.Varchars;
import io.trino.testing.DataProviders;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;

import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetEncoding.DELTA_BYTE_ARRAY;
import static io.trino.parquet.ParquetEncoding.DELTA_LENGTH_BYTE_ARRAY;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.reader.TestData.randomAsciiData;
import static io.trino.parquet.reader.TestData.randomBinaryData;
import static io.trino.parquet.reader.flat.BinaryColumnAdapter.BINARY_ADAPTER;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestByteArrayValueDecoders
        extends AbstractValueDecodersTest
{
    private static final List<ParquetEncoding> ENCODINGS = ImmutableList.of(PLAIN, RLE_DICTIONARY, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY);

    private static final BiConsumer<BinaryBuffer, BinaryBuffer> BINARY_ASSERT = (actual, expected) -> {
        assertThat(actual.getOffsets()).containsExactly(expected.getOffsets());
        assertThat(actual.asSlice()).isEqualTo(expected.asSlice());
    };

    @Override
    protected Object[][] tests()
    {
        return DataProviders.concat(
                testArgs(
                        new TestType<>(
                                createField(BINARY, OptionalInt.empty(), VARBINARY),
                                ValueDecoders::getBinaryDecoder,
                                BinaryApacheParquetValueDecoder::new,
                                BINARY_ADAPTER,
                                BINARY_ASSERT),
                        ENCODINGS,
                        generateUnboundedBinaryInputs()),
                testArgs(
                        new TestType<>(
                                createField(BINARY, OptionalInt.empty(), createUnboundedVarcharType()),
                                ValueDecoders::getBinaryDecoder,
                                BinaryApacheParquetValueDecoder::new,
                                BINARY_ADAPTER,
                                BINARY_ASSERT),
                        ENCODINGS,
                        generateUnboundedBinaryInputs()),
                testArgs(createBoundedVarcharTestType(), ENCODINGS, generateBoundedVarcharInputs()),
                testArgs(createCharTestType(), ENCODINGS, generateCharInputs()));
    }

    private static TestType<BinaryBuffer> createBoundedVarcharTestType()
    {
        VarcharType varcharType = createVarcharType(5);
        return new TestType<>(
                createField(BINARY, OptionalInt.empty(), varcharType),
                ValueDecoders::getBoundedVarcharBinaryDecoder,
                valuesReader -> new BoundedVarcharApacheParquetValueDecoder(valuesReader, varcharType),
                BINARY_ADAPTER,
                BINARY_ASSERT);
    }

    private static TestType<BinaryBuffer> createCharTestType()
    {
        CharType charType = createCharType(5);
        return new TestType<>(
                createField(BINARY, OptionalInt.empty(), charType),
                ValueDecoders::getCharBinaryDecoder,
                valuesReader -> new CharApacheParquetValueDecoder(valuesReader, charType),
                BINARY_ADAPTER,
                BINARY_ASSERT);
    }

    private static InputDataProvider[] generateUnboundedBinaryInputs()
    {
        return ImmutableList.of(5, 10, 11, 13, 100, 1000).stream()
                .map(length -> ImmutableList.of(
                        createRandomBinaryInputProvider(length),
                        createRepeatBinaryInputProvider(length)))
                .flatMap(List::stream)
                .toArray(InputDataProvider[]::new);
    }

    private static InputDataProvider[] generateBoundedVarcharInputs()
    {
        return ImmutableList.of(1, 3, 4, 5, 6).stream()
                .map(length -> ImmutableList.of(
                        createRandomUtf8BinaryInputProvider(length),
                        createRandomAsciiBinaryInputProvider(length)))
                .flatMap(List::stream)
                .toArray(InputDataProvider[]::new);
    }

    private static InputDataProvider[] generateCharInputs()
    {
        return ImmutableList.of(1, 3, 4, 5, 6).stream()
                .map(length -> ImmutableList.of(
                        createRandomUtf8BinaryInputProvider(length),
                        createRandomAsciiBinaryInputProvider(length),
                        createRandomUtf8WithPaddingBinaryInputProvider(length),
                        createRandomAsciiWithPaddingBinaryInputProvider(length)))
                .flatMap(List::stream)
                .toArray(InputDataProvider[]::new);
    }

    private static InputDataProvider createRandomBinaryInputProvider(int length)
    {
        return new InputDataProvider()
        {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                return writeBytes(valuesWriter, randomBinaryData(dataSize, 0, length));
            }

            @Override
            public String toString()
            {
                return "random(0-" + length + ")";
            }
        };
    }

    private static InputDataProvider createRepeatBinaryInputProvider(int length)
    {
        return new InputDataProvider()
        {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(Objects.hash(dataSize, length));
                byte[][] repeatedValues = randomBinaryData(23, 0, length);
                byte[][] values = new byte[dataSize][];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = repeatedValues[random.nextInt(repeatedValues.length)];
                }
                return writeBytes(valuesWriter, values);
            }

            @Override
            public String toString()
            {
                return "repeat(0-" + length + ")";
            }
        };
    }

    private static InputDataProvider createRandomUtf8BinaryInputProvider(int maxLength)
    {
        return createRandomBinaryInputProvider(
                maxLength,
                () -> "random utf8(0-" + maxLength + ")",
                random -> randomUtf8(random, maxLength));
    }

    private static InputDataProvider createRandomUtf8WithPaddingBinaryInputProvider(int maxLength)
    {
        return createRandomBinaryInputProvider(
                maxLength,
                () -> "random utf8 with padding(0-" + maxLength + ")",
                random -> randomUtf8WithPadding(random, maxLength));
    }

    private static InputDataProvider createRandomAsciiWithPaddingBinaryInputProvider(int maxLength)
    {
        return createRandomBinaryInputProvider(
                maxLength,
                () -> "random ascii with padding(0-" + maxLength + ")",
                random -> randomAsciiWithPadding(random, maxLength));
    }

    private static InputDataProvider createRandomAsciiBinaryInputProvider(int length)
    {
        return new InputDataProvider() {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                return writeBytes(valuesWriter, randomAsciiData(dataSize, 0, length));
            }

            @Override
            public String toString()
            {
                return "random ascii(0-" + length + ")";
            }
        };
    }

    private static InputDataProvider createRandomBinaryInputProvider(
            int maxLength,
            Supplier<String> descriptionProvider,
            Function<Random, byte[]> dataProvider)
    {
        return new InputDataProvider() {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(Objects.hash(dataSize, maxLength));
                byte[][] values = new byte[dataSize][];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = dataProvider.apply(random);
                }
                return writeBytes(valuesWriter, values);
            }

            @Override
            public String toString()
            {
                return descriptionProvider.get();
            }
        };
    }

    private static byte[] randomUtf8(Random random, int maxLength)
    {
        StringBuilder builder = new StringBuilder();
        int length = random.nextInt(maxLength + 1);
        for (int i = 0; i < length; i++) {
            builder.append((char) random.nextInt());
        }
        return builder.toString().getBytes(UTF_8);
    }

    private static byte[] randomUtf8WithPadding(Random random, int maxLength)
    {
        StringBuilder builder = new StringBuilder();
        int length = random.nextInt(maxLength + 1);
        for (int i = 0; i < length; i++) {
            builder.append((char) random.nextInt());
        }
        int paddingLength = random.nextInt(maxLength + 1);
        builder.append(" ".repeat(paddingLength));
        return builder.toString().getBytes(UTF_8);
    }

    private static byte[] randomAsciiWithPadding(Random random, int maxLength)
    {
        int length = random.nextInt(maxLength + 1);
        int paddingLength = random.nextInt(maxLength + 1);
        byte[] value = new byte[length + paddingLength];
        for (int i = 0; i < length; i++) {
            value[i] = (byte) random.nextInt(128);
        }
        for (int i = length; i < length + paddingLength; i++) {
            value[i] = ' ';
        }
        return value;
    }

    private static DataBuffer writeBytes(ValuesWriter valuesWriter, byte[][] input)
    {
        for (byte[] value : input) {
            valuesWriter.writeBytes(Binary.fromConstantByteArray(value));
        }

        return getWrittenBuffer(valuesWriter);
    }

    private static final class BoundedVarcharApacheParquetValueDecoder
            implements ValueDecoder<BinaryBuffer>
    {
        private final ValuesReader delegate;
        private final int boundedLength;

        public BoundedVarcharApacheParquetValueDecoder(ValuesReader delegate, VarcharType varcharType)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            checkArgument(
                    !varcharType.isUnbounded(),
                    "Trino type %s is not a bounded varchar",
                    varcharType);
            this.boundedLength = varcharType.getBoundedLength();
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(BinaryBuffer values, int offsetsIndex, int length)
        {
            for (int i = 0; i < length; i++) {
                byte[] value = delegate.readBytes().getBytes();
                Slice slice = Varchars.truncateToLength(Slices.wrappedBuffer(value), boundedLength);
                values.add(slice, i + offsetsIndex);
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    private static final class CharApacheParquetValueDecoder
            implements ValueDecoder<BinaryBuffer>
    {
        private final ValuesReader delegate;
        private final int maxLength;

        public CharApacheParquetValueDecoder(ValuesReader delegate, CharType charType)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.maxLength = charType.getLength();
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(BinaryBuffer values, int offsetsIndex, int length)
        {
            for (int i = 0; i < length; i++) {
                byte[] value = delegate.readBytes().getBytes();
                Slice slice = Chars.trimTrailingSpaces(truncateToLength(Slices.wrappedBuffer(value), maxLength));
                values.add(slice, i + offsetsIndex);
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }
}
