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
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.SimpleSliceInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.OptionalInt;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.trino.parquet.ParquetEncoding.DELTA_BINARY_PACKED;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.ParquetReaderUtils.toByteExact;
import static io.trino.parquet.reader.TestData.randomInt;
import static io.trino.parquet.reader.TestingValuesWriters.getValuesWriter;
import static io.trino.parquet.reader.flat.ByteColumnAdapter.BYTE_ADAPTER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public final class TestByteValueDecoders
        extends AbstractValueDecodersTest
{
    @Override
    protected Object[][] tests()
    {
        PrimitiveField field = createField(INT32, OptionalInt.empty(), TINYINT);
        ValueDecoders valueDecoders = new ValueDecoders(field);
        return testArgs(
                new TestType<>(
                        field,
                        valueDecoders::getByteDecoder,
                        ByteApacheParquetValueDecoder::new,
                        BYTE_ADAPTER,
                        (actual, expected) -> assertThat(actual).isEqualTo(expected)),
                ImmutableList.of(PLAIN, RLE_DICTIONARY, DELTA_BINARY_PACKED),
                generateInputDataProviders());
    }

    @Test
    public void testDeltaBinaryPackedWideDeltas()
    {
        assertDeltaDecoded(Byte.MIN_VALUE, Byte.MAX_VALUE);
        assertDeltaDecoded(Byte.MAX_VALUE, Byte.MIN_VALUE);
        // a full block of zero deltas followed by a block with a single wide delta, which has its own minimum delta
        byte[] values = new byte[130];
        Arrays.fill(values, Byte.MIN_VALUE);
        values[129] = Byte.MAX_VALUE;
        assertDeltaDecoded(values);
        // wide deltas of both signs in one miniblock, so the minimum delta is wider than a byte and the bit width is not 0
        assertDeltaDecoded(Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MAX_VALUE, Byte.MIN_VALUE);
        // the padding after the last value can leave the range, with and without a bit width of 0
        assertDeltaDecoded(new byte[] {100, 110, 125});
        assertDeltaDecoded(new byte[] {100, 125});
    }

    @Test
    public void testDeltaBinaryPackedValuesOutOfRange()
    {
        assertDeltaOverflow(0, 300);
        assertDeltaOverflow(0, 300, 2 * 300);
        assertDeltaOverflow(0, 100, 201);
        assertDeltaOverflow(0, 100, 200);
        assertDeltaOverflow(300, 100);
        // the first value after the wide delta fits, the second one cannot
        assertDeltaOverflow(-128, 127, 382);
        // the same wide delta in every miniblock of a block
        assertDeltaOverflow(IntStream.range(0, 129).map(i -> i * 300).toArray());
    }

    private static void assertDeltaDecoded(byte... expected)
    {
        DataBuffer dataBuffer = writeBytes(getValuesWriter(DELTA_BINARY_PACKED, INT32, OptionalInt.empty()), expected);
        byte[] actual = new byte[expected.length];
        createDeltaDecoder(dataBuffer).read(actual, 0, expected.length);
        assertThat(actual).containsExactly(expected);
    }

    private static void assertDeltaOverflow(int... values)
    {
        ValuesWriter valuesWriter = getValuesWriter(DELTA_BINARY_PACKED, INT32, OptionalInt.empty());
        for (int value : values) {
            valuesWriter.writeInteger(value);
        }
        ValueDecoder<byte[]> decoder = createDeltaDecoder(getWrittenBuffer(valuesWriter));

        assertThatThrownBy(() -> decoder.read(new byte[values.length], 0, values.length))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("byte overflow");
    }

    private static ValueDecoder<byte[]> createDeltaDecoder(DataBuffer dataBuffer)
    {
        PrimitiveField field = createField(INT32, OptionalInt.empty(), TINYINT);
        ValueDecoder<byte[]> decoder = new ValueDecoders(field).getByteDecoder(DELTA_BINARY_PACKED);
        decoder.init(new SimpleSliceInputStream(dataBuffer.dataPage()));
        return decoder;
    }

    private static InputDataProvider[] generateInputDataProviders()
    {
        return Stream.concat(
                        Arrays.stream(ByteInputProvider.values()),
                        IntStream.range(1, 9)
                                .mapToObj(TestByteValueDecoders::createRandomInputDataProvider))
                .toArray(InputDataProvider[]::new);
    }

    private static InputDataProvider createRandomInputDataProvider(int bitWidth)
    {
        return new InputDataProvider()
        {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(123L * bitWidth * dataSize);
                byte[] values = new byte[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = toByteExact(randomInt(random, bitWidth));
                }
                return writeBytes(valuesWriter, values);
            }

            @Override
            public String toString()
            {
                return "BYTE_RANDOM(" + bitWidth + ")";
            }
        };
    }

    private enum ByteInputProvider
            implements InputDataProvider
    {
        BYTE_SEQUENCE {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                byte[] values = new byte[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = (byte) i;
                }
                return writeBytes(valuesWriter, values);
            }
        },
        BYTE_CONSTANT {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                byte[] values = new byte[dataSize];
                Arrays.fill(values, (byte) 123);
                return writeBytes(valuesWriter, values);
            }
        },
        BYTE_REPEAT {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                byte[] constants = {Byte.MIN_VALUE, -1, 0, 5, 17, Byte.MAX_VALUE};
                byte[] values = new byte[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = constants[random.nextInt(constants.length)];
                }
                return writeBytes(valuesWriter, values);
            }
        }
    }

    private static DataBuffer writeBytes(ValuesWriter valuesWriter, byte[] input)
    {
        for (byte value : input) {
            valuesWriter.writeInteger(value);
        }

        return getWrittenBuffer(valuesWriter);
    }

    private static final class ByteApacheParquetValueDecoder
            implements ValueDecoder<byte[]>
    {
        private final ValuesReader delegate;

        public ByteApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(byte[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                values[i] = toByteExact(delegate.readInteger());
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }
}
