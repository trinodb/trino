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
import static io.trino.parquet.ParquetReaderUtils.toShortExact;
import static io.trino.parquet.reader.TestData.randomInt;
import static io.trino.parquet.reader.TestingValuesWriters.getValuesWriter;
import static io.trino.parquet.reader.flat.ShortColumnAdapter.SHORT_ADAPTER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public final class TestShortValueDecoders
        extends AbstractValueDecodersTest
{
    @Override
    protected Object[][] tests()
    {
        PrimitiveField field = createField(INT32, OptionalInt.empty(), SMALLINT);
        ValueDecoders valueDecoders = new ValueDecoders(field);
        return testArgs(
                new TestType<>(
                        field,
                        valueDecoders::getShortDecoder,
                        ShortApacheParquetValueDecoder::new,
                        SHORT_ADAPTER,
                        (actual, expected) -> assertThat(actual).isEqualTo(expected)),
                ImmutableList.of(PLAIN, RLE_DICTIONARY, DELTA_BINARY_PACKED),
                generateInputDataProviders());
    }

    @Test
    public void testDeltaBinaryPackedWideDeltas()
    {
        assertDeltaDecoded(Short.MIN_VALUE, Short.MAX_VALUE);
        assertDeltaDecoded(Short.MAX_VALUE, Short.MIN_VALUE);
        // a full block of zero deltas followed by a block with a single wide delta, which has its own minimum delta
        short[] values = new short[130];
        Arrays.fill(values, Short.MIN_VALUE);
        values[129] = Short.MAX_VALUE;
        assertDeltaDecoded(values);
        // wide deltas of both signs in one miniblock, so the minimum delta is wider than a short and the bit width is not 0
        assertDeltaDecoded(Short.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE, Short.MIN_VALUE);
        // the padding after the last value can leave the range, with and without a bit width of 0
        assertDeltaDecoded(new short[] {32_000, 32_010, 32_025});
        assertDeltaDecoded(new short[] {32_000, 32_025});
    }

    @Test
    public void testDeltaBinaryPackedValuesOutOfRange()
    {
        assertDeltaOverflow(0, 70_000);
        assertDeltaOverflow(0, 70_000, 2 * 70_000);
        assertDeltaOverflow(0, 30_000, 60_001);
        assertDeltaOverflow(0, 30_000, 60_000);
        assertDeltaOverflow(70_000, 0);
        // the first value after the wide delta fits, the second one cannot
        assertDeltaOverflow(-32768, 32767, 98302);
        // the same wide delta in every miniblock of a block
        assertDeltaOverflow(IntStream.range(0, 129).map(i -> i * 70_000).toArray());
    }

    private static void assertDeltaDecoded(short... expected)
    {
        DataBuffer dataBuffer = writeShorts(getValuesWriter(DELTA_BINARY_PACKED, INT32, OptionalInt.empty()), expected);
        short[] actual = new short[expected.length];
        createDeltaDecoder(dataBuffer).read(actual, 0, expected.length);
        assertThat(actual).containsExactly(expected);
    }

    private static void assertDeltaOverflow(int... values)
    {
        ValuesWriter valuesWriter = getValuesWriter(DELTA_BINARY_PACKED, INT32, OptionalInt.empty());
        for (int value : values) {
            valuesWriter.writeInteger(value);
        }
        ValueDecoder<short[]> decoder = createDeltaDecoder(getWrittenBuffer(valuesWriter));

        assertThatThrownBy(() -> decoder.read(new short[values.length], 0, values.length))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("short overflow");
    }

    private static ValueDecoder<short[]> createDeltaDecoder(DataBuffer dataBuffer)
    {
        PrimitiveField field = createField(INT32, OptionalInt.empty(), SMALLINT);
        ValueDecoder<short[]> decoder = new ValueDecoders(field).getShortDecoder(DELTA_BINARY_PACKED);
        decoder.init(new SimpleSliceInputStream(dataBuffer.dataPage()));
        return decoder;
    }

    private static InputDataProvider[] generateInputDataProviders()
    {
        return Stream.concat(
                        Arrays.stream(ShortInputProvider.values()),
                        IntStream.range(1, 17)
                                .mapToObj(TestShortValueDecoders::createRandomInputDataProvider))
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
                short[] values = new short[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = toShortExact(randomInt(random, bitWidth));
                }
                return writeShorts(valuesWriter, values);
            }

            @Override
            public String toString()
            {
                return "SHORT_RANDOM(" + bitWidth + ")";
            }
        };
    }

    private enum ShortInputProvider
            implements InputDataProvider
    {
        SHORT_SEQUENCE {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                short[] values = new short[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = (short) i;
                }
                return writeShorts(valuesWriter, values);
            }
        },
        SHORT_CONSTANT {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                short[] values = new short[dataSize];
                Arrays.fill(values, (short) 4123);
                return writeShorts(valuesWriter, values);
            }
        },
        SHORT_REPEAT {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                short[] constants = {Short.MIN_VALUE, -4123, 0, 5, 4123, 8956, Short.MAX_VALUE};
                short[] values = new short[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = constants[random.nextInt(constants.length)];
                }
                return writeShorts(valuesWriter, values);
            }
        }
    }

    private static DataBuffer writeShorts(ValuesWriter valuesWriter, short[] input)
    {
        for (short value : input) {
            valuesWriter.writeInteger(value);
        }

        return getWrittenBuffer(valuesWriter);
    }

    private static final class ShortApacheParquetValueDecoder
            implements ValueDecoder<short[]>
    {
        private final ValuesReader delegate;

        public ShortApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(short[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                values[i] = toShortExact(delegate.readInteger());
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }
}
