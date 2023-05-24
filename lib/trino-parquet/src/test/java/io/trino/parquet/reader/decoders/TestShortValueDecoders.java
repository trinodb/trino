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
import io.trino.parquet.reader.SimpleSliceInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;

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
import static io.trino.parquet.reader.flat.ShortColumnAdapter.SHORT_ADAPTER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestShortValueDecoders
        extends AbstractValueDecodersTest
{
    @Override
    protected Object[][] tests()
    {
        return testArgs(
                new TestType<>(
                        createField(INT32, OptionalInt.empty(), SMALLINT),
                        ValueDecoders::getShortDecoder,
                        ShortApacheParquetValueDecoder::new,
                        SHORT_ADAPTER,
                        (actual, expected) -> assertThat(actual).isEqualTo(expected)),
                ImmutableList.of(PLAIN, RLE_DICTIONARY, DELTA_BINARY_PACKED),
                generateInputDataProviders());
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
        return new InputDataProvider() {
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
                short[] constants = new short[] {Short.MIN_VALUE, -4123, 0, 5, 4123, 8956, Short.MAX_VALUE};
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
