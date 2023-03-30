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
import static io.trino.parquet.reader.TestData.randomInt;
import static io.trino.parquet.reader.flat.IntColumnAdapter.INT_ADAPTER;
import static io.trino.parquet.reader.flat.LongColumnAdapter.LONG_ADAPTER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.DataProviders.concat;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestIntValueDecoders
        extends AbstractValueDecodersTest
{
    @Override
    protected Object[][] tests()
    {
        return concat(
                testArgs(
                        new TestType<>(
                                createField(INT32, OptionalInt.empty(), INTEGER),
                                ValueDecoders::getIntDecoder,
                                IntApacheParquetValueDecoder::new,
                                INT_ADAPTER,
                                (actual, expected) -> assertThat(actual).isEqualTo(expected)),
                        ImmutableList.of(PLAIN, RLE_DICTIONARY, DELTA_BINARY_PACKED),
                        generateInputDataProviders()),
                testArgs(
                        new TestType<>(
                                createField(INT32, OptionalInt.empty(), BIGINT),
                                TransformingValueDecoders::getInt32ToLongDecoder,
                                IntToLongApacheParquetValueDecoder::new,
                                LONG_ADAPTER,
                                (actual, expected) -> assertThat(actual).isEqualTo(expected)),
                        ImmutableList.of(PLAIN, RLE_DICTIONARY, DELTA_BINARY_PACKED),
                        generateInputDataProviders()));
    }

    private static InputDataProvider[] generateInputDataProviders()
    {
        return Stream.concat(
                        Arrays.stream(IntInputProvider.values()),
                        IntStream.range(1, 33)
                                .mapToObj(TestIntValueDecoders::createRandomInputDataProvider))
                .toArray(InputDataProvider[]::new);
    }

    private static InputDataProvider createRandomInputDataProvider(int bitWidth)
    {
        return new InputDataProvider() {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(123L * bitWidth * dataSize);
                int[] values = new int[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = randomInt(random, bitWidth);
                }
                return writeInts(valuesWriter, values);
            }

            @Override
            public String toString()
            {
                return "INT_RANDOM(" + bitWidth + ")";
            }
        };
    }

    private enum IntInputProvider
            implements InputDataProvider
    {
        INT_SEQUENCE {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                int[] values = new int[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = i;
                }
                return writeInts(valuesWriter, values);
            }
        },
        INT_CONSTANT {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                int[] values = new int[dataSize];
                Arrays.fill(values, 1412341234);
                return writeInts(valuesWriter, values);
            }
        },
        INT_REPEAT {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                int[] constants = new int[] {Integer.MIN_VALUE, -1412341234, 0, 5, 4123, 1412341234, Integer.MAX_VALUE};
                int[] values = new int[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = constants[random.nextInt(constants.length)];
                }
                return writeInts(valuesWriter, values);
            }
        }
    }

    private static DataBuffer writeInts(ValuesWriter valuesWriter, int[] input)
    {
        for (int value : input) {
            valuesWriter.writeInteger(value);
        }

        return getWrittenBuffer(valuesWriter);
    }

    private static final class IntApacheParquetValueDecoder
            implements ValueDecoder<int[]>
    {
        private final ValuesReader delegate;

        public IntApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(int[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                values[i] = delegate.readInteger();
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    private static final class IntToLongApacheParquetValueDecoder
            implements ValueDecoder<long[]>
    {
        private final ValuesReader delegate;

        public IntToLongApacheParquetValueDecoder(ValuesReader delegate)
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
            for (int i = offset; i < offset + length; i++) {
                values[i] = delegate.readInteger();
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }
}
