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

import java.util.Arrays;
import java.util.OptionalInt;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.trino.parquet.ParquetEncoding.DELTA_BINARY_PACKED;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.reader.TestData.randomLong;
import static io.trino.parquet.reader.flat.LongColumnAdapter.LONG_ADAPTER;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestLongValueDecoders
        extends AbstractValueDecodersTest
{
    @Override
    protected Object[][] tests()
    {
        PrimitiveField field = createField(INT64, OptionalInt.empty(), BIGINT);
        ValueDecoders valueDecoders = new ValueDecoders(field);
        return testArgs(
                new TestType<>(
                        field,
                        valueDecoders::getLongDecoder,
                        LongApacheParquetValueDecoder::new,
                        LONG_ADAPTER,
                        (actual, expected) -> assertThat(actual).isEqualTo(expected)),
                ImmutableList.of(PLAIN, RLE_DICTIONARY, DELTA_BINARY_PACKED),
                generateInputDataProviders());
    }

    private static InputDataProvider[] generateInputDataProviders()
    {
        return Stream.concat(
                        Arrays.stream(LongInputProvider.values()),
                        IntStream.range(1, 65)
                                .mapToObj(TestLongValueDecoders::createRandomInputDataProvider))
                .toArray(InputDataProvider[]::new);
    }

    private static InputDataProvider createRandomInputDataProvider(int bitWidth)
    {
        return new InputDataProvider() {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(123L * bitWidth * dataSize);
                long[] values = new long[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = randomLong(random, bitWidth);
                }
                return writeLongs(valuesWriter, values);
            }

            @Override
            public String toString()
            {
                return "LONG_RANDOM(" + bitWidth + ")";
            }
        };
    }

    private enum LongInputProvider
            implements InputDataProvider
    {
        LONG_SEQUENCE {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                long[] values = new long[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = i;
                }
                return writeLongs(valuesWriter, values);
            }
        },
        LONG_CONSTANT {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                long[] values = new long[dataSize];
                Arrays.fill(values, 111222333444555L);
                return writeLongs(valuesWriter, values);
            }
        },
        LONG_REPEAT {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                long[] constants = new long[] {Long.MIN_VALUE, -111222333444555L, -1412341234L, 0, 4123, 1412341234L, 111222333444555L, Long.MAX_VALUE};
                long[] values = new long[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = constants[random.nextInt(constants.length)];
                }
                return writeLongs(valuesWriter, values);
            }
        }
    }

    private static DataBuffer writeLongs(ValuesWriter valuesWriter, long[] input)
    {
        for (long value : input) {
            valuesWriter.writeLong(value);
        }

        return getWrittenBuffer(valuesWriter);
    }

    private static final class LongApacheParquetValueDecoder
            implements ValueDecoder<long[]>
    {
        private final ValuesReader delegate;

        public LongApacheParquetValueDecoder(ValuesReader delegate)
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
                values[i] = delegate.readLong();
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }
}
