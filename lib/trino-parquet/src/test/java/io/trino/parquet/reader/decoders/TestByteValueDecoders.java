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
import static io.trino.parquet.ParquetReaderUtils.toByteExact;
import static io.trino.parquet.reader.TestData.randomInt;
import static io.trino.parquet.reader.flat.ByteColumnAdapter.BYTE_ADAPTER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;

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
        return new InputDataProvider() {
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
                byte[] constants = new byte[] {Byte.MIN_VALUE, -1, 0, 5, 17, Byte.MAX_VALUE};
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
