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
import io.trino.spi.type.DoubleType;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;

import java.util.OptionalInt;
import java.util.Random;

import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.reader.flat.LongColumnAdapter.LONG_ADAPTER;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestDoubleValueDecoders
        extends AbstractValueDecodersTest
{
    @Override
    protected Object[][] tests()
    {
        return testArgs(
                new TestType<>(
                        createField(DOUBLE, OptionalInt.empty(), DoubleType.DOUBLE),
                        ValueDecoders::getDoubleDecoder,
                        DoubleApacheParquetValueDecoder::new,
                        LONG_ADAPTER,
                        (actual, expected) -> assertThat(actual).isEqualTo(expected)),
                ImmutableList.of(PLAIN, RLE_DICTIONARY),
                DoubleInputProvider.values());
    }

    private enum DoubleInputProvider
            implements InputDataProvider
    {
        DOUBLE_SEQUENCE {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                double[] values = new double[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = ((double) i) / 10;
                }
                return writeDoubles(valuesWriter, values);
            }
        },
        DOUBLE_RANDOM_WITH_NAN_AND_INF {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                double[] values = new double[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = Double.longBitsToDouble(random.nextLong());
                }
                return writeDoubles(valuesWriter, values);
            }
        },
        DOUBLE_RANDOM {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                double[] values = new double[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = random.nextDouble();
                }
                return writeDoubles(valuesWriter, values);
            }
        },
        DOUBLE_REPEAT {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                double[] values = new double[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = ((double) (i % 101)) / 10;
                }
                return writeDoubles(valuesWriter, values);
            }
        }
    }

    private static DataBuffer writeDoubles(ValuesWriter valuesWriter, double[] input)
    {
        for (double value : input) {
            valuesWriter.writeDouble(value);
        }

        return getWrittenBuffer(valuesWriter);
    }

    private static final class DoubleApacheParquetValueDecoder
            implements ValueDecoder<long[]>
    {
        private final ValuesReader delegate;

        public DoubleApacheParquetValueDecoder(ValuesReader delegate)
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
                values[i] = Double.doubleToLongBits(delegate.readDouble());
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }
}
