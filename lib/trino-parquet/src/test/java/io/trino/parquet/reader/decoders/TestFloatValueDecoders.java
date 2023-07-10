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

import java.util.OptionalInt;
import java.util.Random;

import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.reader.flat.IntColumnAdapter.INT_ADAPTER;
import static io.trino.spi.type.RealType.REAL;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestFloatValueDecoders
        extends AbstractValueDecodersTest
{
    @Override
    protected Object[][] tests()
    {
        PrimitiveField field = createField(FLOAT, OptionalInt.empty(), REAL);
        ValueDecoders valueDecoders = new ValueDecoders(field);
        return testArgs(
                new TestType<>(
                        field,
                        valueDecoders::getRealDecoder,
                        FloatApacheParquetValueDecoder::new,
                        INT_ADAPTER,
                        (actual, expected) -> assertThat(actual).isEqualTo(expected)),
                ImmutableList.of(PLAIN, RLE_DICTIONARY),
                FloatInputProvider.values());
    }

    private enum FloatInputProvider
            implements InputDataProvider
    {
        FLOAT_SEQUENCE {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                float[] values = new float[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = ((float) i) / 10;
                }
                return writeFloats(valuesWriter, values);
            }
        },
        FLOAT_RANDOM_WITH_NAN_AND_INF {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                float[] values = new float[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = Float.intBitsToFloat(random.nextInt());
                }
                return writeFloats(valuesWriter, values);
            }
        },
        FLOAT_RANDOM {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                float[] values = new float[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = random.nextFloat();
                }
                return writeFloats(valuesWriter, values);
            }
        },
        FLOAT_REPEAT {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                float[] values = new float[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = ((float) (i % 101)) / 10;
                }
                return writeFloats(valuesWriter, values);
            }
        }
    }

    private static DataBuffer writeFloats(ValuesWriter valuesWriter, float[] input)
    {
        for (float value : input) {
            valuesWriter.writeFloat(value);
        }

        return getWrittenBuffer(valuesWriter);
    }

    private static final class FloatApacheParquetValueDecoder
            implements ValueDecoder<int[]>
    {
        private final ValuesReader delegate;

        public FloatApacheParquetValueDecoder(ValuesReader delegate)
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
                values[i] = Float.floatToIntBits(delegate.readFloat());
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }
}
