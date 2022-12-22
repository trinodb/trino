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
import org.apache.parquet.column.values.ValuesWriter;

import java.util.OptionalInt;
import java.util.Random;

import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.reader.TestData.randomInt;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.ShortApacheParquetValueDecoder;
import static io.trino.parquet.reader.flat.ShortColumnAdapter.SHORT_ADAPTER;
import static io.trino.spi.type.SmallintType.SMALLINT;
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
                ImmutableList.of(PLAIN, RLE_DICTIONARY),
                ShortInputProvider.values());
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
        },
        SHORT_RANDOM {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                short[] values = new short[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = (short) randomInt(random, 16);
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
}
