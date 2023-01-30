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
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.IntApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.IntToLongApacheParquetValueDecoder;
import static io.trino.parquet.reader.flat.IntColumnAdapter.INT_ADAPTER;
import static io.trino.parquet.reader.flat.LongColumnAdapter.LONG_ADAPTER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.DataProviders.concat;
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
                        ImmutableList.of(PLAIN, RLE_DICTIONARY),
                        IntInputProvider.values()),
                testArgs(
                        new TestType<>(
                                createField(INT32, OptionalInt.empty(), BIGINT),
                                ValueDecoders::getIntToLongDecoder,
                                IntToLongApacheParquetValueDecoder::new,
                                LONG_ADAPTER,
                                (actual, expected) -> assertThat(actual).isEqualTo(expected)),
                        ImmutableList.of(PLAIN, RLE_DICTIONARY),
                        IntInputProvider.values()));
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
        },
        INT_RANDOM {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                int[] values = new int[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = randomInt(random, 32);
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
}
