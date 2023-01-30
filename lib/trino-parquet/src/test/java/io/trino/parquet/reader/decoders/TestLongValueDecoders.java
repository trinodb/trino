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
import static io.trino.parquet.reader.TestData.randomLong;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.LongApacheParquetValueDecoder;
import static io.trino.parquet.reader.flat.LongColumnAdapter.LONG_ADAPTER;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestLongValueDecoders
        extends AbstractValueDecodersTest
{
    @Override
    protected Object[][] tests()
    {
        return testArgs(
                        new TestType<>(
                                createField(INT64, OptionalInt.empty(), BIGINT),
                                ValueDecoders::getLongDecoder,
                                LongApacheParquetValueDecoder::new,
                                LONG_ADAPTER,
                                (actual, expected) -> assertThat(actual).isEqualTo(expected)),
                        ImmutableList.of(PLAIN, RLE_DICTIONARY),
                        LongInputProvider.values());
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
        },
        LONG_RANDOM {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                long[] values = new long[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = randomLong(random, 64);
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
}
