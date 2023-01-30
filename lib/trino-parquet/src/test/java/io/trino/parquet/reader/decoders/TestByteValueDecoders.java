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
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.ByteApacheParquetValueDecoder;
import static io.trino.parquet.reader.flat.ByteColumnAdapter.BYTE_ADAPTER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestByteValueDecoders
        extends AbstractValueDecodersTest
{
    @Override
    protected Object[][] tests()
    {
        return testArgs(
                new TestType<>(
                        createField(INT32, OptionalInt.empty(), TINYINT),
                        ValueDecoders::getByteDecoder,
                        ByteApacheParquetValueDecoder::new,
                        BYTE_ADAPTER,
                        (actual, expected) -> assertThat(actual).isEqualTo(expected)),
                ImmutableList.of(PLAIN, RLE_DICTIONARY),
                ByteInputProvider.values());
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
        },
        BYTE_RANDOM {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                byte[] values = new byte[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = (byte) randomInt(random, 8);
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
}
