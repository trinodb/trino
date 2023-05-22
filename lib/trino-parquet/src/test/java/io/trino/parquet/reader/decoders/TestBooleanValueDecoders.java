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
import io.trino.spi.type.BooleanType;
import org.apache.parquet.column.values.ValuesWriter;

import java.util.Arrays;
import java.util.OptionalInt;
import java.util.Random;

import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.reader.TestData.generateMixedData;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.BooleanApacheParquetValueDecoder;
import static io.trino.parquet.reader.flat.ByteColumnAdapter.BYTE_ADAPTER;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestBooleanValueDecoders
        extends AbstractValueDecodersTest
{
    @Override
    protected Object[][] tests()
    {
        PrimitiveField field = createField(BOOLEAN, OptionalInt.empty(), BooleanType.BOOLEAN);
        ValueDecoders valueDecoders = new ValueDecoders(field);
        return testArgs(
                new TestType<>(
                        field,
                        valueDecoders::getBooleanDecoder,
                        BooleanApacheParquetValueDecoder::new,
                        BYTE_ADAPTER,
                        (actual, expected) -> assertThat(actual).isEqualTo(expected)),
                ImmutableList.of(PLAIN, RLE),
                BooleanInputProvider.values());
    }

    private enum BooleanInputProvider
            implements InputDataProvider
    {
        BOOLEAN_ONLY_TRUE {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                boolean[] values = new boolean[dataSize];
                Arrays.fill(values, true);
                return writeBooleans(valuesWriter, values);
            }
        },
        BOOLEAN_ONLY_FALSE {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                return writeBooleans(valuesWriter, new boolean[dataSize]);
            }
        },
        BOOLEAN_RANDOM {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                boolean[] values = new boolean[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    values[i] = random.nextBoolean();
                }
                return writeBooleans(valuesWriter, values);
            }
        },
        BOOLEAN_MIXED_AND_GROUPS_SMALL {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                return writeMixedAndGroupedBooleans(valuesWriter, dataSize / 3, dataSize);
            }
        },
        BOOLEAN_MIXED_AND_GROUPS_LARGE {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                return writeMixedAndGroupedBooleans(valuesWriter, 127, dataSize);
            }
        },
        BOOLEAN_MIXED_AND_GROUPS_HUGE {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                return writeMixedAndGroupedBooleans(valuesWriter, 2111, dataSize);
            }
        }
    }

    private static DataBuffer writeMixedAndGroupedBooleans(ValuesWriter valuesWriter, int maxGroupSize, int dataSize)
    {
        Random random = new Random((long) maxGroupSize * dataSize);
        boolean[] values = generateMixedData(random, dataSize, maxGroupSize);
        return writeBooleans(valuesWriter, values);
    }

    private static DataBuffer writeBooleans(ValuesWriter valuesWriter, boolean[] input)
    {
        for (boolean value : input) {
            valuesWriter.writeBoolean(value);
        }

        return getWrittenBuffer(valuesWriter);
    }
}
