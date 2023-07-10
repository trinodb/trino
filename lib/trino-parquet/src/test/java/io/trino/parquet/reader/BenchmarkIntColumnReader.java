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
package io.trino.parquet.reader;

import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.PrimitiveField;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.openjdk.jmh.annotations.Param;

import java.util.Random;

import static io.trino.parquet.ParquetEncoding.DELTA_BINARY_PACKED;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.reader.TestData.randomInt;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

public class BenchmarkIntColumnReader
        extends AbstractColumnReaderBenchmark<int[]>
{
    private static final Random RANDOM = new Random(23423523L);

    @Param({
            "0", "1", "2", "3", "4", "5", "6", "7", "8",
            "11", "15", "20", "25", "32"
    })
    public int bitWidth;

    @Param({
            "PLAIN",
            "DELTA_BINARY_PACKED",
    })
    public ParquetEncoding encoding;

    @Override
    protected PrimitiveField createPrimitiveField()
    {
        PrimitiveType parquetType = Types.optional(INT32)
                .named("name");
        return new PrimitiveField(
                INTEGER,
                true,
                new ColumnDescriptor(new String[] {"test"}, parquetType, 0, 0),
                0);
    }

    @Override
    protected ValuesWriter createValuesWriter(int bufferSize)
    {
        if (encoding == PLAIN) {
            return new PlainValuesWriter(bufferSize, bufferSize, HeapByteBufferAllocator.getInstance());
        }
        else if (encoding == DELTA_BINARY_PACKED) {
            return new DeltaBinaryPackingValuesWriterForInteger(bufferSize, bufferSize, HeapByteBufferAllocator.getInstance());
        }
        throw new UnsupportedOperationException(format("encoding %s is not supported", encoding));
    }

    @Override
    protected void writeValue(ValuesWriter writer, int[] batch, int index)
    {
        writer.writeInteger(batch[index]);
    }

    @Override
    protected int[] generateDataBatch(int size)
    {
        int[] batch = new int[size];
        if (bitWidth == 0) {
            for (int i = 0; i < size; i++) {
                batch[i] = i;
            }
        }
        else {
            for (int i = 0; i < size; i++) {
                batch[i] = randomInt(RANDOM, bitWidth);
            }
        }
        return batch;
    }

    public static void main(String[] args)
            throws Exception
    {
        run(BenchmarkIntColumnReader.class);
    }
}
