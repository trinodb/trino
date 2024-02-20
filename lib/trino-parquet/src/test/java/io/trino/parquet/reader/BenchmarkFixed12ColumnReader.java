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

import io.trino.parquet.PrimitiveField;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.time.LocalDateTime;
import java.time.Year;
import java.util.Random;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.parquet.reader.TestingColumnReader.encodeInt96Timestamp;
import static io.trino.spi.block.Fixed12Block.decodeFixed12First;
import static io.trino.spi.block.Fixed12Block.decodeFixed12Second;
import static io.trino.spi.block.Fixed12Block.encodeFixed12;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

public class BenchmarkFixed12ColumnReader
        extends AbstractColumnReaderBenchmark<int[]>
{
    private static final int LENGTH = SIZE_OF_LONG + SIZE_OF_INT;

    private final Random random = new Random(56246);

    @Override
    protected PrimitiveField createPrimitiveField()
    {
        PrimitiveType parquetType = Types.optional(INT96).named("name");
        return new PrimitiveField(
                TIMESTAMP_NANOS,
                true,
                new ColumnDescriptor(new String[] {"test"}, parquetType, 0, 0),
                0);
    }

    @Override
    protected ValuesWriter createValuesWriter(int bufferSize)
    {
        return new FixedLenByteArrayPlainValuesWriter(LENGTH, bufferSize, bufferSize, HeapByteBufferAllocator.getInstance());
    }

    @Override
    protected void writeValue(ValuesWriter writer, int[] batch, int index)
    {
        writer.writeBytes(encodeInt96Timestamp(decodeFixed12First(batch, index), decodeFixed12Second(batch, index)));
    }

    @Override
    protected int[] generateDataBatch(int size)
    {
        int[] batch = new int[size * 3];
        for (int i = 0; i < size; i++) {
            LocalDateTime timestamp = LocalDateTime.of(
                    random.nextInt(Year.MIN_VALUE, Year.MAX_VALUE + 1),
                    random.nextInt(1, 13),
                    random.nextInt(1, 29),
                    random.nextInt(24),
                    random.nextInt(60),
                    random.nextInt(60));
            encodeFixed12(timestamp.toEpochSecond(UTC), timestamp.get(NANO_OF_SECOND), batch, i);
        }
        return batch;
    }

    public static void main(String[] args)
            throws Exception
    {
        run(BenchmarkFixed12ColumnReader.class);
    }
}
