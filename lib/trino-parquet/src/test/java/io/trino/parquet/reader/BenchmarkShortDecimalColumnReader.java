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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.PrimitiveField;
import io.trino.spi.type.DecimalType;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.OptionalLong;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.reader.TestData.longToBytes;
import static io.trino.parquet.reader.TestData.unscaledRandomShortDecimalSupplier;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.joda.time.DateTimeZone.UTC;

@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Measurement(iterations = 15, time = 500, timeUnit = MILLISECONDS)
@Warmup(iterations = 5, time = 500, timeUnit = MILLISECONDS)
@Fork(2)
public class BenchmarkShortDecimalColumnReader
{
    // Parquet pages are usually about 1MB
    private static final int MIN_PAGE_SIZE = 1_000_000;
    private static final int OUTPUT_BUFFER_SIZE = MIN_PAGE_SIZE * 2; // Needs to be more than MIN_PAGE_SIZE
    private static final int MAX_VALUES = 5_000_000;

    private static final int DATA_GENERATION_BATCH_SIZE = 16384;
    private static final int READ_BATCH_SIZE = 4096;

    private final List<DataPage> dataPages = new ArrayList<>();

    @Param({
            "1", "2", "3", "4", "5", "6", "7", "8",
    })
    private int byteArrayLength;
    private PrimitiveField field;

    public BenchmarkShortDecimalColumnReader() {}

    public BenchmarkShortDecimalColumnReader(int byteArrayLength)
    {
        this.byteArrayLength = byteArrayLength;
    }

    @Setup
    public void setup()
            throws IOException
    {
        int precision = maxPrecision(byteArrayLength);
        PrimitiveType parquetType = Types.optional(FIXED_LEN_BYTE_ARRAY)
                .length(byteArrayLength)
                .as(LogicalTypeAnnotation.decimalType(0, precision))
                .named("name");
        this.field = new PrimitiveField(
                DecimalType.createDecimalType(precision),
                true,
                new ColumnDescriptor(new String[] {}, parquetType, 0, 0),
                0);

        ValuesWriter writer = createValuesWriter(OUTPUT_BUFFER_SIZE);
        int positions = 0;
        int batchIndex = 0;
        long[] batch = generateDataBatch(DATA_GENERATION_BATCH_SIZE, precision);
        while (writer.getBufferedSize() < MIN_PAGE_SIZE && positions < MAX_VALUES) {
            if (batchIndex == DATA_GENERATION_BATCH_SIZE) {
                dataPages.add(createDataPage(writer, batchIndex));
                batch = generateDataBatch(DATA_GENERATION_BATCH_SIZE, precision);
                batchIndex = 0;
            }
            writeValue(writer, batch, batchIndex++);
            positions++;
        }

        if (batchIndex > 0) {
            dataPages.add(createDataPage(writer, batchIndex));
        }
    }

    @Benchmark
    public int read()
            throws IOException
    {
        PrimitiveColumnReader columnReader = PrimitiveColumnReader.createReader(field, UTC);
        columnReader.setPageReader(new PageReader(UNCOMPRESSED, new LinkedList<>(dataPages), null, MAX_VALUES), null);
        int rowsRead = 0;
        while (rowsRead < MAX_VALUES) {
            int remaining = MAX_VALUES - rowsRead;
            columnReader.prepareNextRead(Math.min(READ_BATCH_SIZE, remaining));
            rowsRead += columnReader.readPrimitive().getBlock().getPositionCount();
        }
        return rowsRead;
    }

    private DataPage createDataPage(ValuesWriter writer, int valuesCount)
    {
        Slice data;
        try {
            data = Slices.wrappedBuffer(writer.getBytes().toByteArray());
            writer.reset();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new DataPageV1(
                data,
                valuesCount,
                byteArrayLength * valuesCount,
                OptionalLong.empty(),
                RLE,
                RLE,
                PLAIN);
    }

    private ValuesWriter createValuesWriter(int bufferSize)
    {
        return new FixedLenByteArrayPlainValuesWriter(byteArrayLength, bufferSize, bufferSize, HeapByteBufferAllocator.getInstance());
    }

    private void writeValue(ValuesWriter writer, long[] batch, int index)
    {
        Binary binary = Binary.fromConstantByteArray(longToBytes(batch[index], byteArrayLength));
        writer.writeBytes(binary);
    }

    private long[] generateDataBatch(int size, int precision)
    {
        return unscaledRandomShortDecimalSupplier(byteArrayLength * Byte.SIZE, precision).apply(size);
    }

    private static int maxPrecision(int numBytes)
    {
        return toIntExact(
                // convert double to long
                Math.round(
                        // number of base-10 digits
                        Math.floor(Math.log10(
                                Math.pow(2, 8 * numBytes - 1) - 1))));  // max value stored in numBytes
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkShortDecimalColumnReader.class, WarmupMode.BULK)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
