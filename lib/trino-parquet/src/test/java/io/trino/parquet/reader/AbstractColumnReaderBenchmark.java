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
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import org.apache.parquet.column.values.ValuesWriter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.ParquetTypeUtils.getParquetEncoding;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.joda.time.DateTimeZone.UTC;

@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Warmup(iterations = 5, time = 500, timeUnit = MILLISECONDS)
@Fork(2)
public abstract class AbstractColumnReaderBenchmark<VALUES>
{
    // Parquet pages are usually about 1MB
    private static final int MIN_PAGE_SIZE = 1_000_000;
    private static final int OUTPUT_BUFFER_SIZE = MIN_PAGE_SIZE * 2; // Needs to be more than MIN_PAGE_SIZE
    private static final int MAX_VALUES = 1_000_000;

    private static final int DATA_GENERATION_BATCH_SIZE = 16384;
    private static final int READ_BATCH_SIZE = 4096;

    private final ColumnReaderFactory columnReaderFactory = new ColumnReaderFactory(UTC, new ParquetReaderOptions().withBatchColumnReaders(true));
    private final List<DataPage> dataPages = new ArrayList<>();
    private int dataPositions;

    protected PrimitiveField field;

    protected abstract PrimitiveField createPrimitiveField();

    protected abstract ValuesWriter createValuesWriter(int bufferSize);

    protected abstract VALUES generateDataBatch(int size);

    protected abstract void writeValue(ValuesWriter writer, VALUES batch, int index);

    @Setup
    public void setup()
            throws IOException
    {
        this.field = createPrimitiveField();

        ValuesWriter writer = createValuesWriter(OUTPUT_BUFFER_SIZE);
        int batchIndex = 0;
        VALUES batch = generateDataBatch(DATA_GENERATION_BATCH_SIZE);
        while (writer.getBufferedSize() < MIN_PAGE_SIZE && dataPositions < MAX_VALUES) {
            if (batchIndex == DATA_GENERATION_BATCH_SIZE) {
                dataPages.add(createDataPage(writer, batchIndex));
                batch = generateDataBatch(DATA_GENERATION_BATCH_SIZE);
                batchIndex = 0;
            }
            writeValue(writer, batch, batchIndex++);
            dataPositions++;
        }

        if (batchIndex > 0) {
            dataPages.add(createDataPage(writer, batchIndex));
        }
    }

    @Benchmark
    public int read()
            throws IOException
    {
        ColumnReader columnReader = columnReaderFactory.create(field, newSimpleAggregatedMemoryContext());
        columnReader.setPageReader(new PageReader(UNCOMPRESSED, dataPages.iterator(), false, false), Optional.empty());
        int rowsRead = 0;
        while (rowsRead < dataPositions) {
            int remaining = dataPositions - rowsRead;
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
                data.length(),
                OptionalLong.empty(),
                RLE,
                RLE,
                getParquetEncoding(writer.getEncoding()));
    }

    protected static void run(Class<?> clazz)
            throws RunnerException
    {
        benchmark(clazz, WarmupMode.BULK)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgsAppend("-Xmx4g", "-Xms4g"))
                .run();
    }
}
