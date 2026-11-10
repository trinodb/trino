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

import com.google.common.primitives.Bytes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.TestingColumnReader.DataPageVersion;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.DevNullValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
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
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.joda.time.DateTimeZone.UTC;

@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Warmup(iterations = 5, time = 500, timeUnit = MILLISECONDS)
@Fork(2)
public class BenchmarkNestedColumnReader
{
    private static final int VALUES_PER_PAGE = 16384;
    private static final int PAGES = 64;
    private static final int READ_BATCH_SIZE = 4096;
    private static final int ARRAY_LENGTH = 4;

    public enum Shape
    {
        // optional group a { optional int32 b }
        STRUCT_FIELD(0, 2, 1),
        // optional group a (LIST) { repeated group list { optional int32 element } }
        ARRAY_ELEMENT(1, 3, ARRAY_LENGTH);

        private final int maxRepetitionLevel;
        private final int maxDefinitionLevel;
        private final int valuesPerRow;

        Shape(int maxRepetitionLevel, int maxDefinitionLevel, int valuesPerRow)
        {
            this.maxRepetitionLevel = maxRepetitionLevel;
            this.maxDefinitionLevel = maxDefinitionLevel;
            this.valuesPerRow = valuesPerRow;
        }
    }

    @Param
    public Shape shape;

    @Param({"0.0", "0.1"})
    public double nullFraction;

    @Param
    public DataPageVersion pageVersion;

    private final ColumnReaderFactory columnReaderFactory = new ColumnReaderFactory(UTC, ParquetReaderOptions.defaultOptions());
    private final List<DataPage> dataPages = new ArrayList<>();
    private PrimitiveField field;

    @Setup
    public void setup()
            throws IOException
    {
        PrimitiveType parquetType = Types.optional(INT32).named("b");
        field = new PrimitiveField(
                INTEGER,
                false,
                // Two path elements force the nested reader
                new ColumnDescriptor(new String[] {"a", "b"}, parquetType, shape.maxRepetitionLevel, shape.maxDefinitionLevel),
                0);

        Random random = new Random(23423523L);
        for (int page = 0; page < PAGES; page++) {
            dataPages.add(createPage(random));
        }
    }

    @Benchmark
    public int read()
    {
        ColumnReader columnReader = columnReaderFactory.create(field, newSimpleAggregatedMemoryContext());
        PageReader pageReader = new PageReader(new ParquetDataSourceId("test"), UNCOMPRESSED, dataPages.iterator(), false, false, Optional.empty(), -1, -1);
        columnReader.setPageReader(pageReader, Optional.empty());
        int rows = totalRows();
        int rowsRead = 0;
        int valuesRead = 0;
        while (rowsRead < rows) {
            int batchSize = Math.min(READ_BATCH_SIZE, rows - rowsRead);
            columnReader.prepareNextRead(batchSize);
            valuesRead += columnReader.readPrimitive().getBlock().getPositionCount();
            rowsRead += batchSize;
        }
        return valuesRead;
    }

    private int totalRows()
    {
        // Every page holds whole rows
        return PAGES * VALUES_PER_PAGE / shape.valuesPerRow;
    }

    private DataPage createPage(Random random)
            throws IOException
    {
        int maxDef = shape.maxDefinitionLevel;
        ValuesWriter valuesWriter = new PlainValuesWriter(VALUES_PER_PAGE * Integer.BYTES, VALUES_PER_PAGE * Integer.BYTES, HeapByteBufferAllocator.getInstance());
        ValuesWriter definitionWriter = levelsWriter(maxDef);
        ValuesWriter repetitionWriter = levelsWriter(shape.maxRepetitionLevel);
        int nullCount = 0;
        for (int i = 0; i < VALUES_PER_PAGE; i++) {
            if (random.nextDouble() < nullFraction) {
                definitionWriter.writeInteger(maxDef - 1);
                nullCount++;
            }
            else {
                definitionWriter.writeInteger(maxDef);
                valuesWriter.writeInteger(random.nextInt());
            }
            if (shape.maxRepetitionLevel > 0) {
                repetitionWriter.writeInteger(i % ARRAY_LENGTH == 0 ? 0 : 1);
            }
        }
        byte[] repetitionBytes = repetitionWriter.getBytes().toByteArray();
        byte[] definitionBytes = definitionWriter.getBytes().toByteArray();
        byte[] valueBytes = valuesWriter.getBytes().toByteArray();

        if (pageVersion == DataPageVersion.V1) {
            Slice slice = Slices.wrappedBuffer(Bytes.concat(repetitionBytes, definitionBytes, valueBytes));
            return new DataPageV1(slice, VALUES_PER_PAGE, slice.length(), OptionalLong.empty(), RLE, RLE, PLAIN, 0);
        }
        // Levels are written without the 4 byte length prefix in V2 pages
        return new DataPageV2(
                VALUES_PER_PAGE / shape.valuesPerRow,
                nullCount,
                VALUES_PER_PAGE,
                repetitionBytes.length == 0 ? EMPTY_SLICE : Slices.wrappedBuffer(repetitionBytes, Integer.BYTES, repetitionBytes.length - Integer.BYTES),
                definitionBytes.length == 0 ? EMPTY_SLICE : Slices.wrappedBuffer(definitionBytes, Integer.BYTES, definitionBytes.length - Integer.BYTES),
                PLAIN,
                Slices.wrappedBuffer(valueBytes),
                (repetitionBytes.length == 0 ? 0 : repetitionBytes.length - Integer.BYTES) + (definitionBytes.length == 0 ? 0 : definitionBytes.length - Integer.BYTES) + valueBytes.length,
                OptionalLong.empty(),
                null,
                false,
                0);
    }

    private static ValuesWriter levelsWriter(int maxLevel)
    {
        if (maxLevel == 0) {
            return new DevNullValuesWriter();
        }
        return new RunLengthBitPackingHybridValuesWriter(
                getWidthFromMaxInt(maxLevel),
                VALUES_PER_PAGE + 1,
                VALUES_PER_PAGE + 1,
                HeapByteBufferAllocator.getInstance());
    }

    static void main()
            throws RunnerException
    {
        AbstractColumnReaderBenchmark.run(BenchmarkNestedColumnReader.class);
    }
}
