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
package io.trino.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.sun.management.ThreadMXBean;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.ChunkedInputStream;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.PageReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.TestingParquetDataSource;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.format.CompressionCodec;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.RunnerException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.createParquetWriter;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static java.lang.Math.toIntExact;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 8, time = 1)
@Measurement(iterations = 8, time = 1)
public class BenchmarkParquetSelectedPositions
{
    private static final int SELECTION_WINDOW_SIZE = 8 * 1024;
    private static final ThreadMXBean THREAD_MX_BEAN = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private long comparisonInvocation;
    private Slice parquetFile;
    private ParquetMetadata parquetMetadata;
    private ParquetReaderOptions readerOptions;
    private List<DataType> columnDataTypes;
    private List<Type> columnTypes;
    private int[] columnNullPercentages;
    private List<String> columnNames;
    private int[] selectedFilePositions;
    private int[] dataPageEndPositions;
    private long inputSize;
    private long parquetUncompressedSize;
    private long dictionaryPageBytes;
    private int dictionaryEncodedColumnChunks;
    private int nonDictionaryEncodedColumnChunks;

    @Param("ZSTD")
    public CompressionCodec compression;

    @Param({"BOOLEAN", "INTEGER", "BIGINT", "DOUBLE", "VARCHAR", "DATE", "REAL", "DECIMAL_9", "DECIMAL_18", "DECIMAL_30"})
    public DataType dataType;

    @Param("RANDOM")
    public ValueShape valueShape;

    @Param("ADAPTIVE")
    public PushdownDecision pushdownDecision;

    @Param("1")
    public int columnCount;

    @Param("HOMOGENEOUS")
    public ColumnLayout columnLayout;

    @Param("RUNS")
    public SelectionShape selectionShape;

    @Param("0.10")
    public double selectivity;

    @Param({"1", "8", "12", "16", "128"})
    public int runCount;

    @Param("65536")
    public int rowCount;

    @Param("4096")
    public int pageValueCount;

    @Param("256")
    public int payloadWidth;

    @Param("32")
    public int entropyBytes;

    @Param("0")
    public int dictionaryCardinality;

    @Param("0")
    public int nullPercentage;

    @Param("RANDOM")
    public NullShape nullShape;

    @Param("16")
    public int nullRunCount;

    @Setup
    public void setup()
            throws IOException
    {
        if (selectivity < 0 || selectivity > 1) {
            throw new IllegalArgumentException("selectivity must be between 0 and 1");
        }
        if (payloadWidth < 1 || entropyBytes < 0 || entropyBytes > payloadWidth) {
            throw new IllegalArgumentException("entropyBytes must be between 0 and payloadWidth");
        }
        if (dictionaryCardinality < 0) {
            throw new IllegalArgumentException("dictionaryCardinality is negative");
        }
        if (nullPercentage < 0 || nullPercentage > 100) {
            throw new IllegalArgumentException("nullPercentage must be between 0 and 100");
        }
        if (nullRunCount < 1) {
            throw new IllegalArgumentException("nullRunCount must be positive");
        }
        if (columnCount < 1) {
            throw new IllegalArgumentException("columnCount must be positive");
        }
        if (pageValueCount < 1) {
            throw new IllegalArgumentException("pageValueCount must be positive");
        }
        ImmutableList.Builder<DataType> columnDataTypesBuilder = ImmutableList.builderWithExpectedSize(columnCount);
        ImmutableList.Builder<Type> columnTypesBuilder = ImmutableList.builderWithExpectedSize(columnCount);
        columnNullPercentages = new int[columnCount];
        ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builderWithExpectedSize(columnCount);
        for (int channel = 0; channel < columnCount; channel++) {
            DataType columnDataType = columnLayout.dataType(dataType, channel);
            columnDataTypesBuilder.add(columnDataType);
            columnTypesBuilder.add(columnDataType.getType());
            columnNullPercentages[channel] = columnLayout.nullPercentage(nullPercentage, channel);
            columnNamesBuilder.add("payload_" + channel);
        }
        columnDataTypes = columnDataTypesBuilder.build();
        columnTypes = columnTypesBuilder.build();
        columnNames = columnNamesBuilder.build();

        List<Page> pages = createInputPages();
        inputSize = pages.stream().mapToLong(Page::getSizeInBytes).sum();
        ParquetWriterOptions writerOptions = ParquetWriterOptions.builder()
                .setMaxBlockSize(DataSize.of(64, MEGABYTE))
                .setMaxPageSize(DataSize.of(16, MEGABYTE))
                .setMaxPageValueCount(pageValueCount)
                .build();

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ParquetWriter writer = createParquetWriter(output, writerOptions, columnTypes, columnNames, compression)) {
            for (Page page : pages) {
                writer.write(page);
            }
        }
        parquetFile = Slices.wrappedBuffer(output.toByteArray());
        readerOptions = ParquetReaderOptions.builder()
                .withMaxBufferSize(DataSize.of(64, MEGABYTE))
                .build();
        try (TestingParquetDataSource dataSource = new TestingParquetDataSource(parquetFile, readerOptions)) {
            parquetMetadata = MetadataReader.readFooter(dataSource, readerOptions, Optional.empty(), Optional.empty());
        }
        dataPageEndPositions = readDataPageEndPositions();
        selectedFilePositions = createSelectedFilePositions();
        parquetUncompressedSize = parquetMetadata.getBlocks().stream()
                .flatMap(block -> block.columns().stream())
                .mapToLong(column -> column.getTotalUncompressedSize())
                .sum();
        dictionaryEncodedColumnChunks = toIntExact(parquetMetadata.getBlocks().stream()
                .flatMap(block -> block.columns().stream())
                .filter(column -> column.getEncodingStats().hasDictionaryEncodedPages())
                .count());
        dictionaryPageBytes = parquetMetadata.getBlocks().stream()
                .flatMap(block -> block.columns().stream())
                .filter(column -> column.getDictionaryPageOffset() > 0)
                .mapToLong(column -> column.getFirstDataPageOffset() - column.getDictionaryPageOffset())
                .sum();
        nonDictionaryEncodedColumnChunks = toIntExact(parquetMetadata.getBlocks().stream()
                .flatMap(block -> block.columns().stream())
                .filter(column -> column.getEncodingStats().hasNonDictionaryEncodedPages())
                .count());
    }

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class DataCounters
    {
        public long completelySkippedDataPages;
        public long dictionaryEncodedColumnChunks;
        public long dictionaryPageBytes;
        public long fullDecodeDataPagesRead;
        public long fullDecodeAllocatedBytes;
        public long fullDecodeNanos;
        public long fullDecodeSourcePages;
        public long inputBytes;
        public long nonDictionaryEncodedColumnChunks;
        public long parquetFileBytes;
        public long parquetUncompressedBytes;
        public long rejectedRows;
        public long readOperations;
        public long selectedDataPages;
        public long selectedPushdownDataPagesRead;
        public long selectedPushdownAllocatedBytes;
        public long selectedPushdownNanos;
        public long selectedPushdownSourcePages;
        public long selectedPositionsFallbacks;
        public long selectedPositionsPushdowns;
        public long selectedRuns;
        public long selectedRows;
        public long totalDataPages;
    }

    @Benchmark
    public long fullDecodeThenSelect(DataCounters counters)
            throws IOException
    {
        return read(false, counters);
    }

    @Benchmark
    public long selectedPositionsPushdown(DataCounters counters)
            throws IOException
    {
        return read(true, counters);
    }

    @Benchmark
    public long pairedComparison(DataCounters counters)
            throws IOException
    {
        // Keep both paths in the same invocation and alternate their order to minimize workstation drift.
        long fullDecodeChecksum;
        long selectedPushdownChecksum;
        if ((comparisonInvocation++ & 1) == 0) {
            long start = System.nanoTime();
            long allocatedBytes = THREAD_MX_BEAN.getCurrentThreadAllocatedBytes();
            fullDecodeChecksum = read(false, counters);
            counters.fullDecodeAllocatedBytes += THREAD_MX_BEAN.getCurrentThreadAllocatedBytes() - allocatedBytes;
            counters.fullDecodeNanos += System.nanoTime() - start;

            start = System.nanoTime();
            allocatedBytes = THREAD_MX_BEAN.getCurrentThreadAllocatedBytes();
            selectedPushdownChecksum = read(true, counters);
            counters.selectedPushdownAllocatedBytes += THREAD_MX_BEAN.getCurrentThreadAllocatedBytes() - allocatedBytes;
            counters.selectedPushdownNanos += System.nanoTime() - start;
        }
        else {
            long start = System.nanoTime();
            long allocatedBytes = THREAD_MX_BEAN.getCurrentThreadAllocatedBytes();
            selectedPushdownChecksum = read(true, counters);
            counters.selectedPushdownAllocatedBytes += THREAD_MX_BEAN.getCurrentThreadAllocatedBytes() - allocatedBytes;
            counters.selectedPushdownNanos += System.nanoTime() - start;

            start = System.nanoTime();
            allocatedBytes = THREAD_MX_BEAN.getCurrentThreadAllocatedBytes();
            fullDecodeChecksum = read(false, counters);
            counters.fullDecodeAllocatedBytes += THREAD_MX_BEAN.getCurrentThreadAllocatedBytes() - allocatedBytes;
            counters.fullDecodeNanos += System.nanoTime() - start;
        }
        checkState(fullDecodeChecksum == selectedPushdownChecksum, "Checksums do not match");
        return fullDecodeChecksum;
    }

    private long read(boolean pushdown, DataCounters counters)
            throws IOException
    {
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        TestingParquetDataSource dataSource = new TestingParquetDataSource(parquetFile, readerOptions);
        long[] checksums = new long[columnCount];
        int globalOffset = 0;
        boolean[] selectedPages = new boolean[dataPageEndPositions.length];
        long selectedRuns = 0;
        long selectedRows = 0;
        long sourcePageCount = 0;
        int dataPagesRead;
        int selectedPositionsFallbacks;
        int selectedPositionsPushdowns;
        try (ParquetReader reader = createParquetReader(
                dataSource,
                parquetMetadata,
                readerOptions,
                memoryContext,
                columnTypes,
                columnNames,
                TupleDomain.all(),
                pushdownDecision == PushdownDecision.FORCE)) {
            for (SourcePage page = reader.nextPage(); page != null; page = reader.nextPage()) {
                sourcePageCount++;
                int originalPositionCount = page.getPositionCount();
                int[] positions = selectedPositions(globalOffset, originalPositionCount);

                if (positions.length == 0) {
                    globalOffset += originalPositionCount;
                    continue;
                }

                boolean allPositionsSelected = positions.length == originalPositionCount;
                boolean selectionApplied = pushdown
                        && !allPositionsSelected
                        && page.trySelectPositions(positions, 0, positions.length);

                for (int channel = 0; channel < columnCount; channel++) {
                    Block block = page.getBlock(channel);
                    if (!selectionApplied && !allPositionsSelected) {
                        block = block.copyPositions(positions, 0, positions.length);
                    }
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        checksums[channel] = columnDataTypes.get(channel).checksum(checksums[channel], block, position);
                    }
                }
                for (int position : positions) {
                    selectedPages[lowerBound(dataPageEndPositions, globalOffset + position + 1)] = true;
                }
                selectedRuns += countRuns(positions);
                selectedRows += positions.length;
                globalOffset += originalPositionCount;
            }
            dataPagesRead = reader.getDataPageReadCount();
            selectedPositionsFallbacks = reader.getSelectedPositionsFallbackCount();
            selectedPositionsPushdowns = reader.getSelectedPositionsPushdownCount();
        }
        memoryContext.close();

        counters.dictionaryEncodedColumnChunks += dictionaryEncodedColumnChunks;
        counters.dictionaryPageBytes += dictionaryPageBytes;
        counters.inputBytes += inputSize;
        counters.nonDictionaryEncodedColumnChunks += nonDictionaryEncodedColumnChunks;
        counters.parquetFileBytes += parquetFile.length();
        counters.parquetUncompressedBytes += parquetUncompressedSize;
        long selectedDataPages = (long) countSelectedPages(selectedPages) * columnCount;
        long totalDataPages = (long) selectedPages.length * columnCount;
        counters.completelySkippedDataPages += totalDataPages - selectedDataPages;
        counters.rejectedRows += rowCount - selectedRows;
        counters.readOperations++;
        counters.selectedDataPages += selectedDataPages;
        if (pushdown) {
            counters.selectedPositionsFallbacks += selectedPositionsFallbacks;
            counters.selectedPositionsPushdowns += selectedPositionsPushdowns;
            counters.selectedPushdownDataPagesRead += dataPagesRead;
            counters.selectedPushdownSourcePages += sourcePageCount;
        }
        else {
            counters.fullDecodeDataPagesRead += dataPagesRead;
            counters.fullDecodeSourcePages += sourcePageCount;
        }
        counters.selectedRuns += selectedRuns;
        counters.selectedRows += selectedRows;
        counters.totalDataPages += totalDataPages;
        long checksum = 0;
        for (long channelChecksum : checksums) {
            checksum = 31 * checksum + channelChecksum;
        }
        return checksum;
    }

    private int[] readDataPageEndPositions()
            throws IOException
    {
        List<Integer> pageEndPositions = new ArrayList<>();
        int rowOffset = 0;
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        try (TestingParquetDataSource dataSource = new TestingParquetDataSource(parquetFile, readerOptions)) {
            for (var block : parquetMetadata.getBlocks()) {
                var columnMetadata = block.columns().getFirst();
                DiskRange diskRange = new DiskRange(columnMetadata.getStartingPos(), columnMetadata.getTotalSize());
                ChunkedInputStream input = dataSource.planRead(ImmutableListMultimap.of(0, diskRange), memoryContext).get(0);
                try (input) {
                    PageReader pageReader = PageReader.createPageReader(
                            dataSource.getId(),
                            input,
                            columnMetadata,
                            parquetMetadata.getFileMetaData().getSchema().getColumns().getFirst(),
                            null,
                            Optional.ofNullable(parquetMetadata.getFileMetaData().getCreatedBy()),
                            Optional.empty(),
                            readerOptions.getMaxPageReadSize().toBytes());
                    pageReader.readDictionaryPage();
                    while (pageReader.hasNext()) {
                        DataPage page = pageReader.getNextPage();
                        rowOffset += page.getValueCount();
                        pageEndPositions.add(rowOffset);
                        pageReader.skipNextPage();
                    }
                }
            }
        }
        memoryContext.close();
        checkState(rowOffset == rowCount, "Data pages contain %s rows, expected %s", rowOffset, rowCount);
        return pageEndPositions.stream().mapToInt(Integer::intValue).toArray();
    }

    private int[] createSelectedFilePositions()
    {
        if (selectionShape == SelectionShape.PAGE_ALIGNED || selectionShape == SelectionShape.PAGE_SHIFTED) {
            return createPageSelectedFilePositions(selectionShape == SelectionShape.PAGE_SHIFTED);
        }
        int[] positions = new int[rowCount];
        int selectedCount = 0;
        if (selectionShape == SelectionShape.RUNS) {
            for (int windowOffset = 0; windowOffset < rowCount; windowOffset += SELECTION_WINDOW_SIZE) {
                int windowSize = min(SELECTION_WINDOW_SIZE, rowCount - windowOffset);
                int[] windowPositions = selectionShape.select(windowOffset, windowSize, selectivity, runCount, pageValueCount);
                for (int position : windowPositions) {
                    positions[selectedCount++] = windowOffset + position;
                }
            }
        }
        else {
            int[] filePositions = selectionShape.select(0, rowCount, selectivity, runCount, pageValueCount);
            System.arraycopy(filePositions, 0, positions, 0, filePositions.length);
            selectedCount = filePositions.length;
        }
        return Arrays.copyOf(positions, selectedCount);
    }

    private int[] createPageSelectedFilePositions(boolean shifted)
    {
        boolean[] selected = new boolean[rowCount];
        int pageStart = 0;
        for (int pageIndex = 0; pageIndex < dataPageEndPositions.length; pageIndex++) {
            int pageEnd = dataPageEndPositions[pageIndex];
            if (SelectionShape.isSelectedPage(pageIndex, selectivity)) {
                int shift = shifted ? (pageEnd - pageStart) / 2 : 0;
                for (int position = pageStart; position < pageEnd; position++) {
                    selected[Math.floorMod(position - shift, rowCount)] = true;
                }
            }
            pageStart = pageEnd;
        }

        int[] positions = new int[rowCount];
        int selectedCount = 0;
        for (int position = 0; position < rowCount; position++) {
            if (selected[position]) {
                positions[selectedCount++] = position;
            }
        }
        return Arrays.copyOf(positions, selectedCount);
    }

    private int[] selectedPositions(int pageOffset, int positionCount)
    {
        int start = lowerBound(selectedFilePositions, pageOffset);
        int end = lowerBound(selectedFilePositions, pageOffset + positionCount);
        int[] positions = new int[end - start];
        for (int index = start; index < end; index++) {
            positions[index - start] = selectedFilePositions[index] - pageOffset;
        }
        return positions;
    }

    private static int lowerBound(int[] values, int value)
    {
        int low = 0;
        int high = values.length;
        while (low < high) {
            int middle = (low + high) >>> 1;
            if (values[middle] < value) {
                low = middle + 1;
            }
            else {
                high = middle;
            }
        }
        return low;
    }

    private static int countSelectedPages(boolean[] selectedPages)
    {
        int count = 0;
        for (boolean selected : selectedPages) {
            if (selected) {
                count++;
            }
        }
        return count;
    }

    private static int countRuns(int[] positions)
    {
        int runs = positions.length == 0 ? 0 : 1;
        for (int index = 1; index < positions.length; index++) {
            if (positions[index] != positions[index - 1] + 1) {
                runs++;
            }
        }
        return runs;
    }

    private List<Page> createInputPages()
    {
        List<Page> pages = new ArrayList<>();
        PageBuilder pageBuilder = PageBuilder.withMaxPageSize(toIntExact(DataSize.of(64, MEGABYTE).toBytes()), columnTypes);
        Slice[][] dictionaries = new Slice[columnCount][dictionaryCardinality];
        for (int row = 0; row < rowCount; row++) {
            pageBuilder.declarePosition();
            for (int channel = 0; channel < columnCount; channel++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
                int valueId = row * columnCount + channel;
                if (nullShape.isNull(row, rowCount, columnNullPercentages[channel], nullRunCount, valueId)) {
                    blockBuilder.appendNull();
                }
                else {
                    if (dictionaryCardinality > 0) {
                        valueId %= dictionaryCardinality;
                    }
                    columnDataTypes.get(channel).write(this, blockBuilder, valueId, dictionaries[channel]);
                }
            }

            if ((row + 1) % pageValueCount == 0 || pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return pages;
    }

    public enum DataType
    {
        BOOLEAN(BooleanType.BOOLEAN) {
            @Override
            void write(BenchmarkParquetSelectedPositions benchmark, BlockBuilder builder, int valueId, Slice[] dictionary)
            {
                BooleanType.BOOLEAN.writeBoolean(builder, (mix(valueId) & 1) == 0);
            }

            @Override
            long checksum(long checksum, Block block, int position)
            {
                if (block.isNull(position)) {
                    return 31 * checksum + 1;
                }
                return 31 * checksum + (BooleanType.BOOLEAN.getBoolean(block, position) ? 1231 : 1237);
            }
        },
        INTEGER(IntegerType.INTEGER) {
            @Override
            void write(BenchmarkParquetSelectedPositions benchmark, BlockBuilder builder, int valueId, Slice[] dictionary)
            {
                IntegerType.INTEGER.writeInt(builder, (int) benchmark.valueShape.value(valueId));
            }

            @Override
            long checksum(long checksum, Block block, int position)
            {
                return block.isNull(position) ? 31 * checksum + 1 : 31 * checksum + IntegerType.INTEGER.getInt(block, position);
            }
        },
        BIGINT(BigintType.BIGINT) {
            @Override
            void write(BenchmarkParquetSelectedPositions benchmark, BlockBuilder builder, int valueId, Slice[] dictionary)
            {
                BigintType.BIGINT.writeLong(builder, benchmark.valueShape.value(valueId));
            }

            @Override
            long checksum(long checksum, Block block, int position)
            {
                return block.isNull(position) ? 31 * checksum + 1 : 31 * checksum + BigintType.BIGINT.getLong(block, position);
            }
        },
        DOUBLE(DoubleType.DOUBLE) {
            @Override
            void write(BenchmarkParquetSelectedPositions benchmark, BlockBuilder builder, int valueId, Slice[] dictionary)
            {
                DoubleType.DOUBLE.writeDouble(builder, benchmark.valueShape.doubleValue(valueId));
            }

            @Override
            long checksum(long checksum, Block block, int position)
            {
                return block.isNull(position) ? 31 * checksum + 1 : 31 * checksum + Double.doubleToLongBits(DoubleType.DOUBLE.getDouble(block, position));
            }
        },
        VARCHAR(VarcharType.VARCHAR) {
            @Override
            void write(BenchmarkParquetSelectedPositions benchmark, BlockBuilder builder, int valueId, Slice[] dictionary)
            {
                Slice value = dictionary.length == 0 ? null : dictionary[valueId];
                if (value == null) {
                    value = benchmark.createPayload(valueId);
                    if (dictionary.length > 0) {
                        dictionary[valueId] = value;
                    }
                }
                VarcharType.VARCHAR.writeSlice(builder, value);
            }

            @Override
            long checksum(long checksum, Block block, int position)
            {
                if (block.isNull(position)) {
                    return 31 * checksum + 1;
                }
                Slice value = VarcharType.VARCHAR.getSlice(block, position);
                checksum = 31 * checksum + value.length();
                checksum = 31 * checksum + value.getByte(0);
                return 31 * checksum + value.getByte(value.length() - 1);
            }
        },
        DATE(DateType.DATE) {
            @Override
            void write(BenchmarkParquetSelectedPositions benchmark, BlockBuilder builder, int valueId, Slice[] dictionary)
            {
                DateType.DATE.writeLong(builder, Math.floorMod(valueId, 365_000));
            }

            @Override
            long checksum(long checksum, Block block, int position)
            {
                return block.isNull(position) ? 31 * checksum + 1 : 31 * checksum + DateType.DATE.getLong(block, position);
            }
        },
        REAL(RealType.REAL) {
            @Override
            void write(BenchmarkParquetSelectedPositions benchmark, BlockBuilder builder, int valueId, Slice[] dictionary)
            {
                float value = (float) benchmark.valueShape.doubleValue(valueId);
                RealType.REAL.writeLong(builder, Float.floatToRawIntBits(value));
            }

            @Override
            long checksum(long checksum, Block block, int position)
            {
                return block.isNull(position) ? 31 * checksum + 1 : 31 * checksum + Float.floatToRawIntBits(RealType.REAL.getFloat(block, position));
            }
        },
        DECIMAL_9(DecimalType.createDecimalType(9, 2)) {
            @Override
            void write(BenchmarkParquetSelectedPositions benchmark, BlockBuilder builder, int valueId, Slice[] dictionary)
            {
                ((DecimalType) getType()).writeLong(builder, Math.floorMod(benchmark.valueShape.value(valueId), 100_000_000L));
            }

            @Override
            long checksum(long checksum, Block block, int position)
            {
                return block.isNull(position) ? 31 * checksum + 1 : 31 * checksum + ((DecimalType) getType()).getLong(block, position);
            }
        },
        DECIMAL_18(DecimalType.createDecimalType(18, 2)) {
            @Override
            void write(BenchmarkParquetSelectedPositions benchmark, BlockBuilder builder, int valueId, Slice[] dictionary)
            {
                ((DecimalType) getType()).writeLong(builder, Math.floorMod(benchmark.valueShape.value(valueId), 100_000_000_000_000_000L));
            }

            @Override
            long checksum(long checksum, Block block, int position)
            {
                return block.isNull(position) ? 31 * checksum + 1 : 31 * checksum + ((DecimalType) getType()).getLong(block, position);
            }
        },
        DECIMAL_30(DecimalType.createDecimalType(30, 2)) {
            @Override
            void write(BenchmarkParquetSelectedPositions benchmark, BlockBuilder builder, int valueId, Slice[] dictionary)
            {
                ((DecimalType) getType()).writeObject(builder, Int128.valueOf(benchmark.valueShape.value(valueId)));
            }

            @Override
            long checksum(long checksum, Block block, int position)
            {
                if (block.isNull(position)) {
                    return 31 * checksum + 1;
                }
                Int128 value = (Int128) ((DecimalType) getType()).getObject(block, position);
                checksum = 31 * checksum + value.getHigh();
                return 31 * checksum + value.getLow();
            }
        };

        private final Type type;

        DataType(Type type)
        {
            this.type = type;
        }

        Type getType()
        {
            return type;
        }

        abstract void write(BenchmarkParquetSelectedPositions benchmark, BlockBuilder builder, int valueId, Slice[] dictionary);

        abstract long checksum(long checksum, Block block, int position);
    }

    public enum ValueShape
    {
        RANDOM {
            @Override
            long value(int valueId)
            {
                return mix(valueId);
            }

            @Override
            double doubleValue(int valueId)
            {
                return (mix(valueId) >>> 11) * 0x1.0p-53;
            }
        },
        SEQUENTIAL {
            @Override
            long value(int valueId)
            {
                return valueId;
            }
        },
        LOW_CARDINALITY {
            @Override
            long value(int valueId)
            {
                return valueId % 16;
            }
        },
        SORTED_RUNS {
            @Override
            long value(int valueId)
            {
                return valueId / 32;
            }
        };

        abstract long value(int valueId);

        double doubleValue(int valueId)
        {
            return value(valueId);
        }
    }

    public enum PushdownDecision
    {
        ADAPTIVE,
        FORCE,
    }

    public enum ColumnLayout
    {
        HOMOGENEOUS {
            @Override
            DataType dataType(DataType firstColumnType, int channel)
            {
                return firstColumnType;
            }

            @Override
            int nullPercentage(int configuredNullPercentage, int channel)
            {
                return configuredNullPercentage;
            }
        },
        FIRST_TYPE_THEN_BIGINT {
            @Override
            DataType dataType(DataType firstColumnType, int channel)
            {
                return channel == 0 ? firstColumnType : DataType.BIGINT;
            }

            @Override
            int nullPercentage(int configuredNullPercentage, int channel)
            {
                return channel == 0 ? 0 : configuredNullPercentage;
            }
        };

        abstract DataType dataType(DataType firstColumnType, int channel);

        abstract int nullPercentage(int configuredNullPercentage, int channel);
    }

    private Slice createPayload(int valueId)
    {
        byte[] bytes = new byte[payloadWidth];
        Arrays.fill(bytes, (byte) 'x');
        long state = mix(valueId);
        for (int index = 0; index < entropyBytes; index++) {
            if ((index & 7) == 0) {
                state = mix(state + index);
            }
            bytes[index] = (byte) (state >>> ((index & 7) * Byte.SIZE));
        }
        return Slices.wrappedBuffer(bytes);
    }

    private static long mix(long value)
    {
        value = (value ^ (value >>> 30)) * 0xBF58476D1CE4E5B9L;
        value = (value ^ (value >>> 27)) * 0x94D049BB133111EBL;
        return value ^ (value >>> 31);
    }

    private static int unsignedRemainder(long value, int divisor)
    {
        return (int) Long.remainderUnsigned(value, divisor);
    }

    public enum SelectionShape
    {
        RUNS {
            @Override
            int[] select(int globalOffset, int positionCount, double selectivity, int requestedRunCount, int pageValueCount)
            {
                int selectedCount = (int) round(positionCount * selectivity);
                if (selectivity > 0 && positionCount > 0) {
                    selectedCount = max(1, selectedCount);
                }
                selectedCount = min(positionCount, selectedCount);
                if (selectedCount == 0) {
                    return new int[0];
                }

                int runs = min(max(1, requestedRunCount), min(selectedCount, positionCount - selectedCount + 1));
                int[] positions = new int[selectedCount];
                int[] gaps = new int[runs + 1];
                int freeGaps = positionCount - selectedCount - (runs - 1);
                Arrays.fill(gaps, freeGaps / gaps.length);
                for (int gap = 0; gap < freeGaps % gaps.length; gap++) {
                    gaps[gap]++;
                }

                int outputIndex = 0;
                int position = gaps[0];
                for (int run = 0; run < runs; run++) {
                    int runLength = selectedCount / runs + (run < selectedCount % runs ? 1 : 0);
                    for (int index = 0; index < runLength; index++) {
                        positions[outputIndex++] = position++;
                    }
                    if (run + 1 < runs) {
                        position += 1 + gaps[run + 1];
                    }
                }
                return positions;
            }
        },
        CONCENTRATED {
            @Override
            int[] select(int globalOffset, int positionCount, double selectivity, int requestedRunCount, int pageValueCount)
            {
                int selectedCount = selectedCount(positionCount, selectivity);
                int firstPosition = (positionCount - selectedCount) / 2;
                int[] positions = new int[selectedCount];
                for (int index = 0; index < selectedCount; index++) {
                    positions[index] = firstPosition + index;
                }
                return positions;
            }
        },
        DISTRIBUTED {
            @Override
            int[] select(int globalOffset, int positionCount, double selectivity, int requestedRunCount, int pageValueCount)
            {
                int selectedCount = selectedCount(positionCount, selectivity);
                if (selectedCount == 0) {
                    return new int[0];
                }
                int[] positions = new int[selectedCount];
                for (int index = 0; index < selectedCount; index++) {
                    positions[index] = toIntExact(((long) index * positionCount + positionCount / 2) / selectedCount);
                }
                return positions;
            }
        },
        PAGE_ALIGNED {
            @Override
            int[] select(int globalOffset, int positionCount, double selectivity, int requestedRunCount, int pageValueCount)
            {
                if (selectivity == 0) {
                    return new int[0];
                }
                int[] positions = new int[positionCount];
                int selectedCount = 0;
                for (int position = 0; position < positionCount; position++) {
                    long globalPosition = (long) globalOffset + position;
                    if (isSelectedPage(globalPosition / pageValueCount, selectivity)) {
                        positions[selectedCount++] = position;
                    }
                }
                return Arrays.copyOf(positions, selectedCount);
            }
        },
        PAGE_SHIFTED {
            @Override
            int[] select(int globalOffset, int positionCount, double selectivity, int requestedRunCount, int pageValueCount)
            {
                if (selectivity == 0) {
                    return new int[0];
                }
                int shift = max(1, pageValueCount / 2);
                int[] positions = new int[positionCount];
                int selectedCount = 0;
                for (int position = 0; position < positionCount; position++) {
                    long globalPosition = (long) globalOffset + position + shift;
                    if (isSelectedPage(globalPosition / pageValueCount, selectivity)) {
                        positions[selectedCount++] = position;
                    }
                }
                return Arrays.copyOf(positions, selectedCount);
            }
        };

        abstract int[] select(int globalOffset, int positionCount, double selectivity, int requestedRunCount, int pageValueCount);

        private static int selectedCount(int positionCount, double selectivity)
        {
            int selectedCount = (int) round(positionCount * selectivity);
            if (selectivity > 0 && positionCount > 0) {
                selectedCount = max(1, selectedCount);
            }
            return min(positionCount, selectedCount);
        }

        private static boolean isSelectedPage(long pageIndex, double selectivity)
        {
            return (long) Math.floor((pageIndex + 1) * selectivity + 0.5) > (long) Math.floor(pageIndex * selectivity + 0.5);
        }
    }

    public enum NullShape
    {
        RANDOM {
            @Override
            boolean isNull(int row, int rowCount, int nullPercentage, int nullRunCount, int valueId)
            {
                return unsignedRemainder(mix(valueId), 100) < nullPercentage;
            }
        },
        CLUSTERED {
            @Override
            boolean isNull(int row, int rowCount, int nullPercentage, int nullRunCount, int valueId)
            {
                int nullCount = rowCount * nullPercentage / 100;
                int firstNull = (rowCount - nullCount) / 2;
                return row >= firstNull && row < firstNull + nullCount;
            }
        },
        RUNS {
            @Override
            boolean isNull(int row, int rowCount, int nullPercentage, int nullRunCount, int valueId)
            {
                int cycleLength = max(1, rowCount / nullRunCount);
                return row % cycleLength < cycleLength * nullPercentage / 100;
            }
        };

        abstract boolean isNull(int row, int rowCount, int nullPercentage, int nullRunCount, int valueId);
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkParquetSelectedPositions.class)
                .withOptions(optionsBuilder -> optionsBuilder
                        .addProfiler(GCProfiler.class)
                        .jvmArgsAppend("-Xmx4g", "-Xms4g", "--add-modules=jdk.incubator.vector"))
                .run();
    }
}
