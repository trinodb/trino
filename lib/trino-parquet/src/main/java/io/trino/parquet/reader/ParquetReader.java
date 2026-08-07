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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.FormatMethod;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.ChunkKey;
import io.trino.parquet.Column;
import io.trino.parquet.DiskRange;
import io.trino.parquet.Field;
import io.trino.parquet.GroupField;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetWriteValidation;
import io.trino.parquet.ParquetWriteValidation.StatisticsValidation;
import io.trino.parquet.ParquetWriteValidation.WriteChecksumBuilder;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.VariantField;
import io.trino.parquet.crypto.FileDecryptionContext;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.PrunedBlockMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.FilteredOffsetIndex.OffsetRange;
import io.trino.parquet.spark.Variant;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariantBlock;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import jakarta.annotation.Nullable;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ObjLongConsumer;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.parquet.ParquetReaderUtils.isOnlyDictionaryEncodingPages;
import static io.trino.parquet.ParquetValidationUtils.validateParquet;
import static io.trino.parquet.ParquetWriteValidation.StatisticsValidation.createStatisticsValidationBuilder;
import static io.trino.parquet.ParquetWriteValidation.WriteChecksumBuilder.createWriteChecksumBuilder;
import static io.trino.parquet.reader.ListColumnReader.calculateCollectionOffsets;
import static io.trino.parquet.reader.PageReader.createPageReader;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Bitmap.wordsForBits;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VariantType.VARIANT;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class ParquetReader
        implements Closeable
{
    private static final Logger log = Logger.get(ParquetReader.class);

    private static final int INITIAL_BATCH_SIZE = 1;
    private static final int BATCH_SIZE_GROWTH_FACTOR = 2;
    private static final int MAX_FIXED_WIDTH_SELECTED_PERCENTAGE = 5;
    private static final int MAX_DICTIONARY_SELECTED_PERCENTAGE = 10;
    private static final int MAX_BINARY_SELECTED_PERCENTAGE = 25;
    private static final int MAX_NULLABLE_FIXED_LEN_BYTE_ARRAY_SELECTED_PERCENTAGE = 25;
    private static final long VERY_WIDE_BINARY_BYTES = 1_024;
    private static final int MAX_VERY_WIDE_BINARY_PROJECTED_COLUMNS = 4;
    private static final int MIN_SKIPPED_POSITIONS_PER_RUN = 350;
    private static final long MIN_SKIPPED_PAGE_BYTES = 4 * 1024;
    private static final int MAX_PAGE_AWARE_SELECTED_PERCENTAGE = 75;
    public static final String PARQUET_CODEC_METRIC_PREFIX = "ParquetReaderCompressionFormat_";
    public static final String COLUMN_INDEX_ROWS_FILTERED = "ParquetColumnIndexRowsFiltered";

    private final Optional<String> fileCreatedBy;
    private final List<RowGroupInfo> rowGroups;
    private final List<Column> columnFields;
    private final boolean appendRowNumberColumn;
    private final List<PrimitiveField> primitiveFields;
    private final ParquetDataSource dataSource;
    private final ZoneId zoneId;
    private final ColumnReaderFactory columnReaderFactory;
    private final AggregatedMemoryContext memoryContext;

    private int currentRowGroup = -1;
    private PrunedBlockMetadata currentBlockMetadata;
    private long currentGroupRowCount;
    /**
     * Index in the Parquet file of the first row of the current group
     */
    private long firstRowIndexInGroup;
    /**
     * Index in the current group of the next row
     */
    private long nextRowInGroup;
    private int batchSize;
    private int nextBatchSize = INITIAL_BATCH_SIZE;
    private final Map<Integer, ColumnReader> columnReaders;
    private final Map<Integer, SelectedPositionsPushdownCharacteristics> selectedPositionsPushdownCharacteristics = new HashMap<>();
    private final Map<Integer, Double> maxBytesPerCell;
    private double maxCombinedBytesPerRow;
    private final ParquetReaderOptions options;
    private int maxBatchSize;

    private AggregatedMemoryContext currentRowGroupMemoryContext;
    private final Map<ChunkKey, ChunkedInputStream> chunkReaders;
    private final Optional<ParquetWriteValidation> writeValidation;
    private final Optional<WriteChecksumBuilder> writeChecksumBuilder;
    private final Optional<StatisticsValidation> rowGroupStatisticsValidation;
    private final FilteredRowRanges[] blockRowRanges;
    private final Function<Exception, RuntimeException> exceptionTransform;
    private final Map<String, Metric<?>> codecMetrics;

    private int currentPageId;
    private int completedRowGroupDataPageReadCount;
    private int selectedPositionsFallbackCount;
    private int selectedPositionsPushdownCount;

    private long columnIndexRowsFiltered = -1;
    private final Optional<FileDecryptionContext> decryptionContext;
    private final boolean forceSelectedPositionsPushdown;

    public ParquetReader(
            Optional<String> fileCreatedBy,
            List<Column> columnFields,
            boolean appendRowNumberColumn,
            List<RowGroupInfo> rowGroups,
            ParquetDataSource dataSource,
            DateTimeZone timeZone,
            AggregatedMemoryContext memoryContext,
            ParquetReaderOptions options,
            Function<Exception, RuntimeException> exceptionTransform,
            Optional<TupleDomainParquetPredicate> parquetPredicate,
            Optional<ParquetWriteValidation> writeValidation,
            Optional<FileDecryptionContext> decryptionContext)
            throws IOException
    {
        this(fileCreatedBy,
                columnFields,
                appendRowNumberColumn,
                rowGroups,
                dataSource,
                timeZone,
                memoryContext,
                options,
                exceptionTransform,
                parquetPredicate,
                writeValidation,
                decryptionContext,
                false);
    }

    @VisibleForTesting
    public ParquetReader(
            Optional<String> fileCreatedBy,
            List<Column> columnFields,
            boolean appendRowNumberColumn,
            List<RowGroupInfo> rowGroups,
            ParquetDataSource dataSource,
            DateTimeZone timeZone,
            AggregatedMemoryContext memoryContext,
            ParquetReaderOptions options,
            Function<Exception, RuntimeException> exceptionTransform,
            Optional<TupleDomainParquetPredicate> parquetPredicate,
            Optional<ParquetWriteValidation> writeValidation,
            Optional<FileDecryptionContext> decryptionContext,
            boolean forceSelectedPositionsPushdown)
            throws IOException
    {
        this.fileCreatedBy = requireNonNull(fileCreatedBy, "fileCreatedBy is null");
        requireNonNull(columnFields, "columnFields is null");
        this.columnFields = ImmutableList.copyOf(columnFields);
        this.appendRowNumberColumn = appendRowNumberColumn;
        this.primitiveFields = getPrimitiveFields(columnFields.stream().map(Column::field).collect(toImmutableList()));
        this.rowGroups = requireNonNull(rowGroups, "rowGroups is null");
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.zoneId = requireNonNull(timeZone, "timeZone is null").toTimeZone().toZoneId();
        this.columnReaderFactory = new ColumnReaderFactory(timeZone, options);
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.currentRowGroupMemoryContext = memoryContext.newAggregatedMemoryContext();
        this.options = requireNonNull(options, "options is null");
        this.maxBatchSize = options.getMaxReadBlockRowCount();
        this.columnReaders = new HashMap<>();
        this.maxBytesPerCell = new HashMap<>();
        this.decryptionContext = requireNonNull(decryptionContext, "decryptionContext is null");
        this.forceSelectedPositionsPushdown = forceSelectedPositionsPushdown;

        this.writeValidation = requireNonNull(writeValidation, "writeValidation is null");
        validateWrite(
                validation -> fileCreatedBy.equals(Optional.of(validation.getCreatedBy())),
                "Expected created by %s, found %s",
                writeValidation.map(ParquetWriteValidation::getCreatedBy),
                fileCreatedBy);
        validateBlockMetadata(rowGroups);
        this.writeChecksumBuilder = writeValidation.map(validation -> createWriteChecksumBuilder(validation.getTypes()));
        this.rowGroupStatisticsValidation = writeValidation.map(validation -> createStatisticsValidationBuilder(validation.getTypes()));

        requireNonNull(parquetPredicate, "parquetPredicate is null");
        Optional<FilterPredicate> filter = Optional.empty();
        if (parquetPredicate.isPresent() && options.isUseColumnIndex()) {
            filter = parquetPredicate.get().toParquetFilter(timeZone);
        }
        this.blockRowRanges = calculateFilteredRowRanges(rowGroups, filter, primitiveFields);

        this.exceptionTransform = exceptionTransform;
        ListMultimap<ChunkKey, DiskRange> ranges = ArrayListMultimap.create();
        Map<String, LongCount> codecMetrics = new HashMap<>();
        for (int rowGroup = 0; rowGroup < rowGroups.size(); rowGroup++) {
            PrunedBlockMetadata blockMetadata = rowGroups.get(rowGroup).prunedBlockMetadata();
            long rowGroupRowCount = blockMetadata.getRowCount();
            for (PrimitiveField field : primitiveFields) {
                int columnId = field.getId();
                ColumnChunkMetadata chunkMetadata = blockMetadata.getColumnChunkMetaData(field.getDescriptor());
                ColumnPath columnPath = chunkMetadata.getPath();

                long startingPosition = chunkMetadata.getStartingPos();
                long totalLength = chunkMetadata.getTotalSize();
                long totalDataSize = 0;
                FilteredOffsetIndex filteredOffsetIndex = null;
                if (blockRowRanges[rowGroup] != null) {
                    filteredOffsetIndex = getFilteredOffsetIndex(blockRowRanges[rowGroup], rowGroup, rowGroupRowCount, columnPath);
                }
                if (filteredOffsetIndex == null) {
                    DiskRange range = new DiskRange(startingPosition, totalLength);
                    totalDataSize = range.length();
                    ranges.put(new ChunkKey(columnId, rowGroup), range);
                }
                else {
                    List<OffsetRange> offsetRanges = filteredOffsetIndex.calculateOffsetRanges(startingPosition);
                    for (OffsetRange offsetRange : offsetRanges) {
                        DiskRange range = new DiskRange(offsetRange.getOffset(), offsetRange.getLength());
                        totalDataSize += range.length();
                        ranges.put(new ChunkKey(columnId, rowGroup), range);
                    }
                    // Initialize columnIndexRowsFiltered only when column indexes are found and used
                    columnIndexRowsFiltered = 0;
                }
                // Update the metrics which records the codecs used along with data size
                codecMetrics.merge(
                        PARQUET_CODEC_METRIC_PREFIX + chunkMetadata.getCodec().name(),
                        new LongCount(totalDataSize),
                        LongCount::mergeWith);
            }
        }
        this.codecMetrics = ImmutableMap.copyOf(codecMetrics);
        this.chunkReaders = dataSource.planRead(ranges, memoryContext);
    }

    @Override
    public void close()
            throws IOException
    {
        // Release memory usage from column readers
        columnReaders.clear();
        currentRowGroupMemoryContext.close();

        for (ChunkedInputStream chunkedInputStream : chunkReaders.values()) {
            chunkedInputStream.close();
        }
        dataSource.close();

        if (writeChecksumBuilder.isPresent()) {
            ParquetWriteValidation parquetWriteValidation = writeValidation.orElseThrow();
            parquetWriteValidation.validateChecksum(dataSource.getId(), writeChecksumBuilder.get().build());
        }
    }

    public SourcePage nextPage()
            throws IOException
    {
        int batchSize = nextBatch();
        if (batchSize <= 0) {
            return null;
        }
        // create a lazy page
        currentPageId++;
        SourcePage page = new ParquetSourcePage(batchSize);
        validateWritePageChecksum(page);
        return page;
    }

    private class ParquetSourcePage
            implements SourcePage
    {
        private static final long INSTANCE_SIZE = instanceSize(ParquetSourcePage.class);

        private final int expectedPageId = currentPageId;
        private final Block[] blocks = new Block[columnFields.size() + (appendRowNumberColumn ? 1 : 0)];
        private final int rowNumberColumnIndex = appendRowNumberColumn ? columnFields.size() : -1;
        private SelectedPositions selectedPositions;
        @Nullable
        private SelectedPositionsReadMode[] selectedPositionsReadModes;
        private int unloadedSelectedColumns;
        private boolean selectedPositionsPushedDown;

        private long sizeInBytes;
        private long retainedSizeInBytes;

        public ParquetSourcePage(int positionCount)
        {
            selectedPositions = SelectedPositions.allPositions(positionCount);
            retainedSizeInBytes = shallowRetainedSizeInBytes();
        }

        @Override
        public int getPositionCount()
        {
            return selectedPositions.positionCount();
        }

        @Override
        public long getSizeInBytes()
        {
            return sizeInBytes;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return retainedSizeInBytes;
        }

        private long shallowRetainedSizeInBytes()
        {
            return INSTANCE_SIZE +
                    sizeOf(blocks) +
                    sizeOf(selectedPositionsReadModes) +
                    selectedPositions.retainedSizeInBytes();
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            consumer.accept(this, INSTANCE_SIZE);
            consumer.accept(blocks, sizeOf(blocks));
            consumer.accept(selectedPositions, selectedPositions.retainedSizeInBytes());
            for (Block block : blocks) {
                if (block != null) {
                    block.retainedBytesForEachPart(consumer);
                }
            }
        }

        @Override
        public int getChannelCount()
        {
            return blocks.length;
        }

        @Override
        public Block getBlock(int channel)
        {
            checkState(currentPageId == expectedPageId, "Parquet reader has been advanced beyond block");
            Block block = blocks[channel];
            if (block == null) {
                if (channel == rowNumberColumnIndex) {
                    block = selectedPositions.createRowNumberBlock(lastBatchStartRow());
                }
                else {
                    try {
                        Field field = columnFields.get(channel).field();
                        SelectedPositionsReadMode readMode = selectedPositionsReadModes == null
                                ? getSelectedPositionsReadMode(field, selectedPositions)
                                : requireNonNull(selectedPositionsReadModes[channel], "selected positions read mode is null");
                        block = switch (readMode) {
                            case FULL -> selectedPositions.apply(readBlock(field));
                            case PAGE -> readPrimitivePageFiltered((PrimitiveField) field, selectedPositions).getBlock();
                            case ROW -> readPrimitive((PrimitiveField) field, selectedPositions).getBlock();
                        };
                        recordSelectionDecision(readMode);
                    }
                    catch (IOException e) {
                        throw exceptionTransform.apply(e);
                    }
                }
                blocks[channel] = block;
                sizeInBytes += block.getSizeInBytes();
                retainedSizeInBytes += block.getRetainedSizeInBytes();
            }
            return block;
        }

        @Override
        public Page getPage()
        {
            // ensure all blocks are loaded
            for (int i = 0; i < blocks.length; i++) {
                getBlock(i);
            }
            return new Page(selectedPositions.positionCount(), blocks);
        }

        @Override
        public boolean trySelectPositions(int[] positions, int offset, int size)
        {
            SelectedPositions newSelectedPositions = selectedPositions.selectPositionsView(positions, offset, size);
            SelectedPositionsReadMode[] readModes = new SelectedPositionsReadMode[columnFields.size()];
            boolean beneficial = false;
            for (int channel = 0; channel < columnFields.size(); channel++) {
                if (blocks[channel] == null) {
                    try {
                        readModes[channel] = getSelectedPositionsReadMode(columnFields.get(channel).field(), newSelectedPositions);
                    }
                    catch (IOException e) {
                        throw exceptionTransform.apply(e);
                    }
                    beneficial |= readModes[channel] != SelectedPositionsReadMode.FULL;
                }
            }
            if (!beneficial) {
                selectedPositionsReadModes = null;
                return false;
            }

            int[] retainedPositions = Arrays.copyOfRange(positions, offset, offset + size);
            selectedPositionsReadModes = readModes;
            selectPositions(selectedPositions.selectPositions(retainedPositions, 0, size), retainedPositions, 0, size);
            resetSelectionTracking();
            return true;
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            selectedPositionsReadModes = null;
            SelectedPositions newSelectedPositions = selectedPositions.selectPositions(positions, offset, size);
            selectPositions(newSelectedPositions, positions, offset, size);
            resetSelectionTracking();
        }

        private void resetSelectionTracking()
        {
            unloadedSelectedColumns = 0;
            selectedPositionsPushedDown = false;
            for (int channel = 0; channel < columnFields.size(); channel++) {
                if (blocks[channel] == null) {
                    unloadedSelectedColumns++;
                }
            }
            if (unloadedSelectedColumns == 0) {
                selectedPositionsFallbackCount++;
            }
        }

        private void recordSelectionDecision(SelectedPositionsReadMode readMode)
        {
            if (unloadedSelectedColumns == 0) {
                return;
            }
            unloadedSelectedColumns--;
            if (readMode != SelectedPositionsReadMode.FULL && !selectedPositionsPushedDown) {
                selectedPositionsPushedDown = true;
                selectedPositionsPushdownCount++;
            }
            if (unloadedSelectedColumns == 0 && !selectedPositionsPushedDown) {
                selectedPositionsFallbackCount++;
            }
        }

        private void selectPositions(SelectedPositions newSelectedPositions, int[] positions, int offset, int size)
        {
            selectedPositions = newSelectedPositions;
            sizeInBytes = 0;
            retainedSizeInBytes = shallowRetainedSizeInBytes();
            for (int i = 0; i < blocks.length; i++) {
                Block block = blocks[i];
                if (block != null) {
                    // loaded blocks already reflect the previous selection, so the incoming
                    // positions apply to them directly
                    block = block.getPositions(positions, offset, size);
                    sizeInBytes += block.getSizeInBytes();
                    retainedSizeInBytes += block.getRetainedSizeInBytes();
                    blocks[i] = block;
                }
            }
        }
    }

    private record SelectedPositions(
            int originalPositionCount,
            int positionCount,
            @Nullable int[] positions,
            int positionsOffset,
            SelectionAnalysis analysis)
    {
        private static final long INSTANCE_SIZE = instanceSize(SelectedPositions.class);
        private static final long ANALYSIS_INSTANCE_SIZE = instanceSize(SelectionAnalysis.class);

        private static SelectedPositions allPositions(int positionCount)
        {
            return new SelectedPositions(positionCount, positionCount, null, 0, SelectionAnalysis.analyzed(true, positionCount == 0 ? 0 : 1, 0));
        }

        public long retainedSizeInBytes()
        {
            return INSTANCE_SIZE + ANALYSIS_INSTANCE_SIZE + sizeOf(positions);
        }

        public boolean hasPositions()
        {
            return positions != null;
        }

        public boolean strictlyAscending()
        {
            analyze();
            return analysis.strictlyAscending;
        }

        public boolean isSingleRunAtPageEdge()
        {
            return positionCount > 0
                    && selectedRunCount() == 1
                    && (positions[positionsOffset] == 0 || positions[positionsOffset + positionCount - 1] == originalPositionCount - 1);
        }

        public int selectedRunCount()
        {
            analyze();
            return analysis.selectedRunCount;
        }

        public int maxSkippedPositionCount()
        {
            analyze();
            return analysis.maxSkippedPositionCount;
        }

        public boolean mayHaveMinimumPotentialSkippedBytes(long estimatedUncompressedBytesPerValue, long minimumSkippedPageBytes)
        {
            if (positionCount == 0) {
                return hasMinimumPotentialSkippedBytes(originalPositionCount, estimatedUncompressedBytesPerValue, minimumSkippedPageBytes);
            }
            int maximumPotentialSkippedPositions = max(positions[positionsOffset], originalPositionCount - positions[positionsOffset + positionCount - 1] - 1);
            for (int index = 0; index < positionCount - 1; index += 64) {
                int nextIndex = min(index + 64, positionCount - 1);
                int skippedPositions = positions[positionsOffset + nextIndex] - positions[positionsOffset + index] - (nextIndex - index);
                maximumPotentialSkippedPositions = max(maximumPotentialSkippedPositions, skippedPositions);
            }
            return hasMinimumPotentialSkippedBytes(maximumPotentialSkippedPositions, estimatedUncompressedBytesPerValue, minimumSkippedPageBytes);
        }

        @CheckReturnValue
        public Block apply(Block block)
        {
            if (positions == null) {
                return block;
            }
            return block.getPositions(positions, positionsOffset, positionCount);
        }

        public Block createRowNumberBlock(long startRowNumber)
        {
            long[] rowNumbers = new long[positionCount];
            for (int i = 0; i < positionCount; i++) {
                int position = positions == null ? i : positions[positionsOffset + i];
                rowNumbers[i] = startRowNumber + position;
            }
            return new LongArrayBlock(positionCount, Optional.empty(), rowNumbers);
        }

        @CheckReturnValue
        public SelectedPositions selectPositions(int[] positions, int offset, int size)
        {
            checkFromIndexSize(offset, size, positions.length);
            int[] newPositions = new int[size];
            if (this.positions == null) {
                for (int i = 0; i < size; i++) {
                    int selectedPosition = positions[offset + i];
                    checkIndex(selectedPosition, positionCount);
                    newPositions[i] = selectedPosition;
                }
            }
            else {
                for (int i = 0; i < size; i++) {
                    int selectedPosition = positions[offset + i];
                    checkIndex(selectedPosition, positionCount);
                    newPositions[i] = this.positions[positionsOffset + selectedPosition];
                }
            }

            return create(originalPositionCount, newPositions, 0, size);
        }

        public SelectedPositions selectPositionsView(int[] positions, int offset, int size)
        {
            if (this.positions != null) {
                return selectPositions(positions, offset, size);
            }
            checkFromIndexSize(offset, size, positions.length);
            return new SelectedPositions(originalPositionCount, size, positions, offset, new SelectionAnalysis());
        }

        private static SelectedPositions create(int originalPositionCount, int[] positions, int offset, int size)
        {
            boolean strictlyAscending = true;
            int selectedRunCount = size == 0 ? 0 : 1;
            int maxSkippedPositionCount = size == 0 ? originalPositionCount : positions[offset];
            for (int i = 1; i < size; i++) {
                if (positions[offset + i] <= positions[offset + i - 1]) {
                    strictlyAscending = false;
                }
                else if (positions[offset + i] != positions[offset + i - 1] + 1) {
                    selectedRunCount++;
                    maxSkippedPositionCount = max(maxSkippedPositionCount, positions[offset + i] - positions[offset + i - 1] - 1);
                }
            }
            if (size > 0 && strictlyAscending) {
                maxSkippedPositionCount = max(maxSkippedPositionCount, originalPositionCount - positions[offset + size - 1] - 1);
            }
            return new SelectedPositions(originalPositionCount, size, positions, offset, SelectionAnalysis.analyzed(strictlyAscending, selectedRunCount, maxSkippedPositionCount));
        }

        private void analyze()
        {
            if (analysis.analyzed) {
                return;
            }
            checkFromIndexSize(positionsOffset, positionCount, positions.length);
            boolean strictlyAscending = true;
            int selectedRunCount = positionCount == 0 ? 0 : 1;
            int maxSkippedPositionCount = positionCount == 0 ? originalPositionCount : positions[positionsOffset];
            for (int i = 0; i < positionCount; i++) {
                checkIndex(positions[positionsOffset + i], originalPositionCount);
                if (i > 0) {
                    if (positions[positionsOffset + i] <= positions[positionsOffset + i - 1]) {
                        strictlyAscending = false;
                    }
                    else if (positions[positionsOffset + i] != positions[positionsOffset + i - 1] + 1) {
                        selectedRunCount++;
                        maxSkippedPositionCount = max(maxSkippedPositionCount, positions[positionsOffset + i] - positions[positionsOffset + i - 1] - 1);
                    }
                }
            }
            if (positionCount > 0 && strictlyAscending) {
                maxSkippedPositionCount = max(maxSkippedPositionCount, originalPositionCount - positions[positionsOffset + positionCount - 1] - 1);
            }
            analysis.set(strictlyAscending, selectedRunCount, maxSkippedPositionCount);
        }
    }

    private static final class SelectionAnalysis
    {
        private boolean analyzed;
        private boolean strictlyAscending;
        private int selectedRunCount;
        private int maxSkippedPositionCount;

        private static SelectionAnalysis analyzed(boolean strictlyAscending, int selectedRunCount, int maxSkippedPositionCount)
        {
            SelectionAnalysis analysis = new SelectionAnalysis();
            analysis.set(strictlyAscending, selectedRunCount, maxSkippedPositionCount);
            return analysis;
        }

        private void set(boolean strictlyAscending, int selectedRunCount, int maxSkippedPositionCount)
        {
            this.analyzed = true;
            this.strictlyAscending = strictlyAscending;
            this.selectedRunCount = selectedRunCount;
            this.maxSkippedPositionCount = maxSkippedPositionCount;
        }
    }

    private record SelectedPositionsPushdownCharacteristics(
            PrimitiveTypeName primitiveType,
            long estimatedUncompressedBytesPerValue,
            boolean dictionaryEncoded,
            boolean hasNulls) {}

    private enum SelectedPositionsReadMode
    {
        FULL,
        PAGE,
        ROW,
    }

    private SelectedPositionsReadMode getSelectedPositionsReadMode(Field field, SelectedPositions selectedPositions)
            throws IOException
    {
        if (!selectedPositions.hasPositions()) {
            return SelectedPositionsReadMode.FULL;
        }
        if (!(field instanceof PrimitiveField primitiveField)) {
            return SelectedPositionsReadMode.FULL;
        }
        if (primitiveField.getDescriptor().getPath().length != 1
                || primitiveField.getRepetitionLevel() != 0
                || !columnReaders.get(primitiveField.getId()).supportsSelectedPositions()) {
            return SelectedPositionsReadMode.FULL;
        }
        if (forceSelectedPositionsPushdown) {
            return selectedPositions.strictlyAscending() ? SelectedPositionsReadMode.ROW : SelectedPositionsReadMode.FULL;
        }
        SelectedPositionsPushdownCharacteristics characteristics = selectedPositionsPushdownCharacteristics.get(primitiveField.getId());
        if (characteristics == null) {
            ColumnChunkMetadata metadata = currentBlockMetadata.getColumnChunkMetaData(primitiveField.getDescriptor());
            long valueCount = metadata.getValueCount();
            boolean hasNulls = !primitiveField.isRequired()
                    && (metadata.getStatistics() == null
                    || !metadata.getStatistics().isNumNullsSet()
                    || metadata.getStatistics().getNumNulls() > 0);
            characteristics = new SelectedPositionsPushdownCharacteristics(
                    primitiveField.getDescriptor().getPrimitiveType().getPrimitiveTypeName(),
                    valueCount == 0 ? 1 : max(1, metadata.getTotalUncompressedSize() / valueCount),
                    isOnlyDictionaryEncodingPages(metadata),
                    hasNulls);
            selectedPositionsPushdownCharacteristics.put(primitiveField.getId(), characteristics);
        }
        boolean potentiallyRowSelectionBeneficial = isRowSelectionBeneficial(
                batchSize,
                selectedPositions.positionCount(),
                0,
                characteristics.primitiveType(),
                characteristics.dictionaryEncoded(),
                characteristics.hasNulls(),
                characteristics.estimatedUncompressedBytesPerValue(),
                columnFields.size());
        boolean potentiallyPageBeneficial = selectedPercentageAtMost(batchSize, selectedPositions.positionCount(), MAX_PAGE_AWARE_SELECTED_PERCENTAGE)
                && selectedPositions.mayHaveMinimumPotentialSkippedBytes(characteristics.estimatedUncompressedBytesPerValue(), MIN_SKIPPED_PAGE_BYTES);
        if ((!potentiallyRowSelectionBeneficial && !potentiallyPageBeneficial) || !selectedPositions.strictlyAscending()) {
            return SelectedPositionsReadMode.FULL;
        }

        boolean rowSelectionBeneficial = isRowSelectionBeneficial(
                batchSize,
                selectedPositions.positionCount(),
                selectedPositions.selectedRunCount(),
                characteristics.primitiveType(),
                characteristics.dictionaryEncoded(),
                characteristics.hasNulls(),
                characteristics.estimatedUncompressedBytesPerValue(),
                columnFields.size());
        if (rowSelectionBeneficial && !selectedPositions.isSingleRunAtPageEdge()) {
            return SelectedPositionsReadMode.ROW;
        }

        potentiallyPageBeneficial &= hasMinimumPotentialSkippedBytes(
                selectedPositions.maxSkippedPositionCount(),
                characteristics.estimatedUncompressedBytesPerValue(),
                MIN_SKIPPED_PAGE_BYTES);
        if (!potentiallyPageBeneficial) {
            return SelectedPositionsReadMode.FULL;
        }

        ColumnReader columnReader = initializeColumnReader(primitiveField);
        long skippedPageBytes = columnReader.preparePageFilteredRead(
                selectedPositions.positions(),
                selectedPositions.positionsOffset(),
                selectedPositions.positionCount(),
                options.getMaxReadBlockSize().toBytes());
        return skippedPageBytes >= MIN_SKIPPED_PAGE_BYTES ? SelectedPositionsReadMode.PAGE : SelectedPositionsReadMode.FULL;
    }

    @VisibleForTesting
    static boolean isRowSelectionBeneficial(
            int batchSize,
            int selectedPositionCount,
            int selectedRunCount,
            PrimitiveTypeName primitiveType,
            boolean dictionaryEncoded,
            boolean hasNulls,
            long estimatedUncompressedBytesPerValue,
            int projectedColumnCount)
    {
        checkArgument(batchSize >= 0, "batchSize is negative");
        checkArgument(selectedPositionCount >= 0 && selectedPositionCount <= batchSize, "selectedPositionCount is invalid");
        checkArgument(selectedRunCount >= 0 && selectedRunCount <= selectedPositionCount, "selectedRunCount is invalid");
        requireNonNull(primitiveType, "primitiveType is null");
        checkArgument(estimatedUncompressedBytesPerValue > 0, "estimatedUncompressedBytesPerValue must be positive");
        checkArgument(projectedColumnCount > 0, "projectedColumnCount must be positive");

        if (selectedPositionCount == 0) {
            return true;
        }

        int skippedPositionCount = batchSize - selectedPositionCount;
        if (!hasMinimumSkippedPositionsPerRun(skippedPositionCount, selectedRunCount, MIN_SKIPPED_POSITIONS_PER_RUN)) {
            return false;
        }
        if (primitiveType == PrimitiveTypeName.BOOLEAN) {
            return false;
        }
        if (hasNulls) {
            return primitiveType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                    && selectedPercentageAtMost(batchSize, selectedPositionCount, MAX_NULLABLE_FIXED_LEN_BYTE_ARRAY_SELECTED_PERCENTAGE);
        }
        if (dictionaryEncoded) {
            return selectedPercentageAtMost(batchSize, selectedPositionCount, MAX_DICTIONARY_SELECTED_PERCENTAGE);
        }
        if (primitiveType == BINARY) {
            if (estimatedUncompressedBytesPerValue >= VERY_WIDE_BINARY_BYTES
                    && projectedColumnCount > MAX_VERY_WIDE_BINARY_PROJECTED_COLUMNS) {
                return false;
            }
            return selectedPercentageAtMost(batchSize, selectedPositionCount, MAX_BINARY_SELECTED_PERCENTAGE);
        }
        return selectedPercentageAtMost(batchSize, selectedPositionCount, MAX_FIXED_WIDTH_SELECTED_PERCENTAGE);
    }

    private static boolean selectedPercentageAtMost(int batchSize, int selectedPositionCount, int percentage)
    {
        return selectedPositionCount <= ((long) batchSize * percentage + 50) / 100;
    }

    private static boolean hasMinimumSkippedPositionsPerRun(int skippedPositionCount, int selectedRunCount, int minimumSkippedPositionsPerRun)
    {
        return (long) skippedPositionCount >= (long) selectedRunCount * minimumSkippedPositionsPerRun;
    }

    private static boolean hasMinimumPotentialSkippedBytes(int skippedPositionCount, long estimatedUncompressedBytesPerValue, long minimumSkippedPageBytes)
    {
        return skippedPositionCount > 0
                && estimatedUncompressedBytesPerValue >= (minimumSkippedPageBytes + skippedPositionCount - 1) / skippedPositionCount;
    }

    /**
     * Get the global row index of the first row in the last batch.
     */
    public long lastBatchStartRow()
    {
        return firstRowIndexInGroup + nextRowInGroup - batchSize;
    }

    @VisibleForTesting
    public int getDataPageReadCount()
    {
        return completedRowGroupDataPageReadCount + columnReaders.values().stream()
                .mapToInt(ColumnReader::getDataPageReadCount)
                .sum();
    }

    @VisibleForTesting
    public int getSelectedPositionsFallbackCount()
    {
        return selectedPositionsFallbackCount;
    }

    @VisibleForTesting
    public int getSelectedPositionsPushdownCount()
    {
        return selectedPositionsPushdownCount;
    }

    private int nextBatch()
            throws IOException
    {
        if (nextRowInGroup >= currentGroupRowCount && !advanceToNextRowGroup()) {
            return -1;
        }

        batchSize = min(nextBatchSize, maxBatchSize);
        nextBatchSize = min(batchSize * BATCH_SIZE_GROWTH_FACTOR, options.getMaxReadBlockRowCount());
        batchSize = toIntExact(min(batchSize, currentGroupRowCount - nextRowInGroup));

        nextRowInGroup += batchSize;
        columnReaders.values().forEach(reader -> reader.prepareNextRead(batchSize));
        return batchSize;
    }

    private boolean advanceToNextRowGroup()
            throws IOException
    {
        currentRowGroupMemoryContext.close();
        currentRowGroupMemoryContext = memoryContext.newAggregatedMemoryContext();
        freeCurrentRowGroupBuffers();

        if (currentRowGroup >= 0 && rowGroupStatisticsValidation.isPresent()) {
            StatisticsValidation statisticsValidation = rowGroupStatisticsValidation.get();
            writeValidation.orElseThrow().validateRowGroupStatistics(dataSource.getId(), currentBlockMetadata, statisticsValidation.build());
            statisticsValidation.reset();
        }

        currentRowGroup++;
        if (currentRowGroup == rowGroups.size()) {
            return false;
        }
        RowGroupInfo rowGroupInfo = rowGroups.get(currentRowGroup);
        currentBlockMetadata = rowGroupInfo.prunedBlockMetadata();
        firstRowIndexInGroup = rowGroupInfo.fileRowOffset();
        currentGroupRowCount = currentBlockMetadata.getRowCount();
        FilteredRowRanges currentGroupRowRanges = blockRowRanges[currentRowGroup];
        log.debug("advanceToNextRowGroup dataSource %s, currentRowGroup %d, rowRanges %s, currentBlockMetadata %s", dataSource.getId(), currentRowGroup, currentGroupRowRanges, currentBlockMetadata);
        if (currentGroupRowRanges != null) {
            long rowCount = currentGroupRowRanges.getRowCount();
            columnIndexRowsFiltered += currentGroupRowCount - rowCount;
            if (rowCount == 0) {
                // Filters on multiple columns with page indexes may yield non-overlapping row ranges and eliminate the entire row group.
                // Advance to next row group to ensure that we don't return a null Page and close the page source before all row groups are processed
                return advanceToNextRowGroup();
            }
            currentGroupRowCount = rowCount;
        }
        nextRowInGroup = 0L;
        initializeColumnReaders();
        return true;
    }

    private void freeCurrentRowGroupBuffers()
    {
        if (currentRowGroup < 0) {
            return;
        }

        for (PrimitiveField field : primitiveFields) {
            ChunkedInputStream chunkedStream = chunkReaders.get(new ChunkKey(field.getId(), currentRowGroup));
            if (chunkedStream != null) {
                chunkedStream.close();
            }
        }
    }

    private ColumnChunk readVariant(VariantField field)
            throws IOException
    {
        ColumnChunk metadataChunk = readColumnChunk(field.getMetadata());
        ColumnChunk valueChunk = readColumnChunk(field.getValue());

        // position count and nulls are derived from metadata def levels
        int positionsCount = metadataChunk.getDefinitionLevels().length;
        int variantDefLevel = field.getDefinitionLevel();
        long[] valueIsValid = null;
        for (int i = 0; i < positionsCount; i++) {
            if (metadataChunk.getDefinitionLevels()[i] >= variantDefLevel) {
                if (valueIsValid != null) {
                    set(valueIsValid, 0, i);
                }
            }
            else if (valueIsValid == null) {
                valueIsValid = new long[wordsForBits(positionsCount)];
                for (int position = 0; position < i; position++) {
                    set(valueIsValid, 0, position);
                }
            }
        }

        // if isNull is present, we need to convert the blocks to not-null-suppressed blocks
        Block metadataBlock = metadataChunk.getBlock();
        Block valueBlock = valueChunk.getBlock();
        if (valueIsValid != null) {
            metadataBlock = toNotNullSupressedBlock(positionsCount, valueIsValid, metadataBlock);
            valueBlock = toNotNullSupressedBlock(positionsCount, valueIsValid, valueBlock);
        }

        Block variantBlock = VariantBlock.create(positionsCount, metadataBlock, valueBlock, Optional.ofNullable(valueIsValid));
        return new ColumnChunk(variantBlock, metadataChunk.getDefinitionLevels(), metadataChunk.getRepetitionLevels());
    }

    private ColumnChunk readVariantAsJson(VariantField field)
            throws IOException
    {
        ColumnChunk metadataChunk = readColumnChunk(field.getMetadata());

        int positionCount = metadataChunk.getBlock().getPositionCount();
        BlockBuilder variantBlock = VARCHAR.createBlockBuilder(null, max(1, positionCount));
        ColumnChunk valueChunk = readColumnChunk(field.getValue());
        for (int position = 0; position < positionCount; position++) {
            Slice metadata = VARBINARY.getSlice(metadataChunk.getBlock(), position);
            if (metadata.length() == 0) {
                variantBlock.appendNull();
                continue;
            }
            Slice value = VARBINARY.getSlice(valueChunk.getBlock(), position);
            Variant variant = new Variant(value.getBytes(), metadata.getBytes());
            VARCHAR.writeSlice(variantBlock, utf8Slice(variant.toJson(zoneId)));
        }
        return new ColumnChunk(variantBlock.build(), metadataChunk.getDefinitionLevels(), metadataChunk.getRepetitionLevels());
    }

    private ColumnChunk readArray(GroupField field)
            throws IOException
    {
        checkArgument(field.getType() instanceof ArrayType, "Expected array type, found: %s", field.getType());
        Optional<Field> children = field.getChildren().get(0);
        if (children.isEmpty()) {
            return new ColumnChunk(field.getType().createNullBlock(), new int[] {}, new int[] {});
        }
        Field elementField = children.get();
        ColumnChunk columnChunk = readColumnChunk(elementField);

        ListColumnReader.BlockPositions collectionPositions = calculateCollectionOffsets(field, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        int positionsCount = collectionPositions.offsets().length - 1;
        Block arrayBlock = ArrayBlock.fromElementBlock(positionsCount, collectionPositions.valueIsValid(), collectionPositions.offsets(), columnChunk.getBlock());
        return new ColumnChunk(arrayBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
    }

    private ColumnChunk readMap(GroupField field)
            throws IOException
    {
        if (!(field.getType() instanceof MapType mapType)) {
            throw new ParquetCorruptionException(dataSource.getId(), "Expected map type, found: %s", field.getType());
        }

        Block[] blocks = new Block[2];

        ColumnChunk columnChunk = readColumnChunk(field.getChildren().get(0).get());
        blocks[0] = columnChunk.getBlock();
        Optional<Field> valueField = field.getChildren().get(1);
        blocks[1] = valueField.isPresent() ? readColumnChunk(valueField.get()).getBlock() : mapType.getValueType().createNullBlock();
        ListColumnReader.BlockPositions collectionPositions = calculateCollectionOffsets(field, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        Block mapBlock = ((MapType) field.getType()).createBlockFromKeyValue(collectionPositions.valueIsValid(), collectionPositions.offsets(), blocks[0], blocks[1]);
        return new ColumnChunk(mapBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
    }

    private ColumnChunk readStruct(GroupField field)
            throws IOException
    {
        RowType rowType = (RowType) field.getType();
        Block[] blocks = new Block[rowType.getFields().size()];
        ColumnChunk columnChunk = null;
        List<Optional<Field>> parameters = field.getChildren();
        for (int i = 0; i < blocks.length; i++) {
            Optional<Field> parameter = parameters.get(i);
            if (parameter.isPresent()) {
                columnChunk = readColumnChunk(parameter.get());
                blocks[i] = columnChunk.getBlock();
            }
        }

        if (columnChunk == null) {
            throw new ParquetCorruptionException(dataSource.getId(), "Struct field does not have any children: %s", field);
        }

        StructColumnReader.RowBlockPositions structIsNull = StructColumnReader.calculateStructOffsets(field, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        Optional<long[]> valueIsValid = structIsNull.valueIsValid();
        for (int i = 0; i < blocks.length; i++) {
            if (blocks[i] == null) {
                blocks[i] = RunLengthEncodedBlock.create(rowType.getFields().get(i).getType(), null, structIsNull.positionsCount());
            }
            else if (valueIsValid.isPresent()) {
                blocks[i] = toNotNullSupressedBlock(structIsNull.positionsCount(), valueIsValid.get(), blocks[i]);
            }
        }
        Block rowBlock = RowBlock.fromNotNullSuppressedFieldBlocks(structIsNull.positionsCount(), structIsNull.valueIsValid(), blocks);
        return new ColumnChunk(rowBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
    }

    private static Block toNotNullSupressedBlock(int positionCount, long[] rowIsValid, Block fieldBlock)
    {
        // find a existing position in the block that is null
        int nullIndex = -1;
        if (fieldBlock.mayHaveNull()) {
            for (int position = 0; position < fieldBlock.getPositionCount(); position++) {
                if (fieldBlock.isNull(position)) {
                    nullIndex = position;
                    break;
                }
            }
        }
        // if there are no null positions, append a null to the end of the block
        if (nullIndex == -1) {
            nullIndex = fieldBlock.getPositionCount();
            fieldBlock = fieldBlock.copyWithAppendedNull();
        }

        // create a dictionary that maps null positions to the null index
        int[] dictionaryIds = new int[positionCount];
        int nullSuppressedPosition = 0;
        for (int position = 0; position < positionCount; position++) {
            if (isSet(rowIsValid, 0, position)) {
                dictionaryIds[position] = nullSuppressedPosition;
                nullSuppressedPosition++;
            }
            else {
                dictionaryIds[position] = nullIndex;
            }
        }
        return DictionaryBlock.create(positionCount, fieldBlock, dictionaryIds);
    }

    @Nullable
    private FilteredOffsetIndex getFilteredOffsetIndex(FilteredRowRanges rowRanges, int rowGroup, long rowGroupRowCount, ColumnPath columnPath)
    {
        Optional<ColumnIndexStore> rowGroupColumnIndexStore = this.rowGroups.get(rowGroup).columnIndexStore();
        if (rowGroupColumnIndexStore.isEmpty()) {
            return null;
        }
        // We have a selective rowRanges for the rowGroup, every column must have a valid offset index
        // to figure out which rows need to be read from the required parquet pages
        OffsetIndex offsetIndex = requireNonNull(
                rowGroupColumnIndexStore.get().getOffsetIndex(columnPath),
                format("Missing OffsetIndex for column %s", columnPath));
        return FilteredOffsetIndex.filterOffsetIndex(offsetIndex, rowRanges.getParquetRowRanges(), rowGroupRowCount);
    }

    private ColumnChunk readPrimitive(PrimitiveField field)
            throws IOException
    {
        return readPrimitive(field, null);
    }

    private ColumnChunk readPrimitive(PrimitiveField field, @Nullable SelectedPositions selectedPositions)
            throws IOException
    {
        ColumnReader columnReader = initializeColumnReader(field);
        ColumnChunk columnChunk;
        if (selectedPositions != null && selectedPositions.positions() != null) {
            columnChunk = columnReader.readPrimitive(selectedPositions.positions(), selectedPositions.positionsOffset(), selectedPositions.positionCount());
        }
        else {
            columnChunk = columnReader.readPrimitive();
        }

        updateMaxBytesPerCell(field.getId(), columnChunk);
        return columnChunk;
    }

    private ColumnChunk readPrimitivePageFiltered(PrimitiveField field, SelectedPositions selectedPositions)
            throws IOException
    {
        ColumnReader columnReader = initializeColumnReader(field);
        ColumnChunk columnChunk = columnReader.readPrimitivePageFiltered(
                selectedPositions.positions(),
                selectedPositions.positionsOffset(),
                selectedPositions.positionCount());
        updateMaxBytesPerCell(field.getId(), columnChunk);
        return columnChunk;
    }

    private void updateMaxBytesPerCell(int fieldId, ColumnChunk columnChunk)
    {
        if (columnChunk.getMaxBlockPositionCount() == 0) {
            return;
        }
        double previousBytesPerCell = maxBytesPerCell.getOrDefault(fieldId, 0.0);
        double bytesPerCell = max(previousBytesPerCell, ((double) columnChunk.getMaxBlockSize()) / columnChunk.getMaxBlockPositionCount());

        if (bytesPerCell != previousBytesPerCell) {
            maxBytesPerCell.put(fieldId, bytesPerCell);
            maxCombinedBytesPerRow = max(0, maxCombinedBytesPerRow + bytesPerCell - previousBytesPerCell);
            maxBatchSize = maxCombinedBytesPerRow == 0
                    ? options.getMaxReadBlockRowCount()
                    : toIntExact(min(options.getMaxReadBlockRowCount(), max(1, (long) (options.getMaxReadBlockSize().toBytes() / maxCombinedBytesPerRow))));
        }
    }

    private ColumnReader initializeColumnReader(PrimitiveField field)
            throws IOException
    {
        ColumnDescriptor columnDescriptor = field.getDescriptor();
        int fieldId = field.getId();
        ColumnReader columnReader = columnReaders.get(fieldId);
        if (!columnReader.hasPageReader()) {
            validateParquet(currentBlockMetadata.getRowCount() > 0, dataSource.getId(), "Row group has 0 rows");
            ColumnChunkMetadata metadata = currentBlockMetadata.getColumnChunkMetaData(columnDescriptor);
            FilteredRowRanges rowRanges = blockRowRanges[currentRowGroup];
            OffsetIndex offsetIndex = null;
            if (rowRanges != null) {
                offsetIndex = getFilteredOffsetIndex(rowRanges, currentRowGroup, currentBlockMetadata.getRowCount(), metadata.getPath());
            }
            ChunkedInputStream columnChunkInputStream = chunkReaders.get(new ChunkKey(fieldId, currentRowGroup));
            columnReader.setPageReader(
                    createPageReader(
                            dataSource.getId(),
                            columnChunkInputStream,
                            metadata,
                            columnDescriptor,
                            offsetIndex,
                            fileCreatedBy,
                            decryptionContext,
                            options.getMaxPageReadSize().toBytes()),
                    Optional.ofNullable(rowRanges));
        }
        return columnReader;
    }

    public List<Column> getColumnFields()
    {
        return columnFields;
    }

    public Metrics getMetrics()
    {
        ImmutableMap.Builder<String, Metric<?>> metrics = ImmutableMap.<String, Metric<?>>builder()
                .putAll(codecMetrics);
        if (columnIndexRowsFiltered >= 0) {
            metrics.put(COLUMN_INDEX_ROWS_FILTERED, new LongCount(columnIndexRowsFiltered));
        }
        metrics.putAll(dataSource.getMetrics().getMetrics());

        return new Metrics(metrics.buildOrThrow());
    }

    private void initializeColumnReaders()
    {
        completedRowGroupDataPageReadCount += columnReaders.values().stream()
                .mapToInt(ColumnReader::getDataPageReadCount)
                .sum();
        selectedPositionsPushdownCharacteristics.clear();
        for (PrimitiveField field : primitiveFields) {
            columnReaders.put(
                    field.getId(),
                    columnReaderFactory.create(field, currentRowGroupMemoryContext));
        }
    }

    public static List<PrimitiveField> getPrimitiveFields(List<Field> fields)
    {
        Map<Integer, PrimitiveField> primitiveFields = new HashMap<>();
        fields.forEach(field -> parseField(field, primitiveFields));

        return ImmutableList.copyOf(primitiveFields.values());
    }

    private static void parseField(Field field, Map<Integer, PrimitiveField> primitiveFields)
    {
        if (field instanceof PrimitiveField primitiveField) {
            primitiveFields.put(primitiveField.getId(), primitiveField);
        }
        else if (field instanceof GroupField groupField) {
            groupField.getChildren().stream()
                    .flatMap(Optional::stream)
                    .forEach(child -> parseField(child, primitiveFields));
        }
        else if (field instanceof VariantField variantField) {
            parseField(variantField.getValue(), primitiveFields);
            parseField(variantField.getMetadata(), primitiveFields);
        }
    }

    public Block readBlock(Field field)
            throws IOException
    {
        return readColumnChunk(field).getBlock();
    }

    private ColumnChunk readColumnChunk(Field field)
            throws IOException
    {
        ColumnChunk columnChunk;
        if (field instanceof VariantField variantField) {
            if (variantField.getType() == VARIANT) {
                // Directly read VARIANT as a single block
                columnChunk = readVariant(variantField);
            }
            else {
                columnChunk = readVariantAsJson(variantField);
            }
        }
        else if (field.getType() instanceof RowType) {
            columnChunk = readStruct((GroupField) field);
        }
        else if (field.getType() instanceof MapType) {
            columnChunk = readMap((GroupField) field);
        }
        else if (field.getType() instanceof ArrayType) {
            columnChunk = readArray((GroupField) field);
        }
        else {
            columnChunk = readPrimitive((PrimitiveField) field);
        }
        return columnChunk;
    }

    public ParquetDataSource getDataSource()
    {
        return dataSource;
    }

    public AggregatedMemoryContext getMemoryContext()
    {
        return memoryContext;
    }

    private static FilteredRowRanges[] calculateFilteredRowRanges(
            List<RowGroupInfo> rowGroups,
            Optional<FilterPredicate> filter,
            List<PrimitiveField> primitiveFields)
    {
        FilteredRowRanges[] blockRowRanges = new FilteredRowRanges[rowGroups.size()];
        if (filter.isEmpty()) {
            return blockRowRanges;
        }
        Set<ColumnPath> paths = primitiveFields.stream()
                .map(field -> ColumnPath.get(field.getDescriptor().getPath()))
                .collect(toImmutableSet());
        for (int rowGroup = 0; rowGroup < rowGroups.size(); rowGroup++) {
            RowGroupInfo rowGroupInfo = rowGroups.get(rowGroup);
            Optional<ColumnIndexStore> rowGroupColumnIndexStore = rowGroupInfo.columnIndexStore();
            if (rowGroupColumnIndexStore.isEmpty()) {
                continue;
            }
            long rowGroupRowCount = rowGroupInfo.prunedBlockMetadata().getRowCount();
            FilteredRowRanges rowRanges = new FilteredRowRanges(ColumnIndexFilter.calculateRowRanges(
                    FilterCompat.get(filter.get()),
                    rowGroupColumnIndexStore.get(),
                    paths,
                    rowGroupRowCount));
            if (rowRanges.getRowCount() < rowGroupRowCount) {
                blockRowRanges[rowGroup] = rowRanges;
            }
        }
        return blockRowRanges;
    }

    private void validateWritePageChecksum(SourcePage sourcePage)
    {
        if (writeChecksumBuilder.isPresent()) {
            Page page = sourcePage.getPage();
            writeChecksumBuilder.get().addPage(page);
            rowGroupStatisticsValidation.orElseThrow().addPage(page);
        }
    }

    private void validateBlockMetadata(List<RowGroupInfo> rowGroups)
            throws ParquetCorruptionException
    {
        if (writeValidation.isPresent()) {
            writeValidation.get().validateBlocksMetadata(dataSource.getId(), rowGroups);
        }
    }

    @SuppressWarnings("FormatStringAnnotation")
    @FormatMethod
    private void validateWrite(Predicate<ParquetWriteValidation> test, String messageFormat, Object... args)
            throws ParquetCorruptionException
    {
        if (writeValidation.isPresent() && !test.test(writeValidation.get())) {
            throw new ParquetCorruptionException(dataSource.getId(), "Write validation failed: " + messageFormat, args);
        }
    }
}
