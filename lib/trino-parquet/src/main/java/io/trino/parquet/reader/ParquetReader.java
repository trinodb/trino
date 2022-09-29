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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slice;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.ChunkKey;
import io.trino.parquet.ChunkReader;
import io.trino.parquet.DiskRange;
import io.trino.parquet.Field;
import io.trino.parquet.GroupField;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetWriteValidation;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.predicate.Predicate;
import io.trino.parquet.reader.FilteredOffsetIndex.OffsetRange;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetValidationUtils.validateParquet;
import static io.trino.parquet.ParquetWriteValidation.StatisticsValidation;
import static io.trino.parquet.ParquetWriteValidation.StatisticsValidation.createStatisticsValidationBuilder;
import static io.trino.parquet.ParquetWriteValidation.WriteChecksumBuilder;
import static io.trino.parquet.ParquetWriteValidation.WriteChecksumBuilder.createWriteChecksumBuilder;
import static io.trino.parquet.reader.ListColumnReader.calculateCollectionOffsets;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class ParquetReader
        implements Closeable
{
    private static final int MAX_VECTOR_LENGTH = 1024;
    private static final int INITIAL_BATCH_SIZE = 1;
    private static final int BATCH_SIZE_GROWTH_FACTOR = 2;
    public static final String PARQUET_CODEC_METRIC_PREFIX = "ParquetReaderCompressionFormat_";
    public static final String COLUMN_INDEX_ROWS_FILTERED = "ParquetColumnIndexRowsFiltered";

    private final Optional<String> fileCreatedBy;
    private final List<BlockMetaData> blocks;
    private final List<Long> firstRowsOfBlocks;
    private final List<Field> columnFields;
    private final List<PrimitiveField> primitiveFields;
    private final ParquetDataSource dataSource;
    private final DateTimeZone timeZone;
    private final AggregatedMemoryContext memoryContext;
    private final Optional<FilterPredicate> filter;

    private int currentRowGroup = -1;
    private BlockMetaData currentBlockMetadata;
    private long currentGroupRowCount;
    /**
     * Index in the Parquet file of the first row of the current group
     */
    private long firstRowIndexInGroup;
    /**
     * Index in the current group of the next row
     */
    private RowRanges currentGroupRowRanges;
    private long nextRowInGroup;
    private int batchSize;
    private int nextBatchSize = INITIAL_BATCH_SIZE;
    private final Map<Integer, PrimitiveColumnReader> columnReaders;
    private final Map<Integer, Long> maxBytesPerCell;
    private long maxCombinedBytesPerRow;
    private final ParquetReaderOptions options;
    private int maxBatchSize = MAX_VECTOR_LENGTH;

    private AggregatedMemoryContext currentRowGroupMemoryContext;
    private final Multimap<ChunkKey, ChunkReader> chunkReaders;
    private final List<Optional<ColumnIndexStore>> columnIndexStore;
    private final Optional<ParquetWriteValidation> writeValidation;
    private final Optional<WriteChecksumBuilder> writeChecksumBuilder;
    private final Optional<StatisticsValidation> rowGroupStatisticsValidation;
    private final List<RowRanges> blockRowRanges;
    private final Map<ColumnPath, ColumnDescriptor> paths = new HashMap<>();
    private final ParquetBlockFactory blockFactory;
    private final Map<String, Metric<?>> codecMetrics;

    private long columnIndexRowsFiltered = -1;

    public ParquetReader(
            Optional<String> fileCreatedBy,
            List<Field> columnFields,
            List<BlockMetaData> blocks,
            List<Long> firstRowsOfBlocks,
            ParquetDataSource dataSource,
            DateTimeZone timeZone,
            AggregatedMemoryContext memoryContext,
            ParquetReaderOptions options,
            Function<Exception, RuntimeException> exceptionTransform)
            throws IOException
    {
        this(fileCreatedBy, columnFields, blocks, firstRowsOfBlocks, dataSource, timeZone, memoryContext, options, exceptionTransform, Optional.empty(), nCopies(blocks.size(), Optional.empty()), Optional.empty());
    }

    public ParquetReader(
            Optional<String> fileCreatedBy,
            List<Field> columnFields,
            List<BlockMetaData> blocks,
            List<Long> firstRowsOfBlocks,
            ParquetDataSource dataSource,
            DateTimeZone timeZone,
            AggregatedMemoryContext memoryContext,
            ParquetReaderOptions options,
            Function<Exception, RuntimeException> exceptionTransform,
            Optional<Predicate> parquetPredicate,
            List<Optional<ColumnIndexStore>> columnIndexStore,
            Optional<ParquetWriteValidation> writeValidation)
            throws IOException
    {
        this.fileCreatedBy = requireNonNull(fileCreatedBy, "fileCreatedBy is null");
        requireNonNull(columnFields, "columnFields is null");
        this.columnFields = ImmutableList.copyOf(columnFields);
        this.primitiveFields = getPrimitiveFields(columnFields);
        this.blocks = requireNonNull(blocks, "blocks is null");
        this.firstRowsOfBlocks = requireNonNull(firstRowsOfBlocks, "firstRowsOfBlocks is null");
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.currentRowGroupMemoryContext = memoryContext.newAggregatedMemoryContext();
        this.options = requireNonNull(options, "options is null");
        this.columnReaders = new HashMap<>();
        this.maxBytesPerCell = new HashMap<>();

        checkArgument(blocks.size() == firstRowsOfBlocks.size(), "elements of firstRowsOfBlocks must correspond to blocks");

        this.writeValidation = requireNonNull(writeValidation, "writeValidation is null");
        validateWrite(
                validation -> fileCreatedBy.equals(Optional.of(validation.getCreatedBy())),
                "Expected created by %s, found %s",
                writeValidation.map(ParquetWriteValidation::getCreatedBy),
                fileCreatedBy);
        validateBlockMetadata(blocks);
        this.writeChecksumBuilder = writeValidation.map(validation -> createWriteChecksumBuilder(validation.getTypes()));
        this.rowGroupStatisticsValidation = writeValidation.map(validation -> createStatisticsValidationBuilder(validation.getTypes()));

        this.blockRowRanges = listWithNulls(this.blocks.size());
        for (PrimitiveField field : primitiveFields) {
            ColumnDescriptor columnDescriptor = field.getDescriptor();
            this.paths.put(ColumnPath.get(columnDescriptor.getPath()), columnDescriptor);
        }

        requireNonNull(parquetPredicate, "parquetPredicate is null");
        this.columnIndexStore = requireNonNull(columnIndexStore, "columnIndexStore is null");
        if (parquetPredicate.isPresent() && options.isUseColumnIndex()) {
            this.filter = parquetPredicate.get().toParquetFilter(timeZone);
        }
        else {
            this.filter = Optional.empty();
        }
        this.blockFactory = new ParquetBlockFactory(exceptionTransform);
        ListMultimap<ChunkKey, DiskRange> ranges = ArrayListMultimap.create();
        Map<String, LongCount> codecMetrics = new HashMap<>();
        for (int rowGroup = 0; rowGroup < blocks.size(); rowGroup++) {
            BlockMetaData metadata = blocks.get(rowGroup);
            for (PrimitiveField field : primitiveFields) {
                int columnId = field.getId();
                ColumnChunkMetaData chunkMetadata = getColumnChunkMetaData(metadata, field.getDescriptor());
                ColumnPath columnPath = chunkMetadata.getPath();
                long rowGroupRowCount = metadata.getRowCount();
                long startingPosition = chunkMetadata.getStartingPos();
                long totalLength = chunkMetadata.getTotalSize();
                long totalDataSize = 0;
                FilteredOffsetIndex filteredOffsetIndex = getFilteredOffsetIndex(rowGroup, rowGroupRowCount, columnPath);
                if (filteredOffsetIndex == null) {
                    DiskRange range = new DiskRange(startingPosition, toIntExact(totalLength));
                    totalDataSize = range.getLength();
                    ranges.put(new ChunkKey(columnId, rowGroup), range);
                }
                else {
                    List<OffsetRange> offsetRanges = filteredOffsetIndex.calculateOffsetRanges(startingPosition);
                    for (OffsetRange offsetRange : offsetRanges) {
                        DiskRange range = new DiskRange(offsetRange.getOffset(), toIntExact(offsetRange.getLength()));
                        totalDataSize += range.getLength();
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
        this.chunkReaders = dataSource.planRead(ranges);
    }

    @Override
    public void close()
            throws IOException
    {
        freeCurrentRowGroupBuffers();
        currentRowGroupMemoryContext.close();
        dataSource.close();

        if (writeChecksumBuilder.isPresent()) {
            ParquetWriteValidation parquetWriteValidation = writeValidation.orElseThrow();
            parquetWriteValidation.validateChecksum(dataSource.getId(), writeChecksumBuilder.get().build());
        }
    }

    public Page nextPage()
            throws IOException
    {
        int batchSize = nextBatch();
        if (batchSize <= 0) {
            return null;
        }
        // create a lazy page
        blockFactory.nextPage();
        Block[] blocks = new Block[columnFields.size()];
        for (int channel = 0; channel < columnFields.size(); channel++) {
            Field field = columnFields.get(channel);
            blocks[channel] = blockFactory.createBlock(batchSize, () -> readBlock(field));
        }
        Page page = new Page(batchSize, blocks);
        validateWritePageChecksum(page);
        return page;
    }

    /**
     * Get the global row index of the first row in the last batch.
     */
    public long lastBatchStartRow()
    {
        return firstRowIndexInGroup + nextRowInGroup - batchSize;
    }

    private int nextBatch()
            throws IOException
    {
        if (nextRowInGroup >= currentGroupRowCount && !advanceToNextRowGroup()) {
            return -1;
        }

        batchSize = min(nextBatchSize, maxBatchSize);
        nextBatchSize = min(batchSize * BATCH_SIZE_GROWTH_FACTOR, MAX_VECTOR_LENGTH);
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
        if (currentRowGroup == blocks.size()) {
            return false;
        }
        currentBlockMetadata = blocks.get(currentRowGroup);
        firstRowIndexInGroup = firstRowsOfBlocks.get(currentRowGroup);
        currentGroupRowCount = currentBlockMetadata.getRowCount();
        if (filter.isPresent() && options.isUseColumnIndex()) {
            if (columnIndexStore.get(currentRowGroup).isPresent()) {
                currentGroupRowRanges = getRowRanges(filter.get(), currentRowGroup);
                long rowCount = currentGroupRowRanges.rowCount();
                columnIndexRowsFiltered += currentGroupRowCount - rowCount;
                if (rowCount == 0) {
                    return false;
                }
                currentGroupRowCount = rowCount;
            }
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

        for (int column = 0; column < primitiveFields.size(); column++) {
            Collection<ChunkReader> readers = chunkReaders.get(new ChunkKey(column, currentRowGroup));
            if (readers != null) {
                for (ChunkReader reader : readers) {
                    reader.free();
                }
            }
        }
    }

    private ColumnChunk readArray(GroupField field)
            throws IOException
    {
        List<Type> parameters = field.getType().getTypeParameters();
        checkArgument(parameters.size() == 1, "Arrays must have a single type parameter, found %s", parameters.size());
        Field elementField = field.getChildren().get(0).get();
        ColumnChunk columnChunk = readColumnChunk(elementField);
        IntList offsets = new IntArrayList();
        BooleanList valueIsNull = new BooleanArrayList();

        calculateCollectionOffsets(field, offsets, valueIsNull, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        Block arrayBlock = ArrayBlock.fromElementBlock(valueIsNull.size(), Optional.of(valueIsNull.toBooleanArray()), offsets.toIntArray(), columnChunk.getBlock());
        return new ColumnChunk(arrayBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
    }

    private ColumnChunk readMap(GroupField field)
            throws IOException
    {
        List<Type> parameters = field.getType().getTypeParameters();
        checkArgument(parameters.size() == 2, "Maps must have two type parameters, found %s", parameters.size());
        Block[] blocks = new Block[parameters.size()];

        ColumnChunk columnChunk = readColumnChunk(field.getChildren().get(0).get());
        blocks[0] = columnChunk.getBlock();
        blocks[1] = readColumnChunk(field.getChildren().get(1).get()).getBlock();
        IntList offsets = new IntArrayList();
        BooleanList valueIsNull = new BooleanArrayList();
        calculateCollectionOffsets(field, offsets, valueIsNull, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        Block mapBlock = ((MapType) field.getType()).createBlockFromKeyValue(Optional.of(valueIsNull.toBooleanArray()), offsets.toIntArray(), blocks[0], blocks[1]);
        return new ColumnChunk(mapBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
    }

    private ColumnChunk readStruct(GroupField field)
            throws IOException
    {
        List<TypeSignatureParameter> fields = field.getType().getTypeSignature().getParameters();
        Block[] blocks = new Block[fields.size()];
        ColumnChunk columnChunk = null;
        List<Optional<Field>> parameters = field.getChildren();
        for (int i = 0; i < fields.size(); i++) {
            Optional<Field> parameter = parameters.get(i);
            if (parameter.isPresent()) {
                columnChunk = readColumnChunk(parameter.get());
                blocks[i] = columnChunk.getBlock();
            }
        }
        for (int i = 0; i < fields.size(); i++) {
            if (blocks[i] == null) {
                blocks[i] = RunLengthEncodedBlock.create(field.getType().getTypeParameters().get(i), null, columnChunk.getBlock().getPositionCount());
            }
        }
        BooleanList structIsNull = StructColumnReader.calculateStructOffsets(field, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        boolean[] structIsNullVector = structIsNull.toBooleanArray();
        Block rowBlock = RowBlock.fromFieldBlocks(structIsNullVector.length, Optional.of(structIsNullVector), blocks);
        return new ColumnChunk(rowBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
    }

    private FilteredOffsetIndex getFilteredOffsetIndex(int rowGroup, long rowGroupRowCount, ColumnPath columnPath)
    {
        if (filter.isPresent()) {
            RowRanges rowRanges = getRowRanges(filter.get(), rowGroup);
            if (rowRanges != null && rowRanges.rowCount() < rowGroupRowCount) {
                Optional<ColumnIndexStore> columnIndexStore = this.columnIndexStore.get(rowGroup);
                if (columnIndexStore.isPresent()) {
                    OffsetIndex offsetIndex = columnIndexStore.get().getOffsetIndex(columnPath);
                    if (offsetIndex != null) {
                        return FilteredOffsetIndex.filterOffsetIndex(offsetIndex, rowRanges, rowGroupRowCount);
                    }
                }
            }
        }
        return null;
    }

    private ColumnChunk readPrimitive(PrimitiveField field)
            throws IOException
    {
        ColumnDescriptor columnDescriptor = field.getDescriptor();
        int fieldId = field.getId();
        PrimitiveColumnReader columnReader = columnReaders.get(fieldId);
        if (!columnReader.hasPageReader()) {
            validateParquet(currentBlockMetadata.getRowCount() > 0, "Row group has 0 rows");
            ColumnChunkMetaData metadata = getColumnChunkMetaData(currentBlockMetadata, columnDescriptor);
            OffsetIndex offsetIndex = getFilteredOffsetIndex(currentRowGroup, currentBlockMetadata.getRowCount(), metadata.getPath());
            List<Slice> slices = allocateBlock(fieldId);
            columnReader.setPageReader(createPageReader(slices, metadata, columnDescriptor, offsetIndex), currentGroupRowRanges);
        }
        ColumnChunk columnChunk = columnReader.readPrimitive();

        // update max size per primitive column chunk
        long bytesPerCell = columnChunk.getBlock().getSizeInBytes() / batchSize;
        if (maxBytesPerCell.getOrDefault(fieldId, 0L) < bytesPerCell) {
            // update batch size
            maxCombinedBytesPerRow = maxCombinedBytesPerRow - maxBytesPerCell.getOrDefault(fieldId, 0L) + bytesPerCell;
            maxBatchSize = toIntExact(min(maxBatchSize, max(1, options.getMaxReadBlockSize().toBytes() / maxCombinedBytesPerRow)));
            maxBytesPerCell.put(fieldId, bytesPerCell);
        }
        return columnChunk;
    }

    public Metrics getMetrics()
    {
        ImmutableMap.Builder<String, Metric<?>> metrics = ImmutableMap.<String, Metric<?>>builder()
                .putAll(codecMetrics);
        if (columnIndexRowsFiltered >= 0) {
            metrics.put(COLUMN_INDEX_ROWS_FILTERED, new LongCount(columnIndexRowsFiltered));
        }

        return new Metrics(metrics.buildOrThrow());
    }

    private PageReader createPageReader(List<Slice> slices, ColumnChunkMetaData metadata, ColumnDescriptor columnDescriptor, OffsetIndex offsetIndex)
            throws IOException
    {
        ColumnChunkDescriptor descriptor = new ColumnChunkDescriptor(columnDescriptor, metadata);
        ParquetColumnChunk columnChunk = new ParquetColumnChunk(fileCreatedBy, descriptor, slices, offsetIndex);
        return columnChunk.readAllPages();
    }

    private List<Slice> allocateBlock(int fieldId)
            throws IOException
    {
        Collection<ChunkReader> readers = chunkReaders.get(new ChunkKey(fieldId, currentRowGroup));
        List<Slice> slices = Lists.newArrayListWithExpectedSize(readers.size());
        for (ChunkReader reader : readers) {
            Slice slice = reader.read();
            // todo this just an estimate and doesn't reflect actual retained memory
            currentRowGroupMemoryContext.newLocalMemoryContext(ParquetReader.class.getSimpleName())
                    .setBytes(slice.length());
            slices.add(slice);
        }
        return slices;
    }

    private ColumnChunkMetaData getColumnChunkMetaData(BlockMetaData blockMetaData, ColumnDescriptor columnDescriptor)
            throws IOException
    {
        for (ColumnChunkMetaData metadata : blockMetaData.getColumns()) {
            if (metadata.getPath().equals(ColumnPath.get(columnDescriptor.getPath()))) {
                return metadata;
            }
        }
        throw new ParquetCorruptionException("Metadata is missing for column: %s", columnDescriptor);
    }

    private void initializeColumnReaders()
    {
        for (PrimitiveField field : primitiveFields) {
            columnReaders.put(field.getId(), PrimitiveColumnReader.createReader(field, timeZone));
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
        if (field.getType() instanceof RowType) {
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

    private static <T> List<T> listWithNulls(int size)
    {
        return Stream.generate(() -> (T) null)
                .limit(size)
                .collect(Collectors.toCollection(ArrayList<T>::new));
    }

    private RowRanges getRowRanges(FilterPredicate filter, int blockIndex)
    {
        requireNonNull(filter, "filter is null");

        RowRanges rowRanges = blockRowRanges.get(blockIndex);
        if (rowRanges == null) {
            Optional<ColumnIndexStore> columnIndexStore = this.columnIndexStore.get(blockIndex);
            if (columnIndexStore.isPresent()) {
                rowRanges = ColumnIndexFilter.calculateRowRanges(
                        FilterCompat.get(filter),
                        columnIndexStore.get(),
                        paths.keySet(),
                        blocks.get(blockIndex).getRowCount());
                blockRowRanges.set(blockIndex, rowRanges);
            }
        }
        return rowRanges;
    }

    private void validateWritePageChecksum(Page page)
    {
        if (writeChecksumBuilder.isPresent()) {
            page = page.getLoadedPage();
            writeChecksumBuilder.get().addPage(page);
            rowGroupStatisticsValidation.orElseThrow().addPage(page);
        }
    }

    private void validateBlockMetadata(List<BlockMetaData> blockMetaData)
            throws ParquetCorruptionException
    {
        if (writeValidation.isPresent()) {
            writeValidation.get().validateBlocksMetadata(dataSource.getId(), blockMetaData);
        }
    }

    private void validateWrite(java.util.function.Predicate<ParquetWriteValidation> test, String messageFormat, Object... args)
            throws ParquetCorruptionException
    {
        if (writeValidation.isPresent() && !test.test(writeValidation.get())) {
            throw new ParquetCorruptionException(dataSource.getId(), "Write validation failed: " + messageFormat, args);
        }
    }
}
