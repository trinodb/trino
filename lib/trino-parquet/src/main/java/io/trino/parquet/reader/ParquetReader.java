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
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.RichColumnDescriptor;
import io.trino.parquet.predicate.Predicate;
import io.trino.parquet.reader.FilteredOffsetIndex.OffsetRange;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
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
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetValidationUtils.validateParquet;
import static io.trino.parquet.reader.ListColumnReader.calculateCollectionOffsets;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ParquetReader
        implements Closeable
{
    private static final int MAX_VECTOR_LENGTH = 1024;
    private static final int INITIAL_BATCH_SIZE = 1;
    private static final int BATCH_SIZE_GROWTH_FACTOR = 2;

    private final Optional<String> fileCreatedBy;
    private final List<BlockMetaData> blocks;
    private final List<Long> firstRowsOfBlocks;
    private final List<PrimitiveColumnIO> columns;
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
    private final PrimitiveColumnReader[] columnReaders;
    private final long[] maxBytesPerCell;
    private long maxCombinedBytesPerRow;
    private final ParquetReaderOptions options;
    private int maxBatchSize = MAX_VECTOR_LENGTH;

    private AggregatedMemoryContext currentRowGroupMemoryContext;
    private final Multimap<ChunkKey, ChunkReader> chunkReaders;
    private final List<Optional<ColumnIndexStore>> columnIndexStore;
    private final List<RowRanges> blockRowRanges;
    private final Map<ColumnPath, ColumnDescriptor> paths = new HashMap<>();

    public ParquetReader(
            Optional<String> fileCreatedBy,
            MessageColumnIO messageColumnIO,
            List<BlockMetaData> blocks,
            List<Long> firstRowsOfBlocks,
            ParquetDataSource dataSource,
            DateTimeZone timeZone,
            AggregatedMemoryContext memoryContext,
            ParquetReaderOptions options)
            throws IOException
    {
        this(fileCreatedBy, messageColumnIO, blocks, firstRowsOfBlocks, dataSource, timeZone, memoryContext, options, null, null);
    }

    public ParquetReader(
            Optional<String> fileCreatedBy,
            MessageColumnIO messageColumnIO,
            List<BlockMetaData> blocks,
            List<Long> firstRowsOfBlocks,
            ParquetDataSource dataSource,
            DateTimeZone timeZone,
            AggregatedMemoryContext memoryContext,
            ParquetReaderOptions options,
            Predicate parquetPredicate,
            List<Optional<ColumnIndexStore>> columnIndexStore)
            throws IOException
    {
        this.fileCreatedBy = requireNonNull(fileCreatedBy, "fileCreatedBy is null");
        this.columns = requireNonNull(messageColumnIO, "messageColumnIO is null").getLeaves();
        this.blocks = requireNonNull(blocks, "blocks is null");
        this.firstRowsOfBlocks = requireNonNull(firstRowsOfBlocks, "firstRowsOfBlocks is null");
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.currentRowGroupMemoryContext = memoryContext.newAggregatedMemoryContext();
        this.options = requireNonNull(options, "options is null");
        this.columnReaders = new PrimitiveColumnReader[columns.size()];
        this.maxBytesPerCell = new long[columns.size()];

        checkArgument(blocks.size() == firstRowsOfBlocks.size(), "elements of firstRowsOfBlocks must correspond to blocks");

        this.columnIndexStore = columnIndexStore;
        this.blockRowRanges = listWithNulls(this.blocks.size());
        for (PrimitiveColumnIO column : columns) {
            ColumnDescriptor columnDescriptor = column.getColumnDescriptor();
            this.paths.put(ColumnPath.get(columnDescriptor.getPath()), columnDescriptor);
        }
        if (parquetPredicate != null && options.isUseColumnIndex()) {
            this.filter = parquetPredicate.toParquetFilter(timeZone);
        }
        else {
            this.filter = Optional.empty();
        }
        ListMultimap<ChunkKey, DiskRange> ranges = ArrayListMultimap.create();
        for (int rowGroup = 0; rowGroup < blocks.size(); rowGroup++) {
            BlockMetaData metadata = blocks.get(rowGroup);
            for (PrimitiveColumnIO column : columns) {
                int columnId = column.getId();
                ColumnChunkMetaData chunkMetadata = getColumnChunkMetaData(metadata, column.getColumnDescriptor());
                ColumnPath columnPath = chunkMetadata.getPath();
                long rowGroupRowCount = metadata.getRowCount();
                long startingPosition = chunkMetadata.getStartingPos();
                long totalLength = chunkMetadata.getTotalSize();
                FilteredOffsetIndex filteredOffsetIndex = getFilteredOffsetIndex(rowGroup, rowGroupRowCount, columnPath);
                if (filteredOffsetIndex == null) {
                    DiskRange range = new DiskRange(startingPosition, toIntExact(totalLength));
                    ranges.put(new ChunkKey(columnId, rowGroup), range);
                }
                else {
                    List<OffsetRange> offsetRanges = filteredOffsetIndex.calculateOffsetRanges(startingPosition);
                    for (OffsetRange offsetRange : offsetRanges) {
                        DiskRange range = new DiskRange(offsetRange.getOffset(), toIntExact(offsetRange.getLength()));
                        ranges.put(new ChunkKey(columnId, rowGroup), range);
                    }
                }
            }
        }

        this.chunkReaders = dataSource.planRead(ranges);
    }

    @Override
    public void close()
            throws IOException
    {
        freeCurrentRowGroupBuffers();
        currentRowGroupMemoryContext.close();
        dataSource.close();
    }

    /**
     * Get the global row index of the first row in the last batch.
     */
    public long lastBatchStartRow()
    {
        return firstRowIndexInGroup + nextRowInGroup - batchSize;
    }

    public int nextBatch()
    {
        if (nextRowInGroup >= currentGroupRowCount && !advanceToNextRowGroup()) {
            return -1;
        }

        batchSize = min(nextBatchSize, maxBatchSize);
        nextBatchSize = min(batchSize * BATCH_SIZE_GROWTH_FACTOR, MAX_VECTOR_LENGTH);
        batchSize = toIntExact(min(batchSize, currentGroupRowCount - nextRowInGroup));

        nextRowInGroup += batchSize;
        Arrays.stream(columnReaders)
                .forEach(reader -> reader.prepareNextRead(batchSize));
        return batchSize;
    }

    private boolean advanceToNextRowGroup()
    {
        currentRowGroupMemoryContext.close();
        currentRowGroupMemoryContext = memoryContext.newAggregatedMemoryContext();
        freeCurrentRowGroupBuffers();
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

        for (int column = 0; column < columns.size(); column++) {
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
        PrimitiveColumnReader columnReader = columnReaders[fieldId];
        if (columnReader.getPageReader() == null) {
            validateParquet(currentBlockMetadata.getRowCount() > 0, "Row group has 0 rows");
            ColumnChunkMetaData metadata = getColumnChunkMetaData(currentBlockMetadata, columnDescriptor);
            OffsetIndex offsetIndex = getFilteredOffsetIndex(currentRowGroup, currentBlockMetadata.getRowCount(), metadata.getPath());
            List<Slice> slices = allocateBlock(fieldId);
            columnReader.setPageReader(createPageReader(slices, metadata, columnDescriptor, offsetIndex), currentGroupRowRanges);
        }
        ColumnChunk columnChunk = columnReader.readPrimitive(field);

        // update max size per primitive column chunk
        long bytesPerCell = columnChunk.getBlock().getSizeInBytes() / batchSize;
        if (maxBytesPerCell[fieldId] < bytesPerCell) {
            // update batch size
            maxCombinedBytesPerRow = maxCombinedBytesPerRow - maxBytesPerCell[fieldId] + bytesPerCell;
            maxBatchSize = toIntExact(min(maxBatchSize, max(1, options.getMaxReadBlockSize().toBytes() / maxCombinedBytesPerRow)));
            maxBytesPerCell[fieldId] = bytesPerCell;
        }
        return columnChunk;
    }

    private PageReader createPageReader(List<Slice> slices, ColumnChunkMetaData metadata, ColumnDescriptor columnDescriptor, OffsetIndex offsetIndex)
            throws IOException
    {
        ColumnChunkDescriptor descriptor = new ColumnChunkDescriptor(columnDescriptor, metadata);
        ParquetColumnChunk columnChunk = new ParquetColumnChunk(fileCreatedBy, descriptor, slices, offsetIndex);
        return columnChunk.readAllPages();
    }

    private List<Slice> allocateBlock(int fieldId)
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
        for (PrimitiveColumnIO columnIO : columns) {
            RichColumnDescriptor column = new RichColumnDescriptor(columnIO.getColumnDescriptor(), columnIO.getType().asPrimitiveType());
            columnReaders[columnIO.getId()] = PrimitiveColumnReader.createReader(column, timeZone);
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
}
