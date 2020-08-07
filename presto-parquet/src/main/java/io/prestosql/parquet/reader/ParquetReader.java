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
package io.prestosql.parquet.reader;

import io.airlift.slice.Slice;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.parquet.ChunkKey;
import io.prestosql.parquet.ChunkReader;
import io.prestosql.parquet.DiskRange;
import io.prestosql.parquet.Field;
import io.prestosql.parquet.GroupField;
import io.prestosql.parquet.ParquetCorruptionException;
import io.prestosql.parquet.ParquetDataSource;
import io.prestosql.parquet.ParquetReaderOptions;
import io.prestosql.parquet.PrimitiveField;
import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.spi.block.ArrayBlock;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.RowBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignatureParameter;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.parquet.ParquetValidationUtils.validateParquet;
import static io.prestosql.parquet.reader.ListColumnReader.calculateCollectionOffsets;
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
    private final List<PrimitiveColumnIO> columns;
    private final ParquetDataSource dataSource;
    private final DateTimeZone timeZone;
    private final AggregatedMemoryContext systemMemoryContext;

    private int currentRowGroup = -1;
    private BlockMetaData currentBlockMetadata;
    private long currentGroupRowCount;
    private long nextRowInGroup;
    private int batchSize;
    private int nextBatchSize = INITIAL_BATCH_SIZE;
    private final PrimitiveColumnReader[] columnReaders;
    private final long[] maxBytesPerCell;
    private long maxCombinedBytesPerRow;
    private final ParquetReaderOptions options;
    private int maxBatchSize = MAX_VECTOR_LENGTH;

    private AggregatedMemoryContext currentRowGroupMemoryContext;
    private final Map<ChunkKey, ChunkReader> chunkReaders;

    public ParquetReader(
            Optional<String> fileCreatedBy,
            MessageColumnIO messageColumnIO,
            List<BlockMetaData> blocks,
            ParquetDataSource dataSource,
            DateTimeZone timeZone,
            AggregatedMemoryContext systemMemoryContext,
            ParquetReaderOptions options)
            throws IOException
    {
        this.fileCreatedBy = requireNonNull(fileCreatedBy, "fileCreatedBy is null");
        this.columns = requireNonNull(messageColumnIO, "messageColumnIO is null").getLeaves();
        this.blocks = requireNonNull(blocks, "blocks is null");
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.currentRowGroupMemoryContext = systemMemoryContext.newAggregatedMemoryContext();
        this.options = requireNonNull(options, "options is null");
        this.columnReaders = new PrimitiveColumnReader[columns.size()];
        this.maxBytesPerCell = new long[columns.size()];

        Map<ChunkKey, DiskRange> ranges = new HashMap<>();
        for (int rowGroup = 0; rowGroup < blocks.size(); rowGroup++) {
            BlockMetaData metadata = blocks.get(rowGroup);
            for (PrimitiveColumnIO column : columns) {
                int columnId = column.getId();
                ColumnChunkMetaData chunkMetadata = getColumnChunkMetaData(metadata, column.getColumnDescriptor());
                DiskRange range = new DiskRange(chunkMetadata.getStartingPos(), toIntExact(chunkMetadata.getTotalSize()));
                ranges.put(new ChunkKey(columnId, rowGroup), range);
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
        currentRowGroupMemoryContext = systemMemoryContext.newAggregatedMemoryContext();

        freeCurrentRowGroupBuffers();
        currentRowGroup++;
        if (currentRowGroup == blocks.size()) {
            return false;
        }
        currentBlockMetadata = blocks.get(currentRowGroup);

        nextRowInGroup = 0L;
        currentGroupRowCount = currentBlockMetadata.getRowCount();
        initializeColumnReaders();
        return true;
    }

    private void freeCurrentRowGroupBuffers()
    {
        if (currentRowGroup < 0) {
            return;
        }

        for (int column = 0; column < columns.size(); column++) {
            ChunkReader reader = chunkReaders.get(new ChunkKey(column, currentRowGroup));
            if (reader != null) {
                reader.free();
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
                blocks[i] = RunLengthEncodedBlock.create(field.getType(), null, columnChunk.getBlock().getPositionCount());
            }
        }
        BooleanList structIsNull = StructColumnReader.calculateStructOffsets(field, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        boolean[] structIsNullVector = structIsNull.toBooleanArray();
        Block rowBlock = RowBlock.fromFieldBlocks(structIsNullVector.length, Optional.of(structIsNullVector), blocks);
        return new ColumnChunk(rowBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
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

            Slice data = chunkReaders.get(new ChunkKey(fieldId, currentRowGroup)).read();
            // todo this just an estimate and doesn't reflect actual retained memory
            currentRowGroupMemoryContext.newLocalMemoryContext(ParquetReader.class.getSimpleName())
                    .setBytes(data.length());

            ColumnChunkDescriptor descriptor = new ColumnChunkDescriptor(columnDescriptor, metadata);
            ParquetColumnChunk columnChunk = new ParquetColumnChunk(fileCreatedBy, descriptor, data);
            columnReader.setPageReader(columnChunk.readAllPages());
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

    public AggregatedMemoryContext getSystemMemoryContext()
    {
        return systemMemoryContext;
    }
}
