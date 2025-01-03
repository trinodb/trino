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
package io.trino.plugin.deltalake;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.trino.filesystem.Location;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.plugin.deltalake.DataFileInfo.DataFileType;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.FileMetaData;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.hasInvalidStatistics;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.jsonEncodeMax;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.jsonEncodeMin;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.UnaryOperator.identity;

public final class DeltaLakeWriter
        implements FileWriter
{
    private final ParquetFileWriter fileWriter;
    private final Location rootTableLocation;
    private final String relativeFilePath;
    private final List<String> partitionValues;
    private final DeltaLakeWriterStats stats;
    private final long creationTime;
    private final Map<Integer, Function<Block, Block>> coercers;
    private final List<DeltaLakeColumnHandle> columnHandles;
    private final DataFileType dataFileType;

    private long rowCount;
    private long inputSizeInBytes;

    public DeltaLakeWriter(
            ParquetFileWriter fileWriter,
            Location rootTableLocation,
            String relativeFilePath,
            List<String> partitionValues,
            DeltaLakeWriterStats stats,
            List<DeltaLakeColumnHandle> columnHandles,
            DataFileType dataFileType)
    {
        this.fileWriter = requireNonNull(fileWriter, "fileWriter is null");
        this.rootTableLocation = requireNonNull(rootTableLocation, "rootTableLocation is null");
        this.relativeFilePath = requireNonNull(relativeFilePath, "relativeFilePath is null");
        this.partitionValues = partitionValues;
        this.stats = stats;
        this.creationTime = Instant.now().toEpochMilli();
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableMap.Builder<Integer, Function<Block, Block>> coercers = ImmutableMap.builder();
        for (int i = 0; i < columnHandles.size(); i++) {
            Optional<Function<Block, Block>> coercer = createCoercer(columnHandles.get(i).baseType());
            if (coercer.isPresent()) {
                coercers.put(i, coercer.get());
            }
        }
        this.coercers = coercers.buildOrThrow();
        this.dataFileType = requireNonNull(dataFileType, "dataFileType is null");
    }

    @Override
    public long getWrittenBytes()
    {
        return fileWriter.getWrittenBytes();
    }

    @Override
    public long getMemoryUsage()
    {
        return fileWriter.getMemoryUsage();
    }

    @Override
    public void appendRows(Page originalPage)
    {
        Page page = originalPage;
        if (!coercers.isEmpty()) {
            Block[] translatedBlocks = new Block[originalPage.getChannelCount()];
            for (int index = 0; index < translatedBlocks.length; index++) {
                Block originalBlock = originalPage.getBlock(index);
                Function<Block, Block> coercer = coercers.get(index);
                if (coercer != null) {
                    translatedBlocks[index] = new LazyBlock(
                            originalBlock.getPositionCount(),
                            new CoercionLazyBlockLoader(originalBlock, coercer));
                }
                else {
                    translatedBlocks[index] = originalBlock;
                }
            }
            page = new Page(originalPage.getPositionCount(), translatedBlocks);
        }

        stats.addInputPageSizesInBytes(page.getRetainedSizeInBytes());
        fileWriter.appendRows(page);
        rowCount += page.getPositionCount();
        inputSizeInBytes += page.getSizeInBytes();
    }

    @Override
    public Closeable commit()
    {
        return fileWriter.commit();
    }

    @Override
    public void rollback()
    {
        fileWriter.rollback();
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public DataFileInfo getDataFileInfo()
            throws IOException
    {
        Location path = rootTableLocation.appendPath(relativeFilePath);
        FileMetaData fileMetaData = fileWriter.getFileMetadata();
        ParquetMetadata parquetMetadata = new ParquetMetadata(fileMetaData, new ParquetDataSourceId(path.toString()));

        return new DataFileInfo(
                relativeFilePath,
                getWrittenBytes(),
                creationTime,
                dataFileType,
                partitionValues,
                readStatistics(parquetMetadata, columnHandles, rowCount),
                Optional.empty());
    }

    public static DeltaLakeJsonFileStatistics readStatistics(ParquetMetadata parquetMetadata, List<DeltaLakeColumnHandle> columnHandles, long rowCount)
            throws IOException
    {
        Map</* lowercase */ String, Type> typeForColumn = columnHandles.stream()
                // Lowercase because the subsequent logic expects lowercase
                .collect(toImmutableMap(column -> column.basePhysicalColumnName().toLowerCase(ENGLISH), DeltaLakeColumnHandle::basePhysicalType));

        ImmutableMultimap.Builder<String, ColumnChunkMetadata> metadataForColumn = ImmutableMultimap.builder();
        for (BlockMetadata blockMetaData : parquetMetadata.getBlocks()) {
            for (ColumnChunkMetadata columnChunkMetaData : blockMetaData.columns()) {
                if (columnChunkMetaData.getPath().size() != 1) {
                    continue; // Only base column stats are supported
                }
                String columnName = getOnlyElement(columnChunkMetaData.getPath());
                metadataForColumn.put(columnName, columnChunkMetaData);
            }
        }

        return mergeStats(metadataForColumn.build(), typeForColumn, rowCount);
    }

    @VisibleForTesting
    static DeltaLakeJsonFileStatistics mergeStats(Multimap<String, ColumnChunkMetadata> metadataForColumn, Map</* lowercase */ String, Type> typeForColumn, long rowCount)
    {
        Map<String, Optional<Statistics<?>>> statsForColumn = metadataForColumn.keySet().stream()
                .collect(toImmutableMap(identity(), key -> mergeMetadataList(metadataForColumn.get(key))));

        Map<String, Object> nullCount = statsForColumn.entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().get().getNumNulls()));

        return new DeltaLakeJsonFileStatistics(
                Optional.of(rowCount),
                Optional.of(jsonEncodeMin(statsForColumn, typeForColumn)),
                Optional.of(jsonEncodeMax(statsForColumn, typeForColumn)),
                Optional.of(nullCount));
    }

    private static Optional<Statistics<?>> mergeMetadataList(Collection<ColumnChunkMetadata> metadataList)
    {
        if (hasInvalidStatistics(metadataList)) {
            return Optional.empty();
        }

        return metadataList.stream()
                .<Statistics<?>>map(ColumnChunkMetadata::getStatistics)
                .reduce((statsA, statsB) -> {
                    statsA.mergeStatistics(statsB);
                    return statsA;
                });
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fileWriter", fileWriter)
                .add("relativeFilePath", relativeFilePath)
                .add("partitionValues", partitionValues)
                .add("creationTime", creationTime)
                .add("rowCount", rowCount)
                .add("inputSizeInBytes", inputSizeInBytes)
                .toString();
    }

    private static Optional<Function<Block, Block>> createCoercer(Type type)
    {
        if (type instanceof ArrayType arrayType) {
            return createCoercer(arrayType.getElementType()).map(ArrayCoercer::new);
        }
        if (type instanceof MapType mapType) {
            return Optional.of(new MapCoercer(mapType));
        }
        if (type instanceof RowType rowType) {
            return Optional.of(new RowCoercer(rowType));
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return Optional.of(new TimestampCoercer());
        }
        return Optional.empty();
    }

    private static class ArrayCoercer
            implements Function<Block, Block>
    {
        private final Function<Block, Block> elementCoercer;

        public ArrayCoercer(Function<Block, Block> elementCoercer)
        {
            this.elementCoercer = requireNonNull(elementCoercer, "elementCoercer is null");
        }

        @Override
        public Block apply(Block block)
        {
            ColumnarArray arrayBlock = toColumnarArray(block);
            Block elementsBlock = elementCoercer.apply(arrayBlock.getElementsBlock());
            boolean[] valueIsNull = new boolean[arrayBlock.getPositionCount()];
            int[] offsets = new int[arrayBlock.getPositionCount() + 1];
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                valueIsNull[i] = arrayBlock.isNull(i);
                offsets[i + 1] = offsets[i] + arrayBlock.getLength(i);
            }
            return ArrayBlock.fromElementBlock(arrayBlock.getPositionCount(), Optional.of(valueIsNull), offsets, elementsBlock);
        }
    }

    private static class MapCoercer
            implements Function<Block, Block>
    {
        private final MapType mapType;
        private final Optional<Function<Block, Block>> keyCoercer;
        private final Optional<Function<Block, Block>> valueCoercer;

        public MapCoercer(MapType mapType)
        {
            this.mapType = requireNonNull(mapType, "mapType is null");
            keyCoercer = createCoercer(mapType.getKeyType());
            valueCoercer = createCoercer(mapType.getValueType());
        }

        @Override
        public Block apply(Block block)
        {
            ColumnarMap mapBlock = toColumnarMap(block);
            Block keysBlock = keyCoercer.isEmpty() ? mapBlock.getKeysBlock() : keyCoercer.get().apply(mapBlock.getKeysBlock());
            Block valuesBlock = valueCoercer.isEmpty() ? mapBlock.getValuesBlock() : valueCoercer.get().apply(mapBlock.getValuesBlock());
            boolean[] valueIsNull = new boolean[mapBlock.getPositionCount()];
            int[] offsets = new int[mapBlock.getPositionCount() + 1];
            for (int i = 0; i < mapBlock.getPositionCount(); i++) {
                valueIsNull[i] = mapBlock.isNull(i);
                offsets[i + 1] = offsets[i] + mapBlock.getEntryCount(i);
            }
            return mapType.createBlockFromKeyValue(Optional.of(valueIsNull), offsets, keysBlock, valuesBlock);
        }
    }

    private static class RowCoercer
            implements Function<Block, Block>
    {
        private final List<Optional<Function<Block, Block>>> fieldCoercers;

        public RowCoercer(RowType rowType)
        {
            fieldCoercers = rowType.getTypeParameters().stream()
                    .map(DeltaLakeWriter::createCoercer)
                    .collect(toImmutableList());
        }

        @Override
        public Block apply(Block block)
        {
            block = block.getLoadedBlock();

            if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
                RowBlock rowBlock = (RowBlock) runLengthEncodedBlock.getValue();
                RowBlock newRowBlock = RowBlock.fromNotNullSuppressedFieldBlocks(
                        1,
                        rowBlock.isNull(0) ? Optional.of(new boolean[] {true}) : Optional.empty(),
                        coerceFields(rowBlock.getFieldBlocks()));
                return RunLengthEncodedBlock.create(newRowBlock, runLengthEncodedBlock.getPositionCount());
            }
            if (block instanceof DictionaryBlock dictionaryBlock) {
                RowBlock rowBlock = (RowBlock) dictionaryBlock.getDictionary();
                List<Block> fieldBlocks = rowBlock.getFieldBlocks().stream()
                        .map(dictionaryBlock::createProjection)
                        .toList();
                return RowBlock.fromNotNullSuppressedFieldBlocks(
                        dictionaryBlock.getPositionCount(),
                        getNulls(dictionaryBlock),
                        coerceFields(fieldBlocks));
            }
            RowBlock rowBlock = (RowBlock) block;
            return RowBlock.fromNotNullSuppressedFieldBlocks(
                    rowBlock.getPositionCount(),
                    getNulls(rowBlock),
                    coerceFields(rowBlock.getFieldBlocks()));
        }

        private static Optional<boolean[]> getNulls(Block rowBlock)
        {
            if (!rowBlock.mayHaveNull()) {
                return Optional.empty();
            }

            boolean[] valueIsNull = new boolean[rowBlock.getPositionCount()];
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                valueIsNull[i] = rowBlock.isNull(i);
            }
            return Optional.of(valueIsNull);
        }

        private Block[] coerceFields(List<Block> fields)
        {
            checkArgument(fields.size() == fieldCoercers.size());
            Block[] newFields = new Block[fieldCoercers.size()];
            for (int i = 0; i < fieldCoercers.size(); i++) {
                Optional<Function<Block, Block>> coercer = fieldCoercers.get(i);
                Block fieldBlock = fields.get(i);
                if (coercer.isPresent()) {
                    newFields[i] = coercer.get().apply(fieldBlock);
                }
                else {
                    newFields[i] = fieldBlock;
                }
            }
            return newFields;
        }
    }

    private static class TimestampCoercer
            implements Function<Block, Block>
    {
        @Override
        public Block apply(Block block)
        {
            int positionCount = block.getPositionCount();
            long[] values = new long[positionCount];
            boolean mayHaveNulls = block.mayHaveNull();
            boolean[] valueIsNull = mayHaveNulls ? new boolean[positionCount] : null;

            for (int position = 0; position < positionCount; position++) {
                if (mayHaveNulls && block.isNull(position)) {
                    valueIsNull[position] = true;
                    continue;
                }
                values[position] = MILLISECONDS.toMicros(unpackMillisUtc(TIMESTAMP_TZ_MILLIS.getLong(block, position)));
            }
            return new LongArrayBlock(positionCount, Optional.ofNullable(valueIsNull), values);
        }
    }

    private static final class CoercionLazyBlockLoader
            implements LazyBlockLoader
    {
        private final Function<Block, Block> coercer;
        private Block block;

        public CoercionLazyBlockLoader(Block block, Function<Block, Block> coercer)
        {
            this.block = requireNonNull(block, "block is null");
            this.coercer = requireNonNull(coercer, "coercer is null");
        }

        @Override
        public Block load()
        {
            checkState(block != null, "Already loaded");

            Block loaded = coercer.apply(block.getLoadedBlock());
            // clear reference to loader to free resources, since load was successful
            block = null;

            return loaded;
        }
    }
}
