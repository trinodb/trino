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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.deltalake.DataFileInfo.DataFileType;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.FileWriter;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.hasInvalidStatistics;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.jsonEncodeMax;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.jsonEncodeMin;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.UnaryOperator.identity;

public class DeltaLakeWriter
        implements FileWriter
{
    private final TrinoFileSystem fileSystem;
    private final FileWriter fileWriter;
    private final Location rootTableLocation;
    private final String relativeFilePath;
    private final List<String> partitionValues;
    private final DeltaLakeWriterStats stats;
    private final long creationTime;
    private final Map<Integer, Function<Block, Block>> coercers;
    private final List<DeltaLakeColumnHandle> columnHandles;

    private long rowCount;
    private long inputSizeInBytes;
    private DataFileType dataFileType;

    public DeltaLakeWriter(
            TrinoFileSystem fileSystem,
            FileWriter fileWriter,
            Location rootTableLocation,
            String relativeFilePath,
            List<String> partitionValues,
            DeltaLakeWriterStats stats,
            List<DeltaLakeColumnHandle> columnHandles,
            DataFileType dataFileType)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.fileWriter = requireNonNull(fileWriter, "fileWriter is null");
        this.rootTableLocation = requireNonNull(rootTableLocation, "rootTableLocation is null");
        this.relativeFilePath = requireNonNull(relativeFilePath, "relativeFilePath is null");
        this.partitionValues = partitionValues;
        this.stats = stats;
        this.creationTime = Instant.now().toEpochMilli();
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableMap.Builder<Integer, Function<Block, Block>> coercers = ImmutableMap.builder();
        for (int i = 0; i < columnHandles.size(); i++) {
            Optional<Function<Block, Block>> coercer = createCoercer(columnHandles.get(i).getBaseType());
            if (coercer.isPresent()) {
                coercers.put(i, coercer.get());
            }
        }
        this.coercers = coercers.buildOrThrow();
        this.dataFileType = dataFileType;
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
        if (coercers.size() > 0) {
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
        TrinoInputFile inputFile = fileSystem.newInputFile(rootTableLocation.appendPath(relativeFilePath));
        Map</* lowercase */ String, Type> dataColumnTypes = columnHandles.stream()
                // Lowercase because the subsequent logic expects lowercase
                .collect(toImmutableMap(column -> column.getBasePhysicalColumnName().toLowerCase(ENGLISH), DeltaLakeColumnHandle::getBasePhysicalType));
        return new DataFileInfo(
                relativeFilePath,
                getWrittenBytes(),
                creationTime,
                dataFileType,
                partitionValues,
                readStatistics(inputFile, dataColumnTypes, rowCount));
    }

    private static DeltaLakeJsonFileStatistics readStatistics(TrinoInputFile inputFile, Map</* lowercase */ String, Type> typeForColumn, long rowCount)
            throws IOException
    {
        try (TrinoParquetDataSource trinoParquetDataSource = new TrinoParquetDataSource(
                inputFile,
                new ParquetReaderOptions(),
                new FileFormatDataSourceStats())) {
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(trinoParquetDataSource, Optional.empty());

            ImmutableMultimap.Builder<String, ColumnChunkMetaData> metadataForColumn = ImmutableMultimap.builder();
            for (BlockMetaData blockMetaData : parquetMetadata.getBlocks()) {
                for (ColumnChunkMetaData columnChunkMetaData : blockMetaData.getColumns()) {
                    if (columnChunkMetaData.getPath().size() != 1) {
                        continue; // Only base column stats are supported
                    }
                    String columnName = getOnlyElement(columnChunkMetaData.getPath());
                    metadataForColumn.put(columnName, columnChunkMetaData);
                }
            }

            return mergeStats(metadataForColumn.build(), typeForColumn, rowCount);
        }
    }

    @VisibleForTesting
    static DeltaLakeJsonFileStatistics mergeStats(Multimap<String, ColumnChunkMetaData> metadataForColumn, Map</* lowercase */ String, Type> typeForColumn, long rowCount)
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

    private static Optional<Statistics<?>> mergeMetadataList(Collection<ColumnChunkMetaData> metadataList)
    {
        if (hasInvalidStatistics(metadataList)) {
            return Optional.empty();
        }

        return metadataList.stream()
                .<Statistics<?>>map(ColumnChunkMetaData::getStatistics)
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
            ColumnarRow rowBlock = toColumnarRow(block);
            Block[] fields = new Block[fieldCoercers.size()];
            for (int i = 0; i < fieldCoercers.size(); i++) {
                Optional<Function<Block, Block>> coercer = fieldCoercers.get(i);
                if (coercer.isPresent()) {
                    fields[i] = coercer.get().apply(rowBlock.getField(i));
                }
                else {
                    fields[i] = rowBlock.getField(i);
                }
            }
            boolean[] valueIsNull = null;
            if (rowBlock.mayHaveNull()) {
                valueIsNull = new boolean[rowBlock.getPositionCount()];
                for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                    valueIsNull[i] = rowBlock.isNull(i);
                }
            }
            return RowBlock.fromFieldBlocks(rowBlock.getPositionCount(), Optional.ofNullable(valueIsNull), fields);
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
