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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;
import io.trino.plugin.hive.FileWriter;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.hasInvalidStatistics;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.jsonEncodeMax;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.jsonEncodeMin;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.UnaryOperator.identity;

public class DeltaLakeWriter
        implements FileWriter
{
    private final FileSystem fileSystem;
    private final FileWriter fileWriter;
    private final Path rootTableLocation;
    private final String relativeFilePath;
    private final List<String> partitionValues;
    private final DeltaLakeWriterStats stats;
    private final long creationTime;
    private final Set<Integer> timestampColumnIndices;
    private final List<DeltaLakeColumnHandle> columnHandles;

    private long rowCount;
    private long inputSizeInBytes;

    public DeltaLakeWriter(
            FileSystem fileSystem,
            FileWriter fileWriter,
            Path rootTableLocation,
            String relativeFilePath,
            List<String> partitionValues,
            DeltaLakeWriterStats stats,
            List<DeltaLakeColumnHandle> columnHandles)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.fileWriter = requireNonNull(fileWriter, "fileWriter is null");
        this.rootTableLocation = requireNonNull(rootTableLocation, "rootTableLocation is null");
        this.relativeFilePath = requireNonNull(relativeFilePath, "relativeFilePath is null");
        this.partitionValues = partitionValues;
        this.stats = stats;
        this.creationTime = Instant.now().toEpochMilli();
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableSet.Builder<Integer> timestampColumnIndices = ImmutableSet.builder();
        for (int i = 0; i < columnHandles.size(); i++) {
            if (columnHandles.get(i).getType() instanceof TimestampWithTimeZoneType) {
                timestampColumnIndices.add(i);
            }
        }
        this.timestampColumnIndices = timestampColumnIndices.build();
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
        if (timestampColumnIndices.size() > 0) {
            Block[] translatedBlocks = new Block[originalPage.getChannelCount()];
            for (int index = 0; index < translatedBlocks.length; index++) {
                Block originalBlock = originalPage.getBlock(index);
                if (timestampColumnIndices.contains(index)) {
                    translatedBlocks[index] = new LazyBlock(
                            originalBlock.getPositionCount(),
                            new TimestampTranslationBlockLoader(originalBlock));
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
    public void commit()
    {
        fileWriter.commit();
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

    public DataFileInfo getDataFileInfo()
            throws IOException
    {
        List<String> dataColumnNames = columnHandles.stream().map(DeltaLakeColumnHandle::getName).collect(toImmutableList());
        List<Type> dataColumnTypes = columnHandles.stream().map(DeltaLakeColumnHandle::getType).collect(toImmutableList());
        return new DataFileInfo(
                relativeFilePath,
                getWrittenBytes(),
                creationTime,
                partitionValues,
                readStatistics(fileSystem, rootTableLocation, dataColumnNames, dataColumnTypes, relativeFilePath, rowCount));
    }

    private static DeltaLakeJsonFileStatistics readStatistics(
            FileSystem fs,
            Path tableLocation,
            List<String> dataColumnNames,
            List<Type> dataColumnTypes,
            String relativeFilePath,
            Long rowCount)
            throws IOException
    {
        ImmutableMap.Builder<String, Type> typeForColumn = ImmutableMap.builder();
        for (int i = 0; i < dataColumnNames.size(); i++) {
            typeForColumn.put(dataColumnNames.get(i), dataColumnTypes.get(i));
        }

        ImmutableMultimap.Builder<String, ColumnChunkMetaData> metadataForColumn = ImmutableMultimap.builder();
        try (ParquetFileReader parquetReader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(tableLocation, relativeFilePath), fs.getConf()))) {
            for (BlockMetaData blockMetaData : parquetReader.getRowGroups()) {
                for (ColumnChunkMetaData columnChunkMetaData : blockMetaData.getColumns()) {
                    if (columnChunkMetaData.getPath().size() != 1) {
                        continue; // Only base column stats are supported
                    }
                    String columnName = getOnlyElement(columnChunkMetaData.getPath());
                    metadataForColumn.put(columnName, columnChunkMetaData);
                }
            }
        }

        return mergeStats(metadataForColumn.build(), typeForColumn.buildOrThrow(), rowCount);
    }

    @VisibleForTesting
    static DeltaLakeJsonFileStatistics mergeStats(Multimap<String, ColumnChunkMetaData> metadataForColumn, Map<String, Type> typeForColumn, long rowCount)
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

    private static final class TimestampTranslationBlockLoader
            implements LazyBlockLoader
    {
        private Block originalBlock;

        public TimestampTranslationBlockLoader(Block originalBlock)
        {
            this.originalBlock = requireNonNull(originalBlock, "originalBlock is null");
        }

        @Override
        public Block load()
        {
            checkState(originalBlock != null, "Already loaded");

            int positionCount = originalBlock.getPositionCount();
            long[] values = new long[positionCount];
            boolean mayHaveNulls = originalBlock.mayHaveNull();
            boolean[] valueIsNull = mayHaveNulls ? new boolean[positionCount] : null;

            for (int position = 0; position < positionCount; position++) {
                if (mayHaveNulls && originalBlock.isNull(position)) {
                    valueIsNull[position] = true;
                    continue;
                }
                values[position] = MILLISECONDS.toMicros(unpackMillisUtc(TIMESTAMP_TZ_MILLIS.getLong(originalBlock, position)));
            }
            Block mapped;
            mapped = new LongArrayBlock(positionCount, Optional.ofNullable(valueIsNull), values);
            // clear reference to Block to free resources, since load was successful
            originalBlock = null;

            return mapped;
        }
    }
}
