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
import io.trino.plugin.hive.RollbackAction;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.FileMetaData;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.hasInvalidStatistics;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.jsonEncodeMax;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.jsonEncodeMin;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.tightBounds;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
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
        // Avoid ImmutableList.copyOf because partitionValues may have nulls
        this.partitionValues = requireNonNull(partitionValues, "partitionValues is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.creationTime = Instant.now().toEpochMilli();
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
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
    public void appendRows(Page page)
    {
        stats.addInputPageSizesInBytes(page.getRetainedSizeInBytes());
        fileWriter.appendRows(page);
        rowCount += page.getPositionCount();
        inputSizeInBytes += page.getSizeInBytes();
    }

    @Override
    public RollbackAction commit()
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
        return fileWriter.getValidationCpuNanos();
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
        ParquetMetadata parquetMetadata = new ParquetMetadata(fileMetaData, new ParquetDataSourceId(path.toString()), Optional.empty());

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

        Map<String, Object> maxValues = jsonEncodeMax(statsForColumn, typeForColumn);
        return new DeltaLakeJsonFileStatistics(
                Optional.of(rowCount),
                Optional.of(jsonEncodeMin(statsForColumn, typeForColumn)),
                Optional.of(maxValues),
                Optional.of(nullCount),
                tightBounds(maxValues.keySet().stream().map(typeForColumn::get).toList()));
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
}
