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
package io.trino.plugin.hudi.stats;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.collection.Pair;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.hudi.HudiUtil.getFileSystemView;

/**
 * Reads table statistics of a Hudi table from the metadata table files and column stats partitions.
 */
public class TableStatisticsReader
{
    private static final Logger log = Logger.get(TableStatisticsReader.class);
    private final HoodieTableMetaClient metaClient;
    private final TableMetadataReader tableMetadata;
    private final HoodieTableFileSystemView fileSystemView;

    private TableStatisticsReader(HoodieTableMetaClient metaClient)
    {
        this.metaClient = metaClient;
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(new TrinoStorageConfiguration());
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
        this.tableMetadata = new TableMetadataReader(
                engineContext, metaClient.getStorage(), metadataConfig, metaClient.getBasePath().toString(), true);
        this.fileSystemView = getFileSystemView(tableMetadata, metaClient);
    }

    public static TableStatisticsReader create(HoodieTableMetaClient metaClient)
    {
        return new TableStatisticsReader(metaClient);
    }

    /**
     * Retrieves table statistics of a Hudi table based on the latest commit and specified columns.
     *
     * @param latestCommit the most recent commit at which to retrieve the statistics
     * @param columnHandles a list of {@link HiveColumnHandle} representing the columns for which statistics are needed
     * @return {@link TableStatistics} object containing the statistics of the specified columns, or empty statistics if unable to retrieve
     */
    public TableStatistics getTableStatistics(HoodieInstant latestCommit,
                                              List<HiveColumnHandle> columnHandles)
    {
        List<String> columnNames = columnHandles.stream()
                .map(HiveColumnHandle::getName)
                .toList();
        Map<String, HoodieColumnRangeMetadata> columnStatsMap = getColumnStats(latestCommit, tableMetadata, fileSystemView, columnNames);
        if (columnStatsMap.isEmpty()) {
            log.warn("Unable to get column stats from metadata table for table, returning empty table statistics: %s",
                    metaClient.getBasePath());
            return TableStatistics.empty();
        }
        long rowCount = columnStatsMap.values().stream()
                .map(e -> e.getNullCount() + e.getValueCount())
                .max(Long::compare)
                .get();
        ImmutableMap.Builder<ColumnHandle, ColumnStatistics> columnHandleBuilder = ImmutableMap.builder();
        for (HiveColumnHandle columnHandle : columnHandles) {
            HoodieColumnRangeMetadata columnStats = columnStatsMap.get(columnHandle.getName());
            if (columnStats == null) {
                log.warn("Unable to get column stats for column %s in table %s",
                        columnHandle.getName(), metaClient.getBasePath());
                continue;
            }
            ColumnStatistics.Builder columnStatisticsBuilder = new ColumnStatistics.Builder();
            long totalCount = columnStats.getNullCount() + columnStats.getValueCount();
            columnStatisticsBuilder.setNullsFraction(Estimate.of(
                    columnStats.getNullCount() / (double) totalCount));
            columnStatisticsBuilder.setDataSize(Estimate.of(columnStats.getTotalUncompressedSize() / (double) totalCount));
            columnHandleBuilder.put(columnHandle, columnStatisticsBuilder.build());
        }
        return new TableStatistics(Estimate.of(rowCount), columnHandleBuilder.buildOrThrow());
    }

    private static Map<String, HoodieColumnRangeMetadata> getColumnStats(
            HoodieInstant latestCommit,
            TableMetadataReader tableMetadata,
            HoodieTableFileSystemView fileSystemView,
            List<String> columnNames)
    {
        fileSystemView.loadAllPartitions();
        List<Pair<String, String>> filePaths = fileSystemView.getAllLatestBaseFilesBeforeOrOn(latestCommit.requestedTime())
                .entrySet()
                .stream().flatMap(entry -> entry.getValue()
                        .map(baseFile -> Pair.of(entry.getKey(), baseFile.getFileName())))
                .toList();
        return tableMetadata.getColumnStats(filePaths, columnNames);
    }
}
