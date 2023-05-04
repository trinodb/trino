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
package io.trino.plugin.hudi.query;

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hudi.HudiFileStatus;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.files.HudiBaseFile;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.plugin.hudi.table.HudiTableFileSystemView;
import io.trino.plugin.hudi.table.HudiTableMetaClient;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class HudiReadOptimizedDirectoryLister
        implements HudiDirectoryLister
{
    private final HudiTableHandle tableHandle;
    private final HiveMetastore hiveMetastore;
    private final Table hiveTable;
    private final SchemaTableName tableName;
    private final List<HiveColumnHandle> partitionColumnHandles;
    private final HudiTableFileSystemView fileSystemView;
    private final TupleDomain<String> partitionKeysFilter;
    private final List<Column> partitionColumns;

    private List<String> hivePartitionNames;

    public HudiReadOptimizedDirectoryLister(
            HudiTableHandle tableHandle,
            HudiTableMetaClient metaClient,
            HiveMetastore hiveMetastore,
            Table hiveTable,
            List<HiveColumnHandle> partitionColumnHandles)
    {
        this.tableHandle = tableHandle;
        this.tableName = tableHandle.getSchemaTableName();
        this.hiveMetastore = hiveMetastore;
        this.hiveTable = hiveTable;
        this.partitionColumnHandles = partitionColumnHandles;
        this.fileSystemView = new HudiTableFileSystemView(metaClient, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
        this.partitionKeysFilter = MetastoreUtil.computePartitionKeyFilter(partitionColumnHandles, tableHandle.getPartitionPredicates());
        this.partitionColumns = hiveTable.getPartitionColumns();
    }

    @Override
    public List<HudiPartitionInfo> getPartitionsToScan()
    {
        if (hivePartitionNames == null) {
            hivePartitionNames = partitionColumns.isEmpty()
                    ? Collections.singletonList("")
                    : getPartitionNamesFromHiveMetastore(partitionKeysFilter);
        }

        List<HudiPartitionInfo> allPartitionInfoList = hivePartitionNames.stream()
                .map(hivePartitionName -> new HiveHudiPartitionInfo(
                        hivePartitionName,
                        partitionColumns,
                        partitionColumnHandles,
                        tableHandle.getPartitionPredicates(),
                        hiveTable,
                        hiveMetastore))
                .collect(toImmutableList());

        return allPartitionInfoList.stream()
                .filter(partitionInfo -> partitionInfo.getHivePartitionKeys().isEmpty() || partitionInfo.doesMatchPredicates())
                .collect(toImmutableList());
    }

    @Override
    public List<HudiFileStatus> listStatus(HudiPartitionInfo partitionInfo)
    {
        return fileSystemView.getLatestBaseFiles(partitionInfo.getRelativePartitionPath())
                .map(HudiBaseFile::getFileEntry)
                .map(fileEntry -> new HudiFileStatus(
                        fileEntry.location(),
                        false,
                        fileEntry.length(),
                        fileEntry.lastModified().toEpochMilli(),
                        fileEntry.blocks().map(listOfBlocks -> (!listOfBlocks.isEmpty()) ? listOfBlocks.get(0).length() : 0).orElse(0L)))
                .collect(toImmutableList());
    }

    private List<String> getPartitionNamesFromHiveMetastore(TupleDomain<String> partitionKeysFilter)
    {
        return hiveMetastore.getPartitionNamesByFilter(
                tableName.getSchemaName(),
                tableName.getTableName(),
                partitionColumns.stream().map(Column::getName).collect(toImmutableList()),
                partitionKeysFilter).orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));
    }

    @Override
    public Map<String, Optional<Partition>> getPartitions(List<String> partitionNames)
    {
        return hiveMetastore.getPartitionsByNames(hiveTable, partitionNames);
    }

    @Override
    public void close()
    {
        if (fileSystemView != null && !fileSystemView.isClosed()) {
            fileSystemView.close();
        }
    }
}
