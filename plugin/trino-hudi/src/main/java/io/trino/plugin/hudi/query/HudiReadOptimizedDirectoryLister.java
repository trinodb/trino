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
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hudi.HudiUtil.getFileStatus;

public class HudiReadOptimizedDirectoryLister
        implements HudiDirectoryLister
{
    private final HudiTableHandle tableHandle;
    private final HiveMetastore hiveMetastore;
    private final Table hiveTable;
    private final List<HiveColumnHandle> partitionColumnHandles;
    private final HoodieTableFileSystemView fileSystemView;
    private final List<Column> partitionColumns;

    public HudiReadOptimizedDirectoryLister(
            HoodieMetadataConfig metadataConfig,
            HoodieEngineContext engineContext,
            HudiTableHandle tableHandle,
            HoodieTableMetaClient metaClient,
            HiveMetastore hiveMetastore,
            Table hiveTable,
            List<HiveColumnHandle> partitionColumnHandles,
            List<Column> partitionColumns)
    {
        this.tableHandle = tableHandle;
        this.hiveMetastore = hiveMetastore;
        this.hiveTable = hiveTable;
        this.partitionColumnHandles = partitionColumnHandles;
        this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext, metaClient, metadataConfig);
        this.partitionColumns = partitionColumns;
    }

    @Override
    public List<HudiPartitionInfo> getPartitionsToScan(List<String> partitionNames)
    {
        Map<String, Optional<Partition>> partitionNameToPartitionMap = getPartitions(partitionNames);

        List<HudiPartitionInfo> allPartitionInfoList = partitionNames.stream()
                .map(hivePartitionName -> new HiveHudiPartitionInfo(
                        hivePartitionName,
                        partitionColumns,
                        partitionColumnHandles,
                        tableHandle.getPartitionPredicates(),
                        hiveTable,
                        hiveMetastore))
                .collect(Collectors.toList());

        return allPartitionInfoList.stream()
                .filter(partitionInfo -> {
                    String partitionName = partitionInfo.getHivePartitionName();
                    Optional<Partition> optionalPartition = partitionNameToPartitionMap.get(partitionName);
                    return partitionInfo.getHivePartitionKeysFromPartition(optionalPartition).isEmpty() || partitionInfo.doesMatchPredicates();
                })
                .collect(Collectors.toList());

    }

    @Override
    public List<FileStatus> listStatus(HudiPartitionInfo partitionInfo)
    {
        return fileSystemView.getLatestBaseFiles(partitionInfo.getRelativePartitionPath())
                .map(baseFile -> getFileStatus(baseFile))
                .collect(toImmutableList());
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
