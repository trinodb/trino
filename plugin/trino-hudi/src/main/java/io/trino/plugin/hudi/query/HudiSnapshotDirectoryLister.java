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

import com.google.common.collect.ImmutableList;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class HudiSnapshotDirectoryLister
        implements HudiDirectoryLister
{
    private final HoodieTableFileSystemView fileSystemView;
    private final List<Column> partitionColumns;
    private final Map<String, HudiPartitionInfo> allPartitionInfoMap;

    public HudiSnapshotDirectoryLister(
            HudiTableHandle tableHandle,
            HoodieTableMetaClient metaClient,
            boolean enableMetadataTable,
            HiveMetastore hiveMetastore,
            Table hiveTable,
            List<HiveColumnHandle> partitionColumnHandles,
            List<String> hivePartitionNames,
            String commitTime)
    {
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(enableMetadataTable)
                .build();
        this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(
                new HoodieLocalEngineContext(new TrinoStorageConfiguration()), metaClient, metadataConfig);
        if (enableMetadataTable) {
            fileSystemView.loadAllPartitions();
        }
        //new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
        this.partitionColumns = hiveTable.getPartitionColumns();
        this.allPartitionInfoMap = hivePartitionNames.stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        hivePartitionName -> new HiveHudiPartitionInfo(
                                hivePartitionName,
                                partitionColumns,
                                partitionColumnHandles,
                                tableHandle.getPartitionPredicates(),
                                hiveTable,
                                hiveMetastore)));
    }

    @Override
    public List<FileSlice> listStatus(HudiPartitionInfo partitionInfo, String commitTime)
    {
        ImmutableList<FileSlice> collect = fileSystemView.getLatestFileSlicesBeforeOrOn(partitionInfo.getRelativePartitionPath(), commitTime, false)
                .collect(toImmutableList());
        return collect;
    }

    @Override
    public Optional<HudiPartitionInfo> getPartitionInfo(String partition)
    {
        return Optional.ofNullable(allPartitionInfoMap.get(partition));
    }

    @Override
    public void close()
    {
        if (fileSystemView != null && !fileSystemView.isClosed()) {
            fileSystemView.close();
        }
    }
}
