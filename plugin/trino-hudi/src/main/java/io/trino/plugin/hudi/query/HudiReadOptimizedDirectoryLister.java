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

import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.storage.StoragePathInfo;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class HudiReadOptimizedDirectoryLister
        implements HudiDirectoryLister
{
    private static final Logger LOG = Logger.get(HudiReadOptimizedDirectoryLister.class);

    private final HoodieTableFileSystemView fileSystemView;
    private final List<Column> partitionColumns;
    private final Map<String, HudiPartitionInfo> allPartitionInfoMap;

    public HudiReadOptimizedDirectoryLister(
            HudiTableHandle tableHandle,
            HoodieTableMetaClient metaClient,
            HiveMetastore hiveMetastore,
            Table hiveTable,
            List<HiveColumnHandle> partitionColumnHandles,
            List<String> hivePartitionNames,
            boolean ignoreAbsentPartitions)
    {
        this.fileSystemView = new HoodieTableFileSystemView(
                metaClient,
                metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants(),
                ignoreAbsentPartitions);
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
        LOG.debug("List partition: partitionInfo=%s", partitionInfo);
        String partition = partitionInfo.getRelativePartitionPath();
        return fileSystemView.getLatestBaseFiles(partitionInfo.getRelativePartitionPath())
                .map(baseFile -> new FileSlice(
                        new HoodieFileGroupId(partition, baseFile.getFileId()),
                        baseFile.getCommitTime(),
                        baseFile,
                        Collections.emptyList()))
                .collect(toImmutableList());
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

    private static StoragePathInfo getStoragePathInfo(HoodieBaseFile baseFile)
    {
        if (baseFile.getBootstrapBaseFile().isPresent()) {
            return baseFile.getBootstrapBaseFile().get().getPathInfo();
        }
        return baseFile.getPathInfo();
    }
}
