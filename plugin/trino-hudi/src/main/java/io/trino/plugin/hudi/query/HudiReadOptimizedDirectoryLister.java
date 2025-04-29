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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.Location;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.HudiFileStatus;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.storage.StoragePathInfo;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class HudiReadOptimizedDirectoryLister
        implements HudiDirectoryLister
{
    private static final Logger LOG = Logger.get(HudiReadOptimizedDirectoryLister.class);
    private static final long MIN_BLOCK_SIZE = DataSize.of(32, MEGABYTE).toBytes();

    private final HoodieTableFileSystemView fileSystemView;
    private final List<Column> partitionColumns;
    private final Map<String, HudiPartitionInfo> allPartitionInfoMap;

    private static final Cache<String, List<HudiFileStatus>> cache = CacheBuilder.newBuilder()
            .maximumWeight(100000000)
            .weigher((Weigher<String, List<HudiFileStatus>>) (_, value) -> value.size())
            .expireAfterWrite(new Duration(15, TimeUnit.MINUTES).toMillis(), TimeUnit.MILLISECONDS)
            .recordStats()
            .build();

    private static final Cache<String, HoodieTableFileSystemView> fsViewCache = CacheBuilder.newBuilder()
            .expireAfterWrite(new Duration(120, TimeUnit.MINUTES).toMillis(), TimeUnit.MILLISECONDS)
            .recordStats()
            .build();

    public HudiReadOptimizedDirectoryLister(
            HudiTableHandle tableHandle,
            HoodieTableMetaClient metaClient,
            HiveMetastore hiveMetastore,
            Table hiveTable,
            List<HiveColumnHandle> partitionColumnHandles,
            List<String> hivePartitionNames,
            boolean ignoreAbsentPartitions)
    {
        this.fileSystemView = getFileSystemView(metaClient, ignoreAbsentPartitions);
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

    private static HoodieTableFileSystemView getFileSystemView(HoodieTableMetaClient metaClient, boolean ignoreAbsentPartitions)
    {
        String timelineHash = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getTimelineHash();
        HoodieTableFileSystemView fsView = fsViewCache.getIfPresent(timelineHash);
        if (fsView != null) {
            return fsView;
        }
        LOG.debug("fsViewCache miss for table: " + metaClient.getBasePath());
        fsView = new HoodieTableFileSystemView(
                metaClient,
                metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants(),
                ignoreAbsentPartitions);
        fsViewCache.put(timelineHash, fsView);
        return fsView;
    }

    @Override
    public List<HudiFileStatus> listStatus(HudiPartitionInfo partitionInfo)
    {
        LOG.debug("List partition: partitionInfo=%s", partitionInfo);
        String timelineHash = fileSystemView.getTimeline().getCommitsTimeline().filterCompletedInstants().getTimelineHash();
        String relativePartitionPath = partitionInfo.getRelativePartitionPath();
        List<HudiFileStatus> fileStatuses = cache.getIfPresent(relativePartitionPath + timelineHash);
        if (fileStatuses != null) {
            return fileStatuses;
        }
        LOG.debug("fileStatusCache miss for partition: " + partitionInfo);
        fileStatuses = fileSystemView.getLatestBaseFiles(partitionInfo.getRelativePartitionPath())
                .map(HudiReadOptimizedDirectoryLister::getStoragePathInfo)
                .map(fileEntry -> new HudiFileStatus(
                        Location.of(fileEntry.getPath().toString()),
                        false,
                        fileEntry.getLength(),
                        fileEntry.getModificationTime(),
                        max(fileEntry.getBlockSize(), min(fileEntry.getLength(), MIN_BLOCK_SIZE))))
                .collect(toImmutableList());
        cache.put(relativePartitionPath + timelineHash, fileStatuses);
        return fileStatuses;
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
