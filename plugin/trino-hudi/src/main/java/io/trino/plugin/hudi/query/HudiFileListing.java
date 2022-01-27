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
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.isNull;

public abstract class HudiFileListing
{
    protected final HoodieMetadataConfig metadataConfig;
    protected final HoodieEngineContext engineContext;
    protected final HoodieTableMetaClient metaClient;
    protected final HudiTableHandle tableHandle;
    protected final HiveMetastore hiveMetastore;
    protected final Table hiveTable;
    protected final SchemaTableName tableName;
    protected final List<HiveColumnHandle> partitionColumnHandles;
    protected final boolean shouldSkipMetastoreForPartition;
    protected HoodieTableFileSystemView fileSystemView;
    protected TupleDomain<String> partitionKeysFilter;
    protected List<Column> partitionColumns;

    public HudiFileListing(
            HoodieMetadataConfig metadataConfig, HoodieEngineContext engineContext,
            HudiTableHandle tableHandle, HoodieTableMetaClient metaClient,
            HiveMetastore hiveMetastore, Table hiveTable,
            List<HiveColumnHandle> partitionColumnHandles, boolean shouldSkipMetastoreForPartition)
    {
        this.metadataConfig = metadataConfig;
        this.engineContext = engineContext;
        this.metaClient = metaClient;
        this.tableHandle = tableHandle;
        this.tableName = tableHandle.getSchemaTableName();
        this.hiveMetastore = hiveMetastore;
        this.hiveTable = hiveTable;
        this.partitionColumnHandles = partitionColumnHandles;
        this.shouldSkipMetastoreForPartition = shouldSkipMetastoreForPartition;
    }

    public abstract List<HudiPartitionInfo> getPartitionsToScan();

    public abstract List<FileStatus> listStatus(HudiPartitionInfo partitionInfo);

    public void close()
    {
        if (!isNull(fileSystemView) && !fileSystemView.isClosed()) {
            fileSystemView.close();
        }
    }

    public Map<String, Optional<Partition>> getPartitions(List<String> partitionNames)
    {
        return hiveMetastore.getPartitionsByNames(hiveTable, partitionNames);
    }

    protected void initFileSystemViewAndPredicates()
    {
        if (isNull(fileSystemView)) {
            // These are time-consuming operations
            // Triggering them when getting the partitions
            this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(
                    engineContext, metaClient, metadataConfig);
            this.partitionKeysFilter = MetastoreUtil.computePartitionKeyFilter(
                    partitionColumnHandles, tableHandle.getPartitionPredicates());
        }
    }
}
