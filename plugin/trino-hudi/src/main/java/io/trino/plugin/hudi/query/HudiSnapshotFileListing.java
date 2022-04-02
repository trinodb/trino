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
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.List;

public class HudiSnapshotFileListing
        extends HudiFileListing
{
    public HudiSnapshotFileListing(
            HoodieMetadataConfig metadataConfig,
            HoodieEngineContext engineContext,
            HudiTableHandle tableHandle,
            HoodieTableMetaClient metaClient,
            HiveMetastore hiveMetastore,
            Table hiveTable,
            List<HiveColumnHandle> partitionColumnHandles,
            boolean shouldSkipMetastoreForPartition)
    {
        super(metadataConfig, engineContext, tableHandle, metaClient, hiveMetastore, hiveTable, partitionColumnHandles, shouldSkipMetastoreForPartition);
    }

    @Override
    public List<HudiPartitionInfo> getPartitionsToScan()
    {
        return null;
    }

    @Override
    public List<FileStatus> listStatus(HudiPartitionInfo partitionInfo)
    {
        return null;
    }
}
