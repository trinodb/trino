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

package io.trino.plugin.hudi.partition;

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.hudi.HudiUtil;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.exception.HoodieIOException;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.isNull;

public class HudiPartitionHiveInfo
        extends HudiPartitionInfo
{
    private final List<Column> partitionColumns;
    private final HiveMetastore hiveMetastore;

    public HudiPartitionHiveInfo(
            String hivePartitionName,
            List<Column> partitionColumns,
            List<HiveColumnHandle> partitionColumnHandles,
            TupleDomain<HiveColumnHandle> constraintSummary,
            Table table, HiveMetastore hiveMetastore)
    {
        super(table, partitionColumnHandles, constraintSummary);
        this.hivePartitionName = hivePartitionName;
        this.partitionColumns = partitionColumns;
        if (partitionColumns.isEmpty()) {
            this.relativePartitionPath = "";
            this.hivePartitionKeys = Collections.emptyList();
        }
        this.hiveMetastore = hiveMetastore;
    }

    @Override
    public String getRelativePartitionPath()
    {
        if (isNull(relativePartitionPath)) {
            loadPartitionInfoFromHiveMetastore();
        }
        return relativePartitionPath;
    }

    @Override
    public String getHivePartitionName()
    {
        return hivePartitionName;
    }

    @Override
    public List<HivePartitionKey> getHivePartitionKeys()
    {
        if (isNull(hivePartitionKeys)) {
            loadPartitionInfoFromHiveMetastore();
        }
        return hivePartitionKeys;
    }

    @Override
    public boolean doesMatchPredicates()
    {
        return HudiUtil.doesPartitionMatchPredicates(
                table.getSchemaTableName(), hivePartitionName,
                partitionColumnHandles, constraintSummary);
    }

    @Override
    public String getComparingKey()
    {
        return hivePartitionName;
    }

    @Override
    public void loadPartitionInfo(Optional<Partition> partition)
    {
        if (partition.isEmpty()) {
            throw new HoodieIOException(format("Cannot find partition in Hive Metastore: %s", hivePartitionName));
        }
        this.relativePartitionPath = FSUtils.getRelativePartitionPath(
                new Path(table.getStorage().getLocation()),
                new Path(partition.get().getStorage().getLocation()));
        this.hivePartitionKeys =
                HudiUtil.buildPartitionKeys(partitionColumns, partition.get().getValues());
    }

    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("HudiPartitionHiveInfo{");
        stringBuilder.append("hivePartitionName=");
        stringBuilder.append(hivePartitionName);
        if (!isNull(hivePartitionKeys)) {
            stringBuilder.append(",hivePartitionKeys=");
            stringBuilder.append(hivePartitionKeys);
        }
        stringBuilder.append("}");
        return stringBuilder.toString();
    }

    private void loadPartitionInfoFromHiveMetastore()
    {
        Optional<Partition> partition = hiveMetastore.getPartition(table, HiveUtil.toPartitionValues(hivePartitionName));
        loadPartitionInfo(partition);
    }
}
