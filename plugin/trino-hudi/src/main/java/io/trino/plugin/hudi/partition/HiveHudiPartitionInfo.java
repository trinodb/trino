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

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_PARTITION_NOT_FOUND;
import static io.trino.plugin.hudi.HudiUtil.buildPartitionKeys;
import static io.trino.plugin.hudi.HudiUtil.partitionMatchesPredicates;
import static java.lang.String.format;

public class HiveHudiPartitionInfo
        implements HudiPartitionInfo
{
    private final Table table;
    private final List<HiveColumnHandle> partitionColumnHandles;
    private final TupleDomain<HiveColumnHandle> constraintSummary;
    private final String hivePartitionName;
    private final List<Column> partitionColumns;
    private final HiveMetastore hiveMetastore;
    private String relativePartitionPath;
    private List<HivePartitionKey> hivePartitionKeys;

    public HiveHudiPartitionInfo(
            String hivePartitionName,
            List<Column> partitionColumns,
            List<HiveColumnHandle> partitionColumnHandles,
            TupleDomain<HiveColumnHandle> constraintSummary,
            Table table,
            HiveMetastore hiveMetastore)
    {
        this.table = table;
        this.partitionColumnHandles = partitionColumnHandles;
        this.constraintSummary = constraintSummary;
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
        if (relativePartitionPath == null) {
            loadPartitionInfo(hiveMetastore.getPartition(table, HiveUtil.toPartitionValues(hivePartitionName)));
        }
        return relativePartitionPath;
    }

    @Override
    public List<HivePartitionKey> getHivePartitionKeys()
    {
        if (hivePartitionKeys == null) {
            loadPartitionInfo(hiveMetastore.getPartition(table, HiveUtil.toPartitionValues(hivePartitionName)));
        }
        return hivePartitionKeys;
    }

    @Override
    public boolean doesMatchPredicates()
    {
        if (hivePartitionName.equals("")) {
            hivePartitionKeys = ImmutableList.of();
            return true;
        }
        return partitionMatchesPredicates(table.getSchemaTableName(), hivePartitionName, partitionColumnHandles, constraintSummary);
    }

    @Override
    public void loadPartitionInfo(Optional<Partition> partition)
    {
        if (partition.isEmpty()) {
            throw new TrinoException(HUDI_PARTITION_NOT_FOUND, format("Cannot find partition in Hive Metastore: %s", hivePartitionName));
        }
        this.relativePartitionPath = getRelativePartitionPath(
                Location.of(table.getStorage().getLocation()),
                Location.of(partition.get().getStorage().getLocation()));
        this.hivePartitionKeys = buildPartitionKeys(partitionColumns, partition.get().getValues());
    }

    private static String getRelativePartitionPath(Location baseLocation, Location fullPartitionLocation)
    {
        String basePath = baseLocation.path();
        String fullPartitionPath = fullPartitionLocation.path();

        if (!fullPartitionPath.startsWith(basePath)) {
            throw new IllegalArgumentException("Partition location does not belong to base-location");
        }

        String baseLocationParent = baseLocation.parentDirectory().path();
        String baseLocationName = baseLocation.fileName();
        int partitionStartIndex = fullPartitionPath.indexOf(
                baseLocationName,
                baseLocationParent == null ? 0 : baseLocationParent.length());
        // Partition-Path could be empty for non-partitioned tables
        boolean isNonPartitionedTable = partitionStartIndex + baseLocationName.length() == fullPartitionPath.length();
        return isNonPartitionedTable ? "" : fullPartitionPath.substring(partitionStartIndex + baseLocationName.length() + 1);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hivePartitionName", hivePartitionName)
                .add("hivePartitionKeys", hivePartitionKeys)
                .toString();
    }
}
