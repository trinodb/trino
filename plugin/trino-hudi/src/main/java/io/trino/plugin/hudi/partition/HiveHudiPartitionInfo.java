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

import io.trino.filesystem.Location;
import io.trino.metastore.Partition;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.hudi.HudiUtil.buildPartitionKeys;
import static io.trino.plugin.hudi.HudiUtil.partitionMatchesPredicates;

public class HiveHudiPartitionInfo
        implements HudiPartitionInfo
{
    public static final String NON_PARTITION = "";

    private final SchemaTableName schemaTableName;
    private final List<HiveColumnHandle> partitionColumnHandles;
    private final TupleDomain<HiveColumnHandle> constraintSummary;
    private final String hivePartitionName;
    private final String relativePartitionPath;
    private final List<HivePartitionKey> hivePartitionKeys;

    public HiveHudiPartitionInfo(
            SchemaTableName schemaTableName,
            Location tableLocation,
            String hivePartitionName,
            Partition partition,
            List<HiveColumnHandle> partitionColumnHandles,
            TupleDomain<HiveColumnHandle> constraintSummary)
    {
        this.schemaTableName = schemaTableName;
        this.partitionColumnHandles = partitionColumnHandles;
        this.constraintSummary = constraintSummary;
        this.hivePartitionName = hivePartitionName;
        this.relativePartitionPath = getRelativePartitionPath(
                tableLocation,
                Location.of(partition.getStorage().getLocation()));
        this.hivePartitionKeys = buildPartitionKeys(
                partitionColumnHandles, partition.getValues());
    }

    @Override
    public String getRelativePartitionPath()
    {
        return relativePartitionPath;
    }

    @Override
    public List<HivePartitionKey> getHivePartitionKeys()
    {
        return hivePartitionKeys;
    }

    @Override
    public boolean doesMatchPredicates()
    {
        if (hivePartitionName.equals(NON_PARTITION)) {
            return true;
        }
        return partitionMatchesPredicates(schemaTableName, hivePartitionName, partitionColumnHandles, constraintSummary);
    }

    public String getHivePartitionName()
    {
        return hivePartitionName;
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
        return isNonPartitionedTable ? NON_PARTITION : fullPartitionPath.substring(partitionStartIndex + baseLocationName.length() + 1);
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
