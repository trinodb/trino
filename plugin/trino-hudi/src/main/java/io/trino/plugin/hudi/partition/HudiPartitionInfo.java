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
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;

public abstract class HudiPartitionInfo
{
    protected final Table table;
    protected final List<HiveColumnHandle> partitionColumnHandles;
    protected final TupleDomain<HiveColumnHandle> constraintSummary;

    // Relative partition path
    protected String relativePartitionPath;
    // Hive partition name containing partition column key-value pairs
    protected String hivePartitionName;
    protected List<HivePartitionKey> hivePartitionKeys;

    public HudiPartitionInfo(
            Table table,
            List<HiveColumnHandle> partitionColumnHandles,
            TupleDomain<HiveColumnHandle> constraintSummary)
    {
        this.table = table;
        this.partitionColumnHandles = partitionColumnHandles;
        this.constraintSummary = constraintSummary;
    }

    public Table getTable()
    {
        return table;
    }

    public abstract String getRelativePartitionPath();

    public abstract String getHivePartitionName();

    public abstract List<HivePartitionKey> getHivePartitionKeys();

    public abstract boolean doesMatchPredicates();

    public abstract String getComparingKey();

    public abstract void loadPartitionInfo(Optional<Partition> partition);
}
