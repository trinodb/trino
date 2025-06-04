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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HiveTypeName;
import io.trino.metastore.Partition;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HivePartitionMetadata
{
    private final Optional<Partition> partition;
    private final HivePartition hivePartition;
    private final Map<Integer, HiveTypeName> hiveColumnCoercions;

    HivePartitionMetadata(
            HivePartition hivePartition,
            Optional<Partition> partition,
            Map<Integer, HiveTypeName> hiveColumnCoercions)
    {
        this.partition = requireNonNull(partition, "partition is null");
        this.hivePartition = requireNonNull(hivePartition, "hivePartition is null");
        this.hiveColumnCoercions = ImmutableMap.copyOf(requireNonNull(hiveColumnCoercions, "hiveColumnCoercions is null"));
    }

    public HivePartition getHivePartition()
    {
        return hivePartition;
    }

    /**
     * @return empty if this HivePartitionMetadata represents an unpartitioned table
     */
    public Optional<Partition> getPartition()
    {
        return partition;
    }

    public Map<Integer, HiveTypeName> getHiveColumnCoercions()
    {
        return hiveColumnCoercions;
    }
}
