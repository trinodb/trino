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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.util.HiveBucketing.HiveBucketFilter;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Result of fetching Partitions in the HivePartitionManager interface.
 * <p>
 * Results are comprised of two parts:
 * 1) The actual partitions
 * 2) The TupleDomain that represents the values that the connector was not able to pre-evaluate
 * when generating the partitions and will need to be double-checked by the final execution plan.
 */
public class HivePartitionResult
{
    private final List<HiveColumnHandle> partitionColumns;
    private final Iterable<HivePartition> partitions;
    private final TupleDomain<ColumnHandle> effectivePredicate;
    private final TupleDomain<HiveColumnHandle> compactEffectivePredicate;
    private final Optional<HiveBucketHandle> bucketHandle;
    private final Optional<HiveBucketFilter> bucketFilter;
    private final Optional<List<String>> partitionNames;

    public HivePartitionResult(
            List<HiveColumnHandle> partitionColumns,
            Optional<List<String>> partitionNames,
            Iterable<HivePartition> partitions,
            TupleDomain<ColumnHandle> effectivePredicate,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<HiveBucketFilter> bucketFilter)
    {
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
        this.partitionNames = requireNonNull(partitionNames, "partitionNames is null").map(ImmutableList::copyOf);
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.compactEffectivePredicate = requireNonNull(compactEffectivePredicate, "compactEffectivePredicate is null");
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
        this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");
    }

    public List<HiveColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    public Optional<List<String>> getPartitionNames()
    {
        return partitionNames;
    }

    public Iterator<HivePartition> getPartitions()
    {
        return partitions.iterator();
    }

    public TupleDomain<ColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    public TupleDomain<HiveColumnHandle> getCompactEffectivePredicate()
    {
        return compactEffectivePredicate;
    }

    public Optional<HiveBucketHandle> getBucketHandle()
    {
        return bucketHandle;
    }

    public Optional<HiveBucketFilter> getBucketFilter()
    {
        return bucketFilter;
    }
}
