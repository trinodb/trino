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
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.SortingColumn;
import io.trino.plugin.hive.util.HiveBucketing;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @param active is the partitioning enabled for reading
 * @param tableBucketCount number of buckets in the table, as specified in table metadata
 * @param forWrite if true, the partitioning is being used for writing and cannot be changed
 */
public record HiveTablePartitioning(
        boolean active,
        List<HiveColumnHandle> columns,
        HivePartitioningHandle partitioningHandle,
        int tableBucketCount,
        List<SortingColumn> sortedBy,
        boolean forWrite)
{
    public HiveTablePartitioning
    {
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        columns.forEach(column -> checkArgument(column.isBaseColumn(), format("projected column %s is not allowed for bucketing", column)));
        checkArgument(columns.stream().map(HiveColumnHandle::getHiveType).toList().equals(partitioningHandle.getHiveTypes()), "columns do not match partitioning handle");
        checkArgument(tableBucketCount > 0, "tableBucketCount must be greater than zero");
        checkArgument(tableBucketCount >= partitioningHandle.getBucketCount(), "tableBucketCount must be greater than or equal to partitioningHandle.bucketCount");
        sortedBy = ImmutableList.copyOf(requireNonNull(sortedBy, "sortedBy is null"));
        checkArgument(!forWrite || active, "Partitioning must be active for write");
        checkArgument(forWrite || !partitioningHandle.isUsePartitionedBucketing(), "Partitioned bucketing is only supported for write");
    }

    public HiveTablePartitioning(
            boolean active,
            HiveBucketing.BucketingVersion bucketingVersion,
            int bucketCount,
            List<HiveColumnHandle> columns,
            boolean usePartitionedBucketing,
            List<SortingColumn> sortedBy,
            boolean forWrite)
    {
        this(active,
                columns,
                new HivePartitioningHandle(
                        bucketingVersion,
                        bucketCount,
                        columns.stream()
                                .map(HiveColumnHandle::getHiveType)
                                .collect(toImmutableList()),
                        usePartitionedBucketing),
                bucketCount,
                sortedBy,
                forWrite);
    }

    public HiveTablePartitioning withActivePartitioning()
    {
        return new HiveTablePartitioning(true, columns, partitioningHandle, tableBucketCount, sortedBy, forWrite);
    }

    public HiveTablePartitioning withPartitioningHandle(HivePartitioningHandle hivePartitioningHandle)
    {
        return new HiveTablePartitioning(active, columns, hivePartitioningHandle, tableBucketCount, sortedBy, forWrite);
    }

    public HiveBucketProperty toTableBucketProperty()
    {
        return new HiveBucketProperty(
                columns.stream()
                        .map(HiveColumnHandle::getName)
                        .collect(toList()),
                tableBucketCount,
                sortedBy);
    }
}
