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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@JsonSerialize
public record BucketedTablePartitioningHandle(SchemaTableName tableName,
                                              long snapshotId,
                                              int specId,
                                              List<String> partitioning,
                                              List<IcebergColumnHandle> partitioningColumns,
                                              List<Integer> bucketCounts,
                                              List<Integer> maxCompatibleBucketCounts)
        implements BucketedPartitioningHandle
{
    public BucketedTablePartitioningHandle
    {
        checkArgument(partitioning.size() == partitioningColumns.size(), "number of partititions must match with partitioningColumns");
        checkArgument(bucketCounts.size() == partitioning.size(), "number of bucketCounts must match with partitioning");
        checkArgument(bucketCounts.size() == maxCompatibleBucketCounts.size(), "number of bucketCounts must match with maxCompatibleBucketCounts");
        checkArgument(bucketCounts.stream().allMatch(n -> n > 0), "bucketCounts must be all > 0 ");
        checkArgument(maxCompatibleBucketCounts.stream().allMatch(n -> n > 0), "maxCompatibleBucketCounts must be all > 0 ");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(bucketCounts, "bucketCounts is null");
        requireNonNull(maxCompatibleBucketCounts, "maxCompatibleBucketCount is null");
    }

    public BucketedTablePartitioningHandle withUpdatedBucketCount(List<Integer> maxCompatibleBucketCounts)
    {
        return new BucketedTablePartitioningHandle(
                tableName,
                snapshotId,
                specId,
                partitioning(),
                partitioningColumns(),
                bucketCounts,
                maxCompatibleBucketCounts);
    }

    @Override
    public List<Integer> getMaxCompatibleBucketCounts()
    {
        return maxCompatibleBucketCounts;
    }
}
