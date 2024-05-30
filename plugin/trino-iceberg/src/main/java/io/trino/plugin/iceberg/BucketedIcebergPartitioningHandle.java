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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BucketedIcebergPartitioningHandle
        extends IcebergPartitioningHandle
        implements BucketedPartitioningHandle
{
    private final SchemaTableName tableName;
    private final long snapshotId;
    private final int specId;
    // we can support non bucketed partition column by assign an fixed implicit bucket count
    private final List<Integer> bucketCounts;
    // Values in this list may be equal or lower than the bucketCounts to make it compatible with other partitioningHandle
    private final List<Integer> maxCompatibleBucketCounts;

    @JsonCreator
    public BucketedIcebergPartitioningHandle(
            @JsonProperty("tableName") SchemaTableName tableName,
            @JsonProperty("snapshotId") long snapshotId,
            @JsonProperty("specId") int specId,
            @JsonProperty("partitioning") List<String> partitioning,
            @JsonProperty("partitioningColumns") List<IcebergColumnHandle> partitioningColumns,
            @JsonProperty("bucketCounts") List<Integer> bucketCounts,
            @JsonProperty("maxCompatibleBucketCounts") List<Integer> maxCompatibleBucketCounts)
    {
        super(partitioning, partitioningColumns);
        checkArgument(partitioning.size() == partitioningColumns.size(), "number of partititions must match with partitioningColumns");
        checkArgument(bucketCounts.size() == partitioning.size(), "number of bucketCounts must match with partitioning");
        checkArgument(bucketCounts.size() == maxCompatibleBucketCounts.size(), "number of bucketCounts must match with maxCompatibleBucketCounts");
        checkArgument(bucketCounts.stream().allMatch(n -> n > 0), "bucketCounts must be all > 0 ");
        checkArgument(maxCompatibleBucketCounts.stream().allMatch(n -> n > 0), "maxCompatibleBucketCounts must be all > 0 ");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.snapshotId = snapshotId;
        this.specId = specId;
        this.bucketCounts = requireNonNull(bucketCounts, "bucketCounts is null");
        this.maxCompatibleBucketCounts = requireNonNull(maxCompatibleBucketCounts, "maxCompatibleBucketCount is null");
    }

    @JsonProperty
    public SchemaTableName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public long getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    public int getSpecId()
    {
        return specId;
    }

    @JsonProperty
    public List<Integer> getBucketCounts()
    {
        return bucketCounts;
    }

    @JsonProperty
    @Override
    public List<Integer> getMaxCompatibleBucketCounts()
    {
        return maxCompatibleBucketCounts;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        BucketedIcebergPartitioningHandle that = (BucketedIcebergPartitioningHandle) o;
        return snapshotId == that.snapshotId &&
                specId == that.specId &&
                Objects.equals(maxCompatibleBucketCounts, that.maxCompatibleBucketCounts) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(bucketCounts, that.bucketCounts);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), tableName, snapshotId, specId, bucketCounts, maxCompatibleBucketCounts);
    }

    public BucketedIcebergPartitioningHandle withUpdatedBucketCount(List<Integer> maxCompatibleBucketCounts)
    {
        return new BucketedIcebergPartitioningHandle(
                tableName,
                snapshotId,
                specId,
                getPartitioning(),
                getPartitioningColumns(),
                bucketCounts,
                maxCompatibleBucketCounts);
    }
}
