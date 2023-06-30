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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.hive.thrift.metastore.StorageDescriptor;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.util.HiveBucketing;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class HiveBucketProperty
{
    private final List<String> bucketedBy;
    private final BucketingVersion bucketingVersion;
    private final int bucketCount;
    private final List<SortingColumn> sortedBy;

    @JsonCreator
    public HiveBucketProperty(
            @JsonProperty("bucketedBy") List<String> bucketedBy,
            @JsonProperty("bucketingVersion") BucketingVersion bucketingVersion,
            @JsonProperty("bucketCount") int bucketCount,
            @JsonProperty("sortedBy") List<SortingColumn> sortedBy)
    {
        this.bucketedBy = ImmutableList.copyOf(requireNonNull(bucketedBy, "bucketedBy is null"));
        this.bucketingVersion = requireNonNull(bucketingVersion, "bucketingVersion is null");
        this.bucketCount = bucketCount;
        this.sortedBy = ImmutableList.copyOf(requireNonNull(sortedBy, "sortedBy is null"));
    }

    public static Optional<HiveBucketProperty> fromStorageDescriptor(Map<String, String> tableParameters, StorageDescriptor storageDescriptor, String tablePartitionName)
    {
        boolean bucketColsSet = storageDescriptor.isSetBucketCols() && !storageDescriptor.getBucketCols().isEmpty();
        boolean numBucketsSet = storageDescriptor.isSetNumBuckets() && storageDescriptor.getNumBuckets() > 0;
        if (!numBucketsSet) {
            // In Hive, a table is considered as not bucketed when its bucketCols is set but its numBucket is not set.
            return Optional.empty();
        }
        if (!bucketColsSet) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Table/partition metadata has 'numBuckets' set, but 'bucketCols' is not set: " + tablePartitionName);
        }
        List<SortingColumn> sortedBy = ImmutableList.of();
        if (storageDescriptor.isSetSortCols()) {
            sortedBy = storageDescriptor.getSortCols().stream()
                    .map(order -> SortingColumn.fromMetastoreApiOrder(order, tablePartitionName))
                    .collect(toImmutableList());
        }
        BucketingVersion bucketingVersion = HiveBucketing.getBucketingVersion(tableParameters);
        List<String> bucketColumnNames = storageDescriptor.getBucketCols().stream()
                // Ensure that the names used for the bucket columns are specified in lower case to match the names of the table columns
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableList());
        return Optional.of(new HiveBucketProperty(bucketColumnNames, bucketingVersion, storageDescriptor.getNumBuckets(), sortedBy));
    }

    @JsonProperty
    public List<String> getBucketedBy()
    {
        return bucketedBy;
    }

    @JsonProperty
    public BucketingVersion getBucketingVersion()
    {
        return bucketingVersion;
    }

    @JsonProperty
    public int getBucketCount()
    {
        return bucketCount;
    }

    @JsonProperty
    public List<SortingColumn> getSortedBy()
    {
        return sortedBy;
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
        HiveBucketProperty that = (HiveBucketProperty) o;
        return bucketingVersion == that.bucketingVersion &&
                bucketCount == that.bucketCount &&
                Objects.equals(bucketedBy, that.bucketedBy) &&
                Objects.equals(sortedBy, that.sortedBy);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketedBy, bucketingVersion, bucketCount, sortedBy);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketedBy", bucketedBy)
                .add("bucketingVersion", bucketingVersion)
                .add("bucketCount", bucketCount)
                .add("sortedBy", sortedBy)
                .toString();
    }
}
