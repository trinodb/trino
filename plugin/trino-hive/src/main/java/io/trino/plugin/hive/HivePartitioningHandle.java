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
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.spi.connector.ConnectorPartitioningHandle;

import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HivePartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final BucketingVersion bucketingVersion;
    private final int bucketCount;
    private final List<String> partitions;
    private final List<HiveType> hiveTypes;
    private final OptionalInt maxCompatibleBucketCount;
    private final boolean usePartitionedBucketingForWrites;

    @JsonCreator
    public HivePartitioningHandle(
            @JsonProperty("bucketingVersion") BucketingVersion bucketingVersion,
            @JsonProperty("bucketCount") int bucketCount,
            @JsonProperty("hiveBucketTypes") List<HiveType> hiveTypes,
            @JsonProperty("maxCompatibleBucketCount") OptionalInt maxCompatibleBucketCount,
            @JsonProperty("usePartitionedBucketingForWrites") boolean usePartitionedBucketingForWrites,
            @JsonProperty("partitions") List<String> partitions)
    {
        this.bucketingVersion = requireNonNull(bucketingVersion, "bucketingVersion is null");
        this.bucketCount = bucketCount;
        this.hiveTypes = requireNonNull(hiveTypes, "hiveTypes is null");
        this.maxCompatibleBucketCount = maxCompatibleBucketCount;
        this.partitions = partitions;
        this.usePartitionedBucketingForWrites = usePartitionedBucketingForWrites;
    }

    public static HivePartitioningHandle partitionsOnly(List<String> partitions)
    {
        return new HivePartitioningHandle(
                BucketingVersion.BUCKETING_V2,
                1,
                ImmutableList.of(),
                OptionalInt.empty(),
                false,
                partitions);
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
    public List<HiveType> getHiveTypes()
    {
        return hiveTypes;
    }

    @JsonProperty
    public OptionalInt getMaxCompatibleBucketCount()
    {
        return maxCompatibleBucketCount;
    }

    @JsonProperty
    public boolean isUsePartitionedBucketingForWrites()
    {
        return usePartitionedBucketingForWrites;
    }

    @JsonProperty
    public List<String> getPartitions()
    {
        return this.partitions;
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper(this)
                .add("buckets", bucketCount)
                .add("hiveTypes", hiveTypes)
                .add("partitions", partitions);
        if (usePartitionedBucketingForWrites) {
            helper.add("usePartitionedBucketingForWrites", usePartitionedBucketingForWrites);
        }
        return helper.toString();
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
        HivePartitioningHandle that = (HivePartitioningHandle) o;
        return bucketCount == that.bucketCount &&
                Objects.equals(partitions, that.partitions) &&
                usePartitionedBucketingForWrites == that.usePartitionedBucketingForWrites &&
                Objects.equals(hiveTypes, that.hiveTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketCount, hiveTypes, partitions, usePartitionedBucketingForWrites);
    }
}
