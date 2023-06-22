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
package io.trino.plugin.hudi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class HudiSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = toIntExact(instanceSize(HudiSplit.class));

    private final String location;
    private final long start;
    private final long length;
    private final long fileSize;
    private final long fileModifiedTime;
    private final TupleDomain<HiveColumnHandle> predicate;
    private final List<HivePartitionKey> partitionKeys;
    private final SplitWeight splitWeight;

    @JsonCreator
    public HudiSplit(
            @JsonProperty("location") String location,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("fileModifiedTime") long fileModifiedTime,
            @JsonProperty("predicate") TupleDomain<HiveColumnHandle> predicate,
            @JsonProperty("partitionKeys") List<HivePartitionKey> partitionKeys,
            @JsonProperty("splitWeight") SplitWeight splitWeight)
    {
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkArgument(start + length <= fileSize, "fileSize must be at least start + length");

        this.location = requireNonNull(location, "location is null");
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.fileModifiedTime = fileModifiedTime;
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.partitionKeys = ImmutableList.copyOf(requireNonNull(partitionKeys, "partitionKeys is null"));
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @JsonIgnore
    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("location", location)
                .put("start", start)
                .put("length", length)
                .put("fileSize", fileSize)
                .put("fileModifiedTime", fileModifiedTime)
                .buildOrThrow();
    }

    @JsonProperty
    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public long getFileSize()
    {
        return fileSize;
    }

    @JsonProperty
    public long getFileModifiedTime()
    {
        return fileModifiedTime;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(location)
                + splitWeight.getRetainedSizeInBytes()
                + predicate.getRetainedSizeInBytes(HiveColumnHandle::getRetainedSizeInBytes)
                + estimatedSizeOf(partitionKeys, HivePartitionKey::getEstimatedSizeInBytes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(location)
                .addValue(start)
                .addValue(length)
                .addValue(fileSize)
                .addValue(fileModifiedTime)
                .toString();
    }
}
