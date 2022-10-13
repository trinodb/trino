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
import static java.util.Objects.requireNonNull;

public class HudiSplit
        implements ConnectorSplit
{
    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final long fileModifiedTime;
    private final List<HostAddress> addresses;
    private final TupleDomain<HiveColumnHandle> predicate;
    private final List<HivePartitionKey> partitionKeys;
    private final SplitWeight splitWeight;

    @JsonCreator
    public HudiSplit(
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("fileModifiedTime") long fileModifiedTime,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("predicate") TupleDomain<HiveColumnHandle> predicate,
            @JsonProperty("partitionKeys") List<HivePartitionKey> partitionKeys,
            @JsonProperty("splitWeight") SplitWeight splitWeight)
    {
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkArgument(start + length <= fileSize, "fileSize must be at least start + length");

        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.fileModifiedTime = fileModifiedTime;
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.partitionKeys = ImmutableList.copyOf(requireNonNull(partitionKeys, "partitionKeys is null"));
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("path", path)
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
    public String getPath()
    {
        return path;
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
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .addValue(start)
                .addValue(length)
                .addValue(fileSize)
                .addValue(fileModifiedTime)
                .toString();
    }
}
