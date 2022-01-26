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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DeltaLakeSplit
        implements ConnectorSplit
{
    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final long fileModifiedTime;
    private final List<HostAddress> addresses;
    private final TupleDomain<DeltaLakeColumnHandle> statisticsPredicate;
    private final Map<String, Optional<String>> partitionKeys;

    @JsonCreator
    public DeltaLakeSplit(
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("fileModifiedTime") long fileModifiedTime,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("statisticsPredicate") TupleDomain<DeltaLakeColumnHandle> statisticsPredicate,
            @JsonProperty("partitionKeys") Map<String, Optional<String>> partitionKeys)
    {
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.fileModifiedTime = fileModifiedTime;
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.statisticsPredicate = requireNonNull(statisticsPredicate, "statisticsPredicate is null");
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
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

    /**
     * A TupleDomain representing the min/max statistics from the file this split was generated from. This does not contain any partitioning information.
     */
    @JsonProperty
    public TupleDomain<DeltaLakeColumnHandle> getStatisticsPredicate()
    {
        return statisticsPredicate;
    }

    @JsonProperty
    public Map<String, Optional<String>> getPartitionKeys()
    {
        return partitionKeys;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("path", path)
                .put("start", start)
                .put("length", length)
                .buildOrThrow();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("path", path)
                .add("start", start)
                .add("length", length)
                .add("fileSize", fileSize)
                .add("addresses", addresses)
                .add("statisticsPredicate", statisticsPredicate)
                .add("partitionKeys", partitionKeys)
                .toString();
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
        DeltaLakeSplit that = (DeltaLakeSplit) o;
        return start == that.start &&
                length == that.length &&
                fileSize == that.fileSize &&
                path.equals(that.path) &&
                addresses.equals(that.addresses) &&
                Objects.equals(statisticsPredicate, that.statisticsPredicate) &&
                Objects.equals(partitionKeys, that.partitionKeys);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, start, length, fileSize, addresses, statisticsPredicate, partitionKeys);
    }
}
