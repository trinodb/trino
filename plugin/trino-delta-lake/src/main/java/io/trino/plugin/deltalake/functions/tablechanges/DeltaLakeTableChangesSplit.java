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
package io.trino.plugin.deltalake.functions.tablechanges;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DeltaLakeTableChangesSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(DeltaLakeTableChangesSplit.class).instanceSize());

    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final long fileModifiedTime;
    private final List<HostAddress> addresses;
    private final SplitWeight splitWeight;
    private final TupleDomain<DeltaLakeColumnHandle> statisticsPredicate;
    private final Map<String, Optional<String>> partitionKeys;
    private final long commitTimestamp;
    private final FileSource source;
    private final long version;

    @JsonCreator
    public DeltaLakeTableChangesSplit(
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("fileModifiedTime") long fileModifiedTime,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("splitWeight") SplitWeight splitWeight,
            @JsonProperty("statisticsPredicate") TupleDomain<DeltaLakeColumnHandle> statisticsPredicate,
            @JsonProperty("partitionKeys") Map<String, Optional<String>> partitionKeys,
            @JsonProperty("commitTimestamp") long commitTimestamp,
            @JsonProperty("source") FileSource source,
            @JsonProperty("version") long version)
    {
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.fileModifiedTime = fileModifiedTime;
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
        this.statisticsPredicate = requireNonNull(statisticsPredicate, "statisticsPredicate is null");
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
        this.commitTimestamp = commitTimestamp;
        this.source = requireNonNull(source, "source is null");
        this.version = version;
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
        return null;
    }

    @JsonProperty
    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

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

    @JsonProperty
    public long getCommitTimestamp()
    {
        return commitTimestamp;
    }

    @JsonProperty
    public FileSource getSource()
    {
        return source;
    }

    @JsonProperty
    public long getVersion()
    {
        return version;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes)
                + splitWeight.getRetainedSizeInBytes()
                + statisticsPredicate.getRetainedSizeInBytes(DeltaLakeColumnHandle::getRetainedSizeInBytes)
                + estimatedSizeOf(partitionKeys, SizeOf::estimatedSizeOf, value -> sizeOf(value, SizeOf::estimatedSizeOf));
    }
}
