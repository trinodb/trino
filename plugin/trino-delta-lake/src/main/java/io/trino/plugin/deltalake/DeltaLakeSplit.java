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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.LONG_INSTANCE_SIZE;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class DeltaLakeSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(DeltaLakeSplit.class);

    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final Optional<Long> fileRowCount;
    private final long fileModifiedTime;
    private final Optional<DeletionVectorEntry> deletionVector;
    private final List<HostAddress> addresses;
    private final SplitWeight splitWeight;
    private final TupleDomain<DeltaLakeColumnHandle> statisticsPredicate;
    private final Map<String, Optional<String>> partitionKeys;

    @JsonCreator
    public DeltaLakeSplit(
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("rowCount") Optional<Long> fileRowCount,
            @JsonProperty("fileModifiedTime") long fileModifiedTime,
            @JsonProperty("deletionVector") Optional<DeletionVectorEntry> deletionVector,
            @JsonProperty("splitWeight") SplitWeight splitWeight,
            @JsonProperty("statisticsPredicate") TupleDomain<DeltaLakeColumnHandle> statisticsPredicate,
            @JsonProperty("partitionKeys") Map<String, Optional<String>> partitionKeys)
    {
        this(
                path,
                start,
                length,
                fileSize,
                fileRowCount,
                fileModifiedTime,
                deletionVector,
                ImmutableList.of(),
                splitWeight,
                statisticsPredicate,
                partitionKeys);
    }

    public DeltaLakeSplit(
            String path,
            long start,
            long length,
            long fileSize,
            Optional<Long> fileRowCount,
            long fileModifiedTime,
            Optional<DeletionVectorEntry> deletionVector,
            List<HostAddress> addresses,
            SplitWeight splitWeight,
            TupleDomain<DeltaLakeColumnHandle> statisticsPredicate,
            Map<String, Optional<String>> partitionKeys)
    {
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.fileRowCount = requireNonNull(fileRowCount, "rowCount is null");
        this.fileModifiedTime = fileModifiedTime;
        this.deletionVector = requireNonNull(deletionVector, "deletionVector is null");
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
        this.statisticsPredicate = requireNonNull(statisticsPredicate, "statisticsPredicate is null");
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
    }

    // do not serialize addresses as they are not needed on workers
    @JsonIgnore
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
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
    public Optional<Long> getFileRowCount()
    {
        return fileRowCount;
    }

    @JsonProperty
    public long getFileModifiedTime()
    {
        return fileModifiedTime;
    }

    @JsonProperty
    public Optional<DeletionVectorEntry> getDeletionVector()
    {
        return deletionVector;
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
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + sizeOf(fileRowCount, value -> LONG_INSTANCE_SIZE)
                + sizeOf(deletionVector, DeletionVectorEntry::sizeInBytes)
                + splitWeight.getRetainedSizeInBytes()
                + statisticsPredicate.getRetainedSizeInBytes(DeltaLakeColumnHandle::retainedSizeInBytes)
                + estimatedSizeOf(partitionKeys, SizeOf::estimatedSizeOf, value -> sizeOf(value, SizeOf::estimatedSizeOf));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("path", path)
                .add("start", start)
                .add("length", length)
                .add("fileSize", fileSize)
                .add("rowCount", fileRowCount)
                .add("fileModifiedTime", fileModifiedTime)
                .add("deletionVector", deletionVector)
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
                fileModifiedTime == that.fileModifiedTime &&
                path.equals(that.path) &&
                fileRowCount.equals(that.fileRowCount) &&
                deletionVector.equals(that.deletionVector) &&
                Objects.equals(addresses, that.addresses) &&
                Objects.equals(statisticsPredicate, that.statisticsPredicate) &&
                Objects.equals(partitionKeys, that.partitionKeys);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, start, length, fileSize, fileRowCount, fileModifiedTime, deletionVector, addresses, statisticsPredicate, partitionKeys);
    }
}
