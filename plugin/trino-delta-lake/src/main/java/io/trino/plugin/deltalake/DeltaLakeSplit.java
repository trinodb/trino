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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.LONG_INSTANCE_SIZE;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record DeltaLakeSplit(
        @JsonProperty("path") String path,
        @JsonProperty("start") long start,
        @JsonProperty("length") long length,
        @JsonProperty("fileSize") long fileSize,
        @JsonProperty("rowCount") Optional<Long> fileRowCount,
        @JsonProperty("fileModifiedTime") long fileModifiedTime,
        @JsonProperty("deletionVector") Optional<DeletionVectorEntry> deletionVector,
        @JsonProperty("affinityKey") Optional<String> affinityKey,
        @JsonProperty("splitWeight") SplitWeight splitWeight,
        @JsonProperty("statisticsPredicate") TupleDomain<DeltaLakeColumnHandle> statisticsPredicate,
        @JsonProperty("partitionKeys") Map<String, Optional<String>> partitionKeys)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(DeltaLakeSplit.class);

    public DeltaLakeSplit
    {
        requireNonNull(path, "path is null");
        requireNonNull(fileRowCount, "rowCount is null");
        requireNonNull(deletionVector, "deletionVector is null");
        requireNonNull(affinityKey, "affinityKey is null");
        requireNonNull(splitWeight, "splitWeight is null");
        requireNonNull(statisticsPredicate, "statisticsPredicate is null");
        requireNonNull(partitionKeys, "partitionKeys is null");
    }

    @Override
    public Optional<String> getAffinityKey()
    {
        return affinityKey;
    }

    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + sizeOf(fileRowCount, value -> LONG_INSTANCE_SIZE)
                + sizeOf(deletionVector, DeletionVectorEntry::sizeInBytes)
                + sizeOf(affinityKey, SizeOf::estimatedSizeOf)
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
                .add("statisticsPredicate", statisticsPredicate)
                .add("partitionKeys", partitionKeys)
                .toString();
    }
}
