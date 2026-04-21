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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record IcebergSplit(
        @JsonProperty("path") String path,
        @JsonProperty("start") long start,
        @JsonProperty("length") long length,
        @JsonProperty("fileSize") long fileSize,
        @JsonProperty("fileRecordCount") long fileRecordCount,
        @JsonProperty("fileFormat") IcebergFileFormat fileFormat,
        @JsonProperty("specId") int specId,
        @JsonProperty("partitionValues") List<Block> partitionValues,
        @JsonProperty("deletes") List<DeleteFile> deletes,
        @JsonProperty("splitWeight") SplitWeight splitWeight,
        @JsonProperty("fileStatisticsDomain") TupleDomain<IcebergColumnHandle> fileStatisticsDomain,
        @JsonProperty("affinityKey") Optional<String> affinityKey,
        @JsonProperty("dataSequenceNumber") long dataSequenceNumber,
        @JsonProperty("fileFirstRowId") OptionalLong fileFirstRowId)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(IcebergSplit.class);

    public IcebergSplit
    {
        requireNonNull(path, "path is null");
        requireNonNull(fileFormat, "fileFormat is null");
        partitionValues = ImmutableList.copyOf(partitionValues);
        deletes = ImmutableList.copyOf(requireNonNull(deletes, "deletes is null"));
        requireNonNull(splitWeight, "splitWeight is null");
        requireNonNull(fileStatisticsDomain, "fileStatisticsDomain is null");
        requireNonNull(affinityKey, "affinityKey is null");
        requireNonNull(fileFirstRowId, "fileFirstRowId is null");
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
                + SIZE_OF_LONG * 4 // start, length, fileSize, fileRecordCount
                + SIZE_OF_INT // specId
                + estimatedSizeOf(partitionValues, Block::getRetainedSizeInBytes)
                + estimatedSizeOf(deletes, DeleteFile::retainedSizeInBytes)
                + splitWeight.getRetainedSizeInBytes()
                + fileStatisticsDomain.getRetainedSizeInBytes(IcebergColumnHandle::getRetainedSizeInBytes)
                + SIZE_OF_LONG // dataSequenceNumber
                + sizeOf(affinityKey, SizeOf::estimatedSizeOf)
                + (fileFirstRowId.isPresent() ? SIZE_OF_LONG : 0);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper(this)
                .addValue(path)
                .add("start", start)
                .add("length", length)
                .add("fileStatisticsDomain", fileStatisticsDomain);
        if (!deletes.isEmpty()) {
            helper.add("deleteFiles", deletes.size());
            helper.add("deleteRecords", deletes.stream()
                    .mapToLong(DeleteFile::recordCount).sum());
        }
        return helper.toString();
    }
}
