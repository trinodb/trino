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

public class IcebergSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(IcebergSplit.class);

    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final long fileRecordCount;
    private final IcebergFileFormat fileFormat;
    private final int specId;
    private final List<Block> partitionValues;
    private final List<DeleteFile> deletes;
    private final SplitWeight splitWeight;
    private final TupleDomain<IcebergColumnHandle> fileStatisticsDomain;
    private final long dataSequenceNumber;
    private final OptionalLong fileFirstRowId;
    private final Optional<String> affinityKey;

    @JsonCreator
    public IcebergSplit(
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
    {
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.fileRecordCount = fileRecordCount;
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.specId = specId;
        this.partitionValues = ImmutableList.copyOf(partitionValues);
        this.deletes = ImmutableList.copyOf(requireNonNull(deletes, "deletes is null"));
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
        this.fileStatisticsDomain = requireNonNull(fileStatisticsDomain, "fileStatisticsDomain is null");
        this.affinityKey = requireNonNull(affinityKey, "affinityKey is null");
        this.dataSequenceNumber = dataSequenceNumber;
        this.fileFirstRowId = requireNonNull(fileFirstRowId, "fileFirstRowId is null");
    }

    @JsonProperty
    @Override
    public Optional<String> getAffinityKey()
    {
        return affinityKey;
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
    public long getFileRecordCount()
    {
        return fileRecordCount;
    }

    @JsonProperty
    public IcebergFileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonProperty
    public int getSpecId()
    {
        return specId;
    }

    @JsonProperty
    public List<Block> getPartitionValues()
    {
        return partitionValues;
    }

    @JsonProperty
    public List<DeleteFile> getDeletes()
    {
        return deletes;
    }

    @JsonProperty
    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

    @JsonProperty
    public TupleDomain<IcebergColumnHandle> getFileStatisticsDomain()
    {
        return fileStatisticsDomain;
    }

    @JsonProperty
    public long getDataSequenceNumber()
    {
        return dataSequenceNumber;
    }

    @JsonProperty
    public OptionalLong getFileFirstRowId()
    {
        return fileFirstRowId;
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
