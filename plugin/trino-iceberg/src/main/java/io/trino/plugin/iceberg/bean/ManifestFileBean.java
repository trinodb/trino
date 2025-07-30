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
package io.trino.plugin.iceberg.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;

import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;

public record ManifestFileBean(
        String path,
        long length,
        int partitionSpecId,
        ManifestContent content,
        long sequenceNumber,
        long minSequenceNumber,
        Long snapshotId,
        Integer addedFilesCount,
        Integer existingFilesCount,
        Integer deletedFilesCount,
        Long addedRowsCount,
        Long existingRowsCount,
        Long deletedRowsCount,
        @JsonDeserialize(contentAs = PartitionFieldSummaryBean.class) List<PartitionFieldSummary> partitions,
        ByteBuffer keyMetadata,
        Long firstRowId)
        implements ManifestFile
{
    public static final long INSTANCE_SIZE = instanceSize(ManifestFileBean.class);

    @JsonCreator
    public ManifestFileBean(
            @JsonProperty("path") String path,
            @JsonProperty("length") long length,
            @JsonProperty("partitionSpecId") int partitionSpecId,
            @JsonProperty("content") ManifestContent content,
            @JsonProperty("sequenceNumber") long sequenceNumber,
            @JsonProperty("minSequenceNumber") long minSequenceNumber,
            @JsonProperty("snapshotId") Long snapshotId,
            @JsonProperty("addedFilesCount") Integer addedFilesCount,
            @JsonProperty("existingFilesCount") Integer existingFilesCount,
            @JsonProperty("deletedFilesCount") Integer deletedFilesCount,
            @JsonProperty("addedRowsCount") Long addedRowsCount,
            @JsonProperty("existingRowsCount") Long existingRowsCount,
            @JsonProperty("deletedRowsCount") Long deletedRowsCount,
            @JsonProperty("partitions") List<PartitionFieldSummary> partitions,
            @JsonProperty("keyMetadata") ByteBuffer keyMetadata,
            @JsonProperty("firstRowId") Long firstRowId)
    {
        this.path = path;
        this.length = length;
        this.partitionSpecId = partitionSpecId;
        this.content = content;
        this.sequenceNumber = sequenceNumber;
        this.minSequenceNumber = minSequenceNumber;
        this.snapshotId = snapshotId;
        this.addedFilesCount = addedFilesCount;
        this.existingFilesCount = existingFilesCount;
        this.deletedFilesCount = deletedFilesCount;
        this.addedRowsCount = addedRowsCount;
        this.existingRowsCount = existingRowsCount;
        this.deletedRowsCount = deletedRowsCount;
        this.partitions = partitions;
        this.keyMetadata = keyMetadata;
        this.firstRowId = firstRowId;
    }

    @JsonProperty
    @Override
    public String path()
    {
        return path;
    }

    @JsonProperty
    @Override
    public long length()
    {
        return length;
    }

    @JsonProperty
    @Override
    public int partitionSpecId()
    {
        return partitionSpecId;
    }

    @JsonProperty
    @Override
    public ManifestContent content()
    {
        return content;
    }

    @JsonProperty
    @Override
    public long sequenceNumber()
    {
        return sequenceNumber;
    }

    @JsonProperty
    @Override
    public long minSequenceNumber()
    {
        return minSequenceNumber;
    }

    @JsonProperty
    @Override
    public Long snapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    @Override
    public Integer addedFilesCount()
    {
        return addedFilesCount;
    }

    @JsonProperty
    @Override
    public Long addedRowsCount()
    {
        return addedRowsCount;
    }

    @JsonProperty
    @Override
    public Integer existingFilesCount()
    {
        return existingFilesCount;
    }

    @JsonProperty
    @Override
    public Long existingRowsCount()
    {
        return existingRowsCount;
    }

    @JsonProperty
    @Override
    public Integer deletedFilesCount()
    {
        return deletedFilesCount;
    }

    @JsonProperty
    @Override
    public Long deletedRowsCount()
    {
        return deletedRowsCount;
    }

    @JsonProperty
    @Override
    public List<PartitionFieldSummary> partitions()
    {
        return partitions;
    }

    @JsonProperty
    @Override
    public ByteBuffer keyMetadata()
    {
        return keyMetadata;
    }

    @JsonProperty
    @Override
    public Long firstRowId()
    {
        return firstRowId;
    }

    @Override
    public ManifestFile copy()
    {
        throw new UnsupportedOperationException("Cannot copy");
    }

    public static ManifestFileBean from(ManifestFile manifestFile)
    {
        return new ManifestFileBean(
                manifestFile.path(),
                manifestFile.length(),
                manifestFile.partitionSpecId(),
                manifestFile.content(),
                manifestFile.sequenceNumber(),
                manifestFile.minSequenceNumber(),
                manifestFile.snapshotId(),
                manifestFile.addedFilesCount(),
                manifestFile.existingFilesCount(),
                manifestFile.deletedFilesCount(),
                manifestFile.addedRowsCount(),
                manifestFile.existingRowsCount(),
                manifestFile.deletedRowsCount(),
                manifestFile.partitions() != null ? manifestFile.partitions().stream().map(PartitionFieldSummaryBean::from).collect(toImmutableList()) : null,
                manifestFile.keyMetadata(),
                manifestFile.firstRowId());
    }
}
