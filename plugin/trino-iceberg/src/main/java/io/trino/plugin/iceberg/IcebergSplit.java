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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
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
    private final String partitionSpecJson;
    private final String partitionDataJson;
    private final List<DeleteFile> deletes;
    private final SplitWeight splitWeight;
    private final TupleDomain<IcebergColumnHandle> fileStatisticsDomain;
    private final Map<String, String> fileIoProperties;
    private final List<HostAddress> addresses;

    @JsonCreator
    public IcebergSplit(
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("fileRecordCount") long fileRecordCount,
            @JsonProperty("fileFormat") IcebergFileFormat fileFormat,
            @JsonProperty("partitionSpecJson") String partitionSpecJson,
            @JsonProperty("partitionDataJson") String partitionDataJson,
            @JsonProperty("deletes") List<DeleteFile> deletes,
            @JsonProperty("splitWeight") SplitWeight splitWeight,
            @JsonProperty("fileStatisticsDomain") TupleDomain<IcebergColumnHandle> fileStatisticsDomain,
            @JsonProperty("fileIoProperties") Map<String, String> fileIoProperties)
    {
        this(
                path,
                start,
                length,
                fileSize,
                fileRecordCount,
                fileFormat,
                partitionSpecJson,
                partitionDataJson,
                deletes,
                splitWeight,
                fileStatisticsDomain,
                fileIoProperties,
                ImmutableList.of());
    }

    public IcebergSplit(
            String path,
            long start,
            long length,
            long fileSize,
            long fileRecordCount,
            IcebergFileFormat fileFormat,
            String partitionSpecJson,
            String partitionDataJson,
            List<DeleteFile> deletes,
            SplitWeight splitWeight,
            TupleDomain<IcebergColumnHandle> fileStatisticsDomain,
            Map<String, String> fileIoProperties,
            List<HostAddress> addresses)
    {
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.fileRecordCount = fileRecordCount;
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.partitionSpecJson = requireNonNull(partitionSpecJson, "partitionSpecJson is null");
        this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
        this.deletes = ImmutableList.copyOf(requireNonNull(deletes, "deletes is null"));
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
        this.fileStatisticsDomain = requireNonNull(fileStatisticsDomain, "fileStatisticsDomain is null");
        this.fileIoProperties = ImmutableMap.copyOf(requireNonNull(fileIoProperties, "fileIoProperties is null"));
        this.addresses = requireNonNull(addresses, "addresses is null");
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
    public String getPartitionSpecJson()
    {
        return partitionSpecJson;
    }

    @JsonProperty
    public String getPartitionDataJson()
    {
        return partitionDataJson;
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
    public Map<String, String> getFileIoProperties()
    {
        return fileIoProperties;
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return ImmutableMap.<String, String>builder()
                .put("path", path)
                .put("start", String.valueOf(start))
                .put("length", String.valueOf(length))
                .buildOrThrow();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + estimatedSizeOf(partitionSpecJson)
                + estimatedSizeOf(partitionDataJson)
                + estimatedSizeOf(deletes, DeleteFile::getRetainedSizeInBytes)
                + splitWeight.getRetainedSizeInBytes()
                + fileStatisticsDomain.getRetainedSizeInBytes(IcebergColumnHandle::getRetainedSizeInBytes)
                + estimatedSizeOf(fileIoProperties, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf)
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
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
