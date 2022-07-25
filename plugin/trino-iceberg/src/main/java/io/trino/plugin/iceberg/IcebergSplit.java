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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.delete.TrinoDeleteFile;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

public class IcebergSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IcebergSplit.class).instanceSize();

    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final long fileRecordCount;
    private final IcebergFileFormat fileFormat;
    private final List<HostAddress> addresses;
    private final String partitionSpecJson;
    private final String partitionDataJson;
    private final Optional<String> schemaAsJson;
    private final List<TrinoDeleteFile> deletes;
    private final SplitWeight splitWeight;

    @JsonCreator
    public IcebergSplit(
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("fileRecordCount") long fileRecordCount,
            @JsonProperty("fileFormat") IcebergFileFormat fileFormat,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("partitionSpecJson") String partitionSpecJson,
            @JsonProperty("partitionDataJson") String partitionDataJson,
            @JsonProperty("schemaAsJson") Optional<String> schemaAsJson,
            @JsonProperty("deletes") List<TrinoDeleteFile> deletes,
            @JsonProperty("splitWeight") SplitWeight splitWeight)
    {
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.fileRecordCount = fileRecordCount;
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.partitionSpecJson = requireNonNull(partitionSpecJson, "partitionSpecJson is null");
        this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
        this.schemaAsJson = requireNonNull(schemaAsJson, "schemaAsJson is null");
        this.deletes = ImmutableList.copyOf(requireNonNull(deletes, "deletes is null"));
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
    public Optional<String> getSchemaAsJson()
    {
        return schemaAsJson;
    }

    @JsonProperty
    public String getPartitionDataJson()
    {
        return partitionDataJson;
    }

    @JsonProperty
    public List<TrinoDeleteFile> getDeletes()
    {
        return deletes;
    }

    @JsonProperty
    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
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
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes)
                + estimatedSizeOf(partitionSpecJson)
                + estimatedSizeOf(partitionDataJson)
                + estimatedSizeOf(deletes, TrinoDeleteFile::getRetainedSizeInBytes)
                + splitWeight.getRetainedSizeInBytes();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .addValue(start)
                .addValue(length)
                .toString();
    }
}
