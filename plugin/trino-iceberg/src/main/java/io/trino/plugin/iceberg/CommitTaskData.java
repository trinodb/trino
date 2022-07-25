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
import org.apache.iceberg.FileContent;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CommitTaskData
{
    private final String path;
    private final IcebergFileFormat fileFormat;
    private final long fileSizeInBytes;
    private final MetricsWrapper metrics;
    private final String partitionSpecJson;
    private final Optional<String> partitionDataJson;
    private final FileContent content;
    private final Optional<String> referencedDataFile;
    private final Optional<Long> fileRecordCount;
    private final Optional<Long> deletedRowCount;

    @JsonCreator
    public CommitTaskData(
            @JsonProperty("path") String path,
            @JsonProperty("fileFormat") IcebergFileFormat fileFormat,
            @JsonProperty("fileSizeInBytes") long fileSizeInBytes,
            @JsonProperty("metrics") MetricsWrapper metrics,
            @JsonProperty("partitionSpecJson") String partitionSpecJson,
            @JsonProperty("partitionDataJson") Optional<String> partitionDataJson,
            @JsonProperty("content") FileContent content,
            @JsonProperty("referencedDataFile") Optional<String> referencedDataFile,
            @JsonProperty("fileRecordCount") Optional<Long> fileRecordCount,
            @JsonProperty("deletedRowCount") Optional<Long> deletedRowCount)
    {
        this.path = requireNonNull(path, "path is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.fileSizeInBytes = fileSizeInBytes;
        this.metrics = requireNonNull(metrics, "metrics is null");
        this.partitionSpecJson = requireNonNull(partitionSpecJson, "partitionSpecJson is null");
        this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
        this.content = requireNonNull(content, "content is null");
        this.referencedDataFile = requireNonNull(referencedDataFile, "referencedDataFile is null");
        this.fileRecordCount = requireNonNull(fileRecordCount, "fileRecordCount is null");
        fileRecordCount.ifPresent(rowCount -> checkArgument(rowCount >= 0, "fileRecordCount cannot be negative"));
        this.deletedRowCount = requireNonNull(deletedRowCount, "deletedRowCount is null");
        deletedRowCount.ifPresent(rowCount -> checkArgument(rowCount >= 0, "deletedRowCount cannot be negative"));
        checkArgument(fileRecordCount.isPresent() == deletedRowCount.isPresent(), "fileRecordCount and deletedRowCount must be specified together");
        checkArgument(fileSizeInBytes >= 0, "fileSizeInBytes is negative");
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public IcebergFileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonProperty
    public long getFileSizeInBytes()
    {
        return fileSizeInBytes;
    }

    @JsonProperty
    public MetricsWrapper getMetrics()
    {
        return metrics;
    }

    @JsonProperty
    public String getPartitionSpecJson()
    {
        return partitionSpecJson;
    }

    @JsonProperty
    public Optional<String> getPartitionDataJson()
    {
        return partitionDataJson;
    }

    @JsonProperty
    public FileContent getContent()
    {
        return content;
    }

    @JsonProperty
    public Optional<String> getReferencedDataFile()
    {
        return referencedDataFile;
    }

    @JsonProperty
    public Optional<Long> getFileRecordCount()
    {
        return fileRecordCount;
    }

    @JsonProperty
    public Optional<Long> getDeletedRowCount()
    {
        return deletedRowCount;
    }
}
