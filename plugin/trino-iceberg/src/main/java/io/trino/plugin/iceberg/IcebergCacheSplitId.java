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
import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.delete.DeleteFile;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class IcebergCacheSplitId
{
    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final String partitionSpecJson;
    private final String partitionDataJson;
    private final List<DeleteFile> deletes;

    public IcebergCacheSplitId(
            String path,
            long start,
            long length,
            long fileSize,
            String partitionSpecJson,
            String partitionDataJson,
            List<DeleteFile> deletes)
    {
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.partitionSpecJson = requireNonNull(partitionSpecJson, "partitionSpecJson is null");
        this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
        this.deletes = ImmutableList.copyOf(requireNonNull(deletes, "deletes is null"));
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
}
