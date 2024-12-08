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
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DeltaLakeCacheSplitId
{
    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    // DeltaLakePageSourceProvider.createPageSource uses fileRowCount field. This field is mutable and affects split result even though data files in DeltaLake are immutable
    private final Optional<Long> fileRowCount;
    private final long fileModifiedTime;
    private final Map<String, Optional<String>> partitionKeys;
    private final Optional<DeletionVectorEntry> deletionVector;

    public DeltaLakeCacheSplitId(
            String path,
            long start,
            long length,
            long fileSize,
            Optional<Long> fileRowCount,
            long fileModifiedTime,
            Map<String, Optional<String>> partitionKeys,
            Optional<DeletionVectorEntry> deletionVector)
    {
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.fileRowCount = requireNonNull(fileRowCount, "rowCount is null");
        this.fileModifiedTime = fileModifiedTime;
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
        this.deletionVector = requireNonNull(deletionVector, "deletionVector is null");
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
    public Map<String, Optional<String>> getPartitionKeys()
    {
        return partitionKeys;
    }

    @JsonProperty
    public Optional<DeletionVectorEntry> getDeletionVector()
    {
        return deletionVector;
    }
}
