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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DataFileInfo
{
    public enum DataFileType
    {
        DATA,
        CHANGE_DATA_FEED,
    }

    private final String path;
    private final List<String> partitionValues;
    private final long size;
    private final DataFileType dataFileType;
    private final long creationTime;
    private final DeltaLakeJsonFileStatistics statistics;

    @JsonCreator
    public DataFileInfo(
            @JsonProperty("path") String path,
            @JsonProperty("size") long size,
            @JsonProperty("creationTime") long creationTime,
            @JsonProperty("fileType") DataFileType dataFileType,
            @JsonProperty("partitionValues") List<String> partitionValues,
            @JsonProperty("statistics") DeltaLakeJsonFileStatistics statistics)
    {
        this.path = path;
        this.size = size;
        this.creationTime = creationTime;
        this.dataFileType = requireNonNull(dataFileType, "dataFileType is null");
        this.partitionValues = partitionValues;
        this.statistics = requireNonNull(statistics, "statistics is null");
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public List<String> getPartitionValues()
    {
        return partitionValues;
    }

    @JsonProperty
    public long getSize()
    {
        return size;
    }

    @JsonProperty
    public long getCreationTime()
    {
        return creationTime;
    }

    @JsonProperty
    public DataFileType getDataFileType()
    {
        return dataFileType;
    }

    @JsonProperty
    public DeltaLakeJsonFileStatistics getStatistics()
    {
        return statistics;
    }
}
