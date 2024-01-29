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
package io.trino.plugin.deltalake.transactionlog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

public class RemoveFileEntry
{
    private final String path;
    private final Map<String, String> partitionValues;
    private final long deletionTimestamp;
    private final boolean dataChange;

    @JsonCreator
    public RemoveFileEntry(
            @JsonProperty("path") String path,
            @JsonProperty("partitionValues") @Nullable Map<String, String> partitionValues,
            @JsonProperty("deletionTimestamp") long deletionTimestamp,
            @JsonProperty("dataChange") boolean dataChange)
    {
        this.path = path;
        this.partitionValues = partitionValues;
        this.deletionTimestamp = deletionTimestamp;
        this.dataChange = dataChange;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @Nullable
    @JsonProperty
    public Map<String, String> getPartitionValues()
    {
        return partitionValues;
    }

    @JsonProperty
    public long getDeletionTimestamp()
    {
        return deletionTimestamp;
    }

    @JsonProperty
    public boolean isDataChange()
    {
        return dataChange;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoveFileEntry that = (RemoveFileEntry) o;
        return deletionTimestamp == that.deletionTimestamp &&
                dataChange == that.dataChange &&
                Objects.equals(path, that.path) &&
                Objects.equals(partitionValues, that.partitionValues);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, partitionValues, deletionTimestamp, dataChange);
    }

    @Override
    public String toString()
    {
        return "RemoveFileEntry{" +
                "path='" + path + '\'' +
                ", partitionValues=" + partitionValues +
                ", deletionTimestamp=" + deletionTimestamp +
                ", dataChange=" + dataChange +
                '}';
    }
}
