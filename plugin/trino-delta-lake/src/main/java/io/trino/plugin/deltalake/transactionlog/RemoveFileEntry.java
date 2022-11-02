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

import java.util.Objects;

import static java.lang.String.format;

public class RemoveFileEntry
{
    private final String path;
    private final long deletionTimestamp;
    private final boolean dataChange;

    @JsonCreator
    public RemoveFileEntry(
            @JsonProperty("path") String path,
            @JsonProperty("deletionTimestamp") long deletionTimestamp,
            @JsonProperty("dataChange") boolean dataChange)
    {
        this.path = path;
        this.deletionTimestamp = deletionTimestamp;
        this.dataChange = dataChange;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
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
        return path.equals(that.path) &&
                deletionTimestamp == that.deletionTimestamp &&
                dataChange == that.dataChange;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, deletionTimestamp, dataChange);
    }

    @Override
    public String toString()
    {
        return format("RemoveFileEntry{path=%s, deletionTimestamp=%d, dataChange=%b}", path, deletionTimestamp, dataChange);
    }
}
