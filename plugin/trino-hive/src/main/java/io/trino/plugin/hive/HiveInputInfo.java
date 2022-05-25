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
package io.trino.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HiveInputInfo
{
    private final List<String> partitionIds;
    private final boolean partitioned;
    private final Optional<String> tableDefaultFileFormat;

    @JsonCreator
    public HiveInputInfo(
            @JsonProperty("partitionIds") List<String> partitionIds,
            @JsonProperty("partitioned") boolean partitioned,
            @JsonProperty("tableDefaultFileFormat") Optional<String> tableDefaultFileFormat)
    {
        this.partitionIds = requireNonNull(partitionIds, "partitionIds is null");
        this.partitioned = partitioned;
        this.tableDefaultFileFormat = requireNonNull(tableDefaultFileFormat, "tableDefaultFileFormat is null");
    }

    @JsonProperty
    public List<String> getPartitionIds()
    {
        return partitionIds;
    }

    @JsonProperty
    public boolean isPartitioned()
    {
        return partitioned;
    }

    @JsonProperty
    public Optional<String> getTableDefaultFileFormat()
    {
        return tableDefaultFileFormat;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HiveInputInfo)) {
            return false;
        }
        HiveInputInfo that = (HiveInputInfo) o;
        return partitionIds.equals(that.partitionIds)
                && partitioned == that.partitioned
                && tableDefaultFileFormat.equals(that.tableDefaultFileFormat);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionIds, partitioned, tableDefaultFileFormat);
    }
}
