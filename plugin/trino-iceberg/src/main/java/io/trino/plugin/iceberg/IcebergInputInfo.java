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

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IcebergInputInfo
{
    private final Optional<Long> snapshotId;
    private final Optional<Boolean> partitioned;
    private final String tableDefaultFileFormat;

    @JsonCreator
    public IcebergInputInfo(
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("partitioned") Optional<Boolean> partitioned,
            @JsonProperty("fileFormat") String tableDefaultFileFormat)
    {
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.partitioned = requireNonNull(partitioned, "partitioned is null");
        this.tableDefaultFileFormat = requireNonNull(tableDefaultFileFormat, "tableDefaultFileFormat is null");
    }

    @JsonProperty
    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    public Optional<Boolean> getPartitioned()
    {
        return partitioned;
    }

    @JsonProperty
    public String getTableDefaultFileFormat()
    {
        return tableDefaultFileFormat;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergInputInfo that)) {
            return false;
        }
        return partitioned.equals(that.partitioned)
                && snapshotId.equals(that.snapshotId)
                && tableDefaultFileFormat.equals(that.tableDefaultFileFormat);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(snapshotId, partitioned, tableDefaultFileFormat);
    }
}
