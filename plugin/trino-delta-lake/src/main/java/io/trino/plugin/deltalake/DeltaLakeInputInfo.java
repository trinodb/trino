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

import java.util.Objects;

public class DeltaLakeInputInfo
{
    private final boolean partitioned;
    private final long version;

    @JsonCreator
    public DeltaLakeInputInfo(@JsonProperty("partitioned") boolean partitioned, @JsonProperty("version") long version)
    {
        this.partitioned = partitioned;
        this.version = version;
    }

    @JsonProperty
    public boolean isPartitioned()
    {
        return partitioned;
    }

    @JsonProperty
    public long getVersion()
    {
        return version;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DeltaLakeInputInfo that)) {
            return false;
        }
        return partitioned == that.partitioned && version == that.version;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioned, version);
    }
}
