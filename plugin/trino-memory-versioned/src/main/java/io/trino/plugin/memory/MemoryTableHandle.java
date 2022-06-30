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
package io.trino.plugin.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class MemoryTableHandle
        implements ConnectorTableHandle
{
    private final long id;
    private final Optional<Set<Long>> versions;
    private final Optional<Long> updateVersion;

    @JsonCreator
    public MemoryTableHandle(
            @JsonProperty("id") long id,
            @JsonProperty("versions") Optional<Set<Long>> versions,
            @JsonProperty("updateVersions") Optional<Long> updateVersion)
    {
        this.id = id;
        this.versions = requireNonNull(versions, "versions is null");
        this.updateVersion = requireNonNull(updateVersion, "updateVersion is null");
    }

    @JsonProperty
    public long getId()
    {
        return id;
    }

    @JsonProperty
    public Optional<Set<Long>> getVersions()
    {
        return versions;
    }

    @JsonProperty
    public Optional<Long> getUpdateVersion()
    {
        return updateVersion;
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
        MemoryTableHandle that = (MemoryTableHandle) o;
        return id == that.id && versions.equals(that.versions) && updateVersion.equals(that.updateVersion);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, versions);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("versions", versions)
                .add("updateVersion", updateVersion)
                .toString();
    }
}
