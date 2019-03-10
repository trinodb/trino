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
package io.prestosql.plugin.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorTableHandle;

import java.util.Objects;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public final class MemoryTableHandle
        implements ConnectorTableHandle
{
    private final long id;
    private final OptionalLong limit;

    public MemoryTableHandle(long id)
    {
        this(id, OptionalLong.empty());
    }

    @JsonCreator
    public MemoryTableHandle(
            @JsonProperty("id") long id,
            @JsonProperty("limit") OptionalLong limit)
    {
        this.id = id;
        this.limit = requireNonNull(limit, "limit is null");
    }

    @JsonProperty
    public long getId()
    {
        return id;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
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
        return id == that.id &&
                limit.equals(that.limit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, limit);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(id);
        limit.ifPresent(value -> builder.append("(limit:" + value + ")"));
        return builder.toString();
    }
}
