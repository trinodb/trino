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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public final class MemoryTableHandle
        implements ConnectorTableHandle
{
    private final long id;
    private final OptionalLong limit;
    private final OptionalDouble sampleRatio;
    private final TupleDomain<ColumnHandle> fullFilter;
    private final TupleDomain<ColumnHandle> extraFilter;
    private final boolean exhaustedMicroPlanning;

    public MemoryTableHandle(long id)
    {
        this(id, OptionalLong.empty(), OptionalDouble.empty(), TupleDomain.all(), TupleDomain.all(), false);
    }

    @JsonCreator
    public MemoryTableHandle(
            @JsonProperty("id") long id,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("sampleRatio") OptionalDouble sampleRatio,
            @JsonProperty("fullFilter") TupleDomain<ColumnHandle> fullFilter,
            @JsonProperty("extraFilter") TupleDomain<ColumnHandle> extraFilter,
            @JsonProperty("exhaustedMicroPlanning") boolean exhaustedMicroPlanning)
    {
        this.id = id;
        this.limit = requireNonNull(limit, "limit is null");
        this.sampleRatio = requireNonNull(sampleRatio, "sampleRatio is null");
        this.fullFilter = requireNonNull(fullFilter, "fullFilter is null");
        this.extraFilter = requireNonNull(extraFilter, "extraFilter is null");
        this.exhaustedMicroPlanning = exhaustedMicroPlanning;
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

    @JsonProperty
    public OptionalDouble getSampleRatio()
    {
        return sampleRatio;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getFullFilter()
    {
        return fullFilter;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getExtraFilter()
    {
        return extraFilter;
    }

    public boolean isExhaustedMicroPlanning()
    {
        return exhaustedMicroPlanning;
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
                limit.equals(that.limit) &&
                sampleRatio.equals(that.sampleRatio) &&
                fullFilter.equals(that.fullFilter) &&
                extraFilter.equals(that.extraFilter) &&
                exhaustedMicroPlanning == that.exhaustedMicroPlanning;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, limit, sampleRatio, fullFilter, extraFilter, exhaustedMicroPlanning);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(id);
        limit.ifPresent(value -> builder.append("(limit:" + value + ")"));
        sampleRatio.ifPresent(value -> builder.append("(sampleRatio:" + value + ")"));
        return builder.toString();
    }
}
