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
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collections;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class MemoryTableHandle
        implements ConnectorTableHandle
{
    private final long id;
    private final OptionalLong limit;
    private final OptionalDouble sampleRatio;
    private final TupleDomain<MemoryColumnHandle> predicate;  // only for testing complex pushdowns, not used for filtering
    private final Set<ConnectorExpression> connectorExpressions;  // only for testing complex pushdowns, not used for filtering

    public MemoryTableHandle(long id)
    {
        this(id, OptionalLong.empty(), OptionalDouble.empty());
    }

    @JsonCreator
    public MemoryTableHandle(
            @JsonProperty("id") long id,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("sampleRatio") OptionalDouble sampleRatio)
    {
        this(id, limit, sampleRatio, TupleDomain.all(), Collections.emptySet());
    }

    public MemoryTableHandle(
            long id,
            OptionalLong limit,
            OptionalDouble sampleRatio,
            TupleDomain<MemoryColumnHandle> predicate,
            Set<ConnectorExpression> connectorExpressions)
    {
        this.id = id;
        this.limit = requireNonNull(limit, "limit is null");
        this.sampleRatio = requireNonNull(sampleRatio, "sampleRatio is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.connectorExpressions = requireNonNull(connectorExpressions, "connectorExpressions is null");
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

    public TupleDomain<MemoryColumnHandle> getPredicate()
    {
        return predicate;
    }

    public Set<ConnectorExpression> getConnectorExpressions()
    {
        return connectorExpressions;
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
                sampleRatio.equals(that.sampleRatio);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, limit, sampleRatio);
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
