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
package io.trino.plugin.neo4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.function.table.Descriptor;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Neo4jQueryRelationHandle
        extends Neo4jRelationHandle
{
    private final String query;
    private final Descriptor descriptor;
    private final Optional<String> databaseName;

    @JsonCreator
    public Neo4jQueryRelationHandle(
            @JsonProperty("query") String query,
            @JsonProperty("descriptor") Descriptor descriptor,
            @JsonProperty("databaseName") Optional<String> databaseName)
    {
        this.query = requireNonNull(query, "query is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public Descriptor getDescriptor()
    {
        return descriptor;
    }

    @JsonProperty
    @Override
    public Optional<String> getDatabaseName()
    {
        return this.databaseName;
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
        Neo4jQueryRelationHandle that = (Neo4jQueryRelationHandle) o;
        return Objects.equals(query, that.query) && Objects.equals(descriptor, that.descriptor);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(query, descriptor);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("query", query)
                .add("descriptor", descriptor)
                .toString();
    }
}
