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
package io.prestosql.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class ElasticsearchTableHandle
        implements ConnectorTableHandle
{
    private final String schema;
    private final String index;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<String> query;

    public ElasticsearchTableHandle(String schema, String index, Optional<String> query)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.index = requireNonNull(index, "index is null");
        this.query = requireNonNull(query, "query is null");

        constraint = TupleDomain.all();
    }

    @JsonCreator
    public ElasticsearchTableHandle(
            @JsonProperty("schema") String schema,
            @JsonProperty("index") String index,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("query") Optional<String> query)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.index = requireNonNull(index, "index is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.query = requireNonNull(query, "query is null");
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<String> getQuery()
    {
        return query;
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
        ElasticsearchTableHandle that = (ElasticsearchTableHandle) o;
        return schema.equals(that.schema) &&
                index.equals(that.index) &&
                constraint.equals(that.constraint) &&
                query.equals(that.query);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, index, constraint, query);
    }
}
