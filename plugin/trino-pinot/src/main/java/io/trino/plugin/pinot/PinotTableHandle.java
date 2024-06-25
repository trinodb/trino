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
package io.trino.plugin.pinot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.pinot.query.DynamicTable;
import io.trino.plugin.pinot.query.ptf.SerializablePinotQuery;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PinotTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final boolean enableNullHandling;
    private final TupleDomain<ColumnHandle> constraint;
    private final OptionalLong limit;
    private final Optional<DynamicTable> query;
    private final Optional<SerializablePinotQuery> pinotQuery;

    public PinotTableHandle(String schemaName, String tableName)
    {
        this(schemaName, tableName, false, TupleDomain.all(), OptionalLong.empty(), Optional.empty(), Optional.empty());
    }

    public PinotTableHandle(String schemaName, String tableName, boolean enableNullHandling)
    {
        this(schemaName, tableName, enableNullHandling, TupleDomain.all(), OptionalLong.empty(), Optional.empty(), Optional.empty());
    }

    @JsonCreator
    public PinotTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("enableNullHandling") boolean enableNullHandling,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("query") Optional<DynamicTable> query,
            @JsonProperty("pinotQuery") Optional<SerializablePinotQuery> pinotQuery)

    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.enableNullHandling = enableNullHandling;
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.query = requireNonNull(query, "query is null");
        this.pinotQuery = requireNonNull(pinotQuery, "pinotQuery is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public boolean isEnableNullHandling()
    {
        return enableNullHandling;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public Optional<DynamicTable> getQuery()
    {
        return query;
    }

    @JsonProperty
    public Optional<SerializablePinotQuery> getPinotQuery()
    {
        return pinotQuery;
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
        PinotTableHandle that = (PinotTableHandle) o;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("enableNullHandling", enableNullHandling)
                .add("constraint", constraint)
                .add("limit", limit)
                .add("query", query)
                .toString();
    }
}
