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
package io.trino.sql.analyzer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.trino.spi.connector.CatalogVersion;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public final class Output
{
    private final String catalogName;
    private final CatalogVersion catalogVersion;
    private final String schema;
    private final String table;
    private final Optional<List<OutputColumn>> columns;

    @JsonCreator
    public Output(
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("catalogVersion") CatalogVersion catalogVersion,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("columns") Optional<List<OutputColumn>> columns)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.catalogVersion = requireNonNull(catalogVersion, "catalogVersion is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = columns.map(ImmutableList::copyOf);
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public CatalogVersion getCatalogVersion()
    {
        return catalogVersion;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<List<OutputColumn>> getColumns()
    {
        return columns;
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
        Output output = (Output) o;
        return Objects.equals(catalogName, output.catalogName) &&
                Objects.equals(catalogVersion, output.catalogVersion) &&
                Objects.equals(schema, output.schema) &&
                Objects.equals(table, output.table) &&
                Objects.equals(columns, output.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, catalogVersion, schema, table, columns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("catalogVersion", catalogVersion)
                .add("schema", schema)
                .add("table", table)
                .add("columns", columns)
                .toString();
    }
}
