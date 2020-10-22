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
package io.prestosql.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TableInfo
{
    private final String catalog;
    private final String schema;
    private final String table;
    private final String authorization;

    private final List<String> filters;
    private final List<ColumnInfo> columns;

    public TableInfo(String catalog, String schema, String table, String authorization, List<String> filters, List<ColumnInfo> columns)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.authorization = requireNonNull(authorization, "authorization is null");
        this.filters = List.copyOf(requireNonNull(filters, "filters is null"));
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
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
    public String getAuthorization()
    {
        return authorization;
    }

    @JsonProperty
    public List<String> getFilters()
    {
        return filters;
    }

    @JsonProperty
    public List<ColumnInfo> getColumns()
    {
        return columns;
    }
}
