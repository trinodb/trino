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
package io.trino.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Unstable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
public class TableInfo
{
    private final String catalog;
    private final String schema;
    private final String table;
    private final String authorization;

    private final List<String> filters;
    private final List<ColumnInfo> columns;
    private final boolean directlyReferenced;
    private final Optional<String> viewText;
    private final List<TableReferenceInfo> referenceChain;

    @JsonCreator
    @Unstable
    public TableInfo(
            String catalog,
            String schema,
            String table,
            String authorization,
            List<String> filters,
            List<ColumnInfo> columns,
            boolean directlyReferenced,
            Optional<String> viewText,
            List<TableReferenceInfo> referenceChain)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.authorization = requireNonNull(authorization, "authorization is null");
        this.filters = List.copyOf(requireNonNull(filters, "filters is null"));
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
        this.directlyReferenced = directlyReferenced;
        this.viewText = requireNonNull(viewText, "viewText is null");
        this.referenceChain = List.copyOf(requireNonNull(referenceChain, "referenceChain is null"));
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

    @JsonProperty
    public boolean isDirectlyReferenced()
    {
        return directlyReferenced;
    }

    /**
     * If this object refers to a view or materialized view, return the SQL text of the view.
     *
     * @return The SQL text of the view, or empty if this object does not refer to a view.
     */
    @JsonProperty
    public Optional<String> getViewText()
    {
        return viewText;
    }

    /**
     * Get the reference chain for this table. Contains entries for view-like expressions such as views, column masks, and row filters.
     * Note that in contrast to {@link #getFilters()} and {@link #getColumns()}, this has information about the reference chain leading
     * this table to be included in the query, rather than information about which filters/masks were applied to this table scan.
     * <p>
     * Let us assume the following setup, with default catalog "default" and default schema "db":
     * <pre>
     * CREATE TABLE t_base (a INT, b INT);
     * CREATE VIEW v1 AS SELECT * FROM t_base;
     * CREATE VIEW v2 AS SELECT * FROM v1;
     *
     * Row filter for t_base:      a NOT IN (SELECT * FROM t_filter)
     * Column mask for t_base.b:   IF(b IN (SELECT * FROM t_mask), b, NULL)
     * </pre>
     * If we execute {@code SELECT * FROM v2}, we will see the following values for {@code referenceChain}:
     * <pre>
     * v2 -> []
     * v1 -> [{"view", "default", "db", "v2"}]
     * t_base -> [{"view", "default", "db", "v2"}, {"view", "default", "db", "v1"}]
     * t_filter -> [{"view", "default", "db", "v2"}, {"view", "default", "db", "v1"}, {"rowFilter", "a NOT IN (SELECT * FROM t_filter)", "default", "db", "t_base"}]
     * t_mask -> [{"view", "default", "db", "v2"}, {"view", "default", "db", "v1"}, {"columnMask", "IF(b IN SELECT * FROM t_mask, b, NULL)", "default", "db", "t_base"}]
     * </pre>
     *
     * @return The reference chain leading to this table, in order.
     */
    @JsonProperty
    public List<TableReferenceInfo> getReferenceChain()
    {
        return referenceChain;
    }
}
