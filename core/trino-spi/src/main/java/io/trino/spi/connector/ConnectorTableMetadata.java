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
package io.trino.spi.connector;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

public class ConnectorTableMetadata
{
    private final SchemaTableName table;
    private final Optional<String> comment;
    private final List<ColumnMetadata> columns;
    private final Map<String, Object> properties;
    private final List<String> checkConstraints;
    private final List<String> primaryKeys;

    public ConnectorTableMetadata(SchemaTableName table, List<ColumnMetadata> columns)
    {
        this(table, columns, emptyMap());
    }

    public ConnectorTableMetadata(SchemaTableName table, List<ColumnMetadata> columns, Map<String, Object> properties)
    {
        this(table, columns, properties, Optional.empty(), List.of(), List.of());
    }

    public ConnectorTableMetadata(SchemaTableName table, List<ColumnMetadata> columns, Map<String, Object> properties, Optional<String> comment)
    {
        this(table, columns, properties, comment, List.of(), List.of());
    }

    public ConnectorTableMetadata(SchemaTableName table, List<ColumnMetadata> columns, Map<String, Object> properties, Optional<String> comment, List<String> checkConstraints)
    {
        this(table, columns, properties, comment, checkConstraints, List.of());
    }

    public ConnectorTableMetadata(SchemaTableName table, List<ColumnMetadata> columns, Map<String, Object> properties, Optional<String> comment, List<String> checkConstraints, List<String> primaryKeys)
    {
        requireNonNull(table, "table is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(comment, "comment is null");
        requireNonNull(checkConstraints, "checkConstraints is null");
        requireNonNull(primaryKeys, "primaryKeys is null");

        this.table = table;
        this.columns = List.copyOf(columns);
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
        this.comment = comment;
        this.checkConstraints = List.copyOf(checkConstraints);
        this.primaryKeys = List.copyOf(primaryKeys);

        Set<String> columnNames = new HashSet<>();
        for (ColumnMetadata column : this.columns) {
            columnNames.add(column.getName().toLowerCase(ENGLISH));
        }
        Set<String> uniquePrimaryKeys = new HashSet<>();
        for (String primaryKey : this.primaryKeys) {
            String canonicalPrimaryKey = primaryKey.toLowerCase(ENGLISH);
            if (!columnNames.contains(canonicalPrimaryKey)) {
                throw new IllegalArgumentException("Primary key '%s' is not a column of table %s".formatted(primaryKey, table));
            }
            if (!uniquePrimaryKeys.add(canonicalPrimaryKey)) {
                throw new IllegalArgumentException("Primary key '%s' is listed more than once for table %s".formatted(primaryKey, table));
            }
        }
    }

    public SchemaTableName getTable()
    {
        return table;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    public Map<String, Object> getProperties()
    {
        return properties;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    /**
     * List of constraints data in a table is expected to satisfy.
     * Engine ensures rows written to a table meet these constraints.
     * A check constraint is satisfied when it evaluates to True or Unknown.
     *
     * @return List of string representation of a Trino SQL scalar expression that can refer to table columns by name and produces a result coercible to boolean
     */
    public List<String> getCheckConstraints()
    {
        return checkConstraints;
    }

    /**
     * Columns forming the declared primary key of the table, in key order, or an empty list when no primary key is declared.
     * The primary key is a declaration only; neither the engine nor the connector is required to enforce it.
     * The engine may use it as a hint for optimizations that do not affect correctness, such as table statistics estimates.
     */
    public List<String> getPrimaryKeys()
    {
        return primaryKeys;
    }

    public ConnectorTableSchema getTableSchema()
    {
        return new ConnectorTableSchema(
                table,
                columns.stream()
                        .map(ColumnMetadata::getColumnSchema)
                        .collect(toUnmodifiableList()),
                checkConstraints);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ConnectorTableMetadata{");
        sb.append("table=").append(table);
        sb.append(", columns=").append(columns);
        sb.append(", properties=").append(properties);
        comment.ifPresent(value -> sb.append(", comment='").append(value).append("'"));
        sb.append(", checkConstraints=").append(checkConstraints);
        sb.append(", primaryKeys=").append(primaryKeys);
        sb.append('}');
        return sb.toString();
    }
}
