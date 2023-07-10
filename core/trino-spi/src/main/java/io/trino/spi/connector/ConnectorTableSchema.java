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

import io.trino.spi.Experimental;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ConnectorTableSchema
{
    private final SchemaTableName table;
    private final List<ColumnSchema> columns;
    private final List<String> checkConstraints;

    public ConnectorTableSchema(SchemaTableName table, List<ColumnSchema> columns)
    {
        this(table, columns, List.of());
    }

    @Experimental(eta = "2023-03-31")
    public ConnectorTableSchema(SchemaTableName table, List<ColumnSchema> columns, List<String> checkConstraints)
    {
        requireNonNull(table, "table is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(checkConstraints, "checkConstraints is null");

        this.table = table;
        this.columns = List.copyOf(columns);
        this.checkConstraints = List.copyOf(checkConstraints);
    }

    public SchemaTableName getTable()
    {
        return table;
    }

    public List<ColumnSchema> getColumns()
    {
        return columns;
    }

    /**
     * List of constraints data in a table is expected to satisfy.
     * Engine ensures rows written to a table meet these constraints.
     * A check constraint is satisfied when it evaluates to True or Unknown.
     *
     * @return List of string representation of a Trino SQL scalar expression that can refer to table columns by name and produces a result coercible to boolean
     */
    @Experimental(eta = "2023-03-31")
    public List<String> getCheckConstraints()
    {
        return checkConstraints;
    }

    @Override
    public String toString()
    {
        return new StringBuilder("ConnectorTableSchema{")
                .append("table=").append(table)
                .append(", columns=").append(columns)
                .append(", checkConstraints=").append(checkConstraints)
                .append('}')
                .toString();
    }
}
