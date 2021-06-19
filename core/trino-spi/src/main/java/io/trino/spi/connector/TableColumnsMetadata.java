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

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TableColumnsMetadata
{
    private final SchemaTableName table;
    private final Optional<List<ColumnMetadata>> columns;

    public TableColumnsMetadata(SchemaTableName table, Optional<List<ColumnMetadata>> columns)
    {
        this.table = requireNonNull(table, "table is null");
        this.columns = requireNonNull(columns, "columns is null");
    }

    public static TableColumnsMetadata forTable(SchemaTableName table, List<ColumnMetadata> columns)
    {
        return new TableColumnsMetadata(table, Optional.of(requireNonNull(columns, "columns is null")));
    }

    public static TableColumnsMetadata forRedirectedTable(SchemaTableName table)
    {
        return new TableColumnsMetadata(table, Optional.empty());
    }

    public SchemaTableName getTable()
    {
        return table;
    }

    /**
     * @return Optional.empty() value means the table is redirected
     */
    public Optional<List<ColumnMetadata>> getColumns()
    {
        return columns;
    }
}
