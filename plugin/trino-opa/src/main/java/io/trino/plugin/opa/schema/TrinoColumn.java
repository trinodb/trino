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
package io.trino.plugin.opa.schema;

import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

/**
 * This class is used to represent information about a column for the purposes of column masking.
 * It is (perhaps counterintuitively) only used for column masking and not for operations like
 * FilterColumns. This is for 3 reasons:
 * - API stability between the batch and non-batch modes: sending an array of TrinoColumn objects would be wasteful for
 *   the batch authorizer mode (as it would repeat the catalog, schema and table names once per column). As such, this
 *   object is not used for FilterColumns even if batch mode is disabled
 * - This object contains in-depth information about the column (e.g. its type), and it may be modified to include
 *   additional fields in the future. This level of information is not provided to operations like FilterColumns
 * - Backwards compatibility
 *
 * @param catalogName The name of the catalog this column's table belongs to
 * @param schemaName The name of the schema this column's table belongs to
 * @param tableName The name of the table this column is in
 * @param columnName Column name
 * @param columnType String representation of the column type
 */
public record TrinoColumn(
        String catalogName,
        String schemaName,
        String tableName,
        String columnName,
        String columnType)
{
    public TrinoColumn
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnName, "columnName is null");
        requireNonNull(columnType, "columnType is null");
    }

    public TrinoColumn(CatalogSchemaTableName tableName, String columnName, Type type)
    {
        this(tableName.getCatalogName(),
                tableName.getSchemaTableName().getSchemaName(),
                tableName.getSchemaTableName().getTableName(),
                columnName,
                type.getDisplayName());
    }
}
