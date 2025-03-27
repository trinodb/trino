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
package io.trino.plugin.storage;

import io.trino.plugin.storage.functions.ListFilesTableFunction.ListFilesFunctionHandle;
import io.trino.plugin.storage.functions.ListFilesTableFunction.ListFilesTableHandle;
import io.trino.plugin.storage.functions.ReadCsvTableFunction.ReadCsvFunctionHandle;
import io.trino.plugin.storage.functions.ReadCsvTableFunction.ReadCsvTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.storage.functions.ListFilesTableFunction.LIST_FILES_COLUMNS_METADATA;
import static io.trino.plugin.storage.functions.ListFilesTableFunction.LIST_FILES_COLUMN_HANDLES;

public class StorageMetadata
        implements ConnectorMetadata
{
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (tableHandle instanceof ListFilesTableHandle) {
            // TODO (https://github.com/trinodb/trino/issues/6694) SchemaTableName should not be required for synthetic ConnectorTableHandle
            return new ConnectorTableMetadata(new SchemaTableName("_generated", "_generated_list_files"), LIST_FILES_COLUMNS_METADATA);
        }
        if (tableHandle instanceof ReadCsvTableHandle readCsv) {
            return new ConnectorTableMetadata(
                    // TODO (https://github.com/trinodb/trino/issues/6694) SchemaTableName should not be required for synthetic ConnectorTableHandle
                    new SchemaTableName("_generated", "_generated_read_sv"),
                    readCsv.columns().stream()
                            .map(column -> new ColumnMetadata(column.name(), column.type()))
                            .collect(toImmutableList()));
        }
        throw new IllegalArgumentException("Unsupported table handle: " + tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        StorageColumnHandle column = (StorageColumnHandle) columnHandle;
        return new ColumnMetadata(column.name(), column.type());
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (handle instanceof ListFilesFunctionHandle listFiles) {
            return Optional.of(new TableFunctionApplicationResult<>(
                    listFiles.tableHandle(),
                    LIST_FILES_COLUMN_HANDLES.stream()
                            .map(column -> new StorageColumnHandle(column.name(), column.type()))
                            .collect(toImmutableList())));
        }
        if (handle instanceof ReadCsvFunctionHandle readCsv) {
            return Optional.of(new TableFunctionApplicationResult<>(
                    readCsv.tableHandle(),
                    readCsv.columns().stream()
                            .map(column -> new StorageColumnHandle(column.name(), column.type()))
                            .collect(toImmutableList())));
        }
        return Optional.empty();
    }
}
