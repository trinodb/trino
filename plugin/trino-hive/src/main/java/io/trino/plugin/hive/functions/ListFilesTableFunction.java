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
package io.trino.plugin.hive.functions;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ArgumentSpecification;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.metastore.HiveType.HIVE_TIMESTAMP;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public final class ListFilesTableFunction
        implements Provider<ConnectorTableFunction>
{
    public static final List<ColumnHandle> LIST_FILES_COLUMNS = ImmutableList.<ColumnHandle>builder()
            .add(createBaseColumn("file_modified_time", 1, HIVE_TIMESTAMP, TIMESTAMP_TZ_MILLIS, REGULAR, Optional.empty()))
            .add(createBaseColumn("size", 2, HIVE_LONG, BIGINT, REGULAR, Optional.empty()))
            .add(createBaseColumn("location", 3, HIVE_STRING, VARCHAR, REGULAR, Optional.empty()))
            .build();

    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public ListFilesTableFunction(TrinoFileSystemFactory fileSystemFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ListFilesFunction(fileSystemFactory);
    }

    public static class ListFilesFunction
            extends AbstractConnectorTableFunction
    {
        private final TrinoFileSystemFactory fileSystemFactory;

        public ListFilesFunction(TrinoFileSystemFactory fileSystemFactory)
        {
            super(
                    "system",
                    "list_files",
                    ImmutableList.<ArgumentSpecification>builder()
                            .add(ScalarArgumentSpecification.builder()
                                    .name("PATH")
                                    .type(VARCHAR)
                                    .build())
                            .build(),
                    GENERIC_TABLE);
            this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl)
        {
            String pathArgument = ((Slice) ((ScalarArgument) arguments.get("PATH")).getValue()).toStringUtf8();
            Location path = Location.of(pathArgument);

            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            try {
                if (!fileSystem.directoryExists(path).orElse(false)) {
                    throw new TrinoException(NOT_FOUND, "Directory not found: " + path);
                }
            }
            catch (IOException e) {
                throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed checking directory path: " + path, e);
            }

            Descriptor returnedType = new Descriptor(LIST_FILES_COLUMNS.stream()
                    .map(column -> (HiveColumnHandle) column)
                    .map(column -> new Descriptor.Field(column.getName(), Optional.of(column.getType())))
                    .collect(toImmutableList()));

            ListFilesTableFunctionHandle handle = new ListFilesTableFunctionHandle(new HiveListFilesTableHandle(path.toString()));

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    public record ListFilesTableFunctionHandle(HiveListFilesTableHandle tableHandle)
            implements ConnectorTableFunctionHandle
    {
        public ListFilesTableFunctionHandle
        {
            requireNonNull(tableHandle, "tableHandle is null");
        }
    }
}
