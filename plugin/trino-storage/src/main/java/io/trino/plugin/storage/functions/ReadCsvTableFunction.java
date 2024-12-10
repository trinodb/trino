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
package io.trino.plugin.storage.functions;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.storage.StorageColumnHandle;
import io.trino.plugin.storage.reader.CsvFileReader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
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
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.Functions.checkFunctionArgument;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class ReadCsvTableFunction
        implements Provider<ConnectorTableFunction>
{
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public ReadCsvTableFunction(TrinoFileSystemFactory fileSystemFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new QueryFunction(fileSystemFactory);
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        private final TrinoFileSystemFactory fileSystemFactory;

        public QueryFunction(TrinoFileSystemFactory fileSystemFactory)
        {
            super(
                    "system",
                    "read_csv",
                    ImmutableList.<ArgumentSpecification>builder()
                            .add(ScalarArgumentSpecification.builder()
                                    .name("LOCATION")
                                    .type(VARCHAR)
                                    .build())
                            .add(ScalarArgumentSpecification.builder()
                                    .name("SEPARATOR")
                                    .type(VARCHAR)
                                    .defaultValue(utf8Slice(","))
                                    .build())
                            .build(),
                    GENERIC_TABLE);
            this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
        {
            String location = ((Slice) ((ScalarArgument) arguments.get("LOCATION")).getValue()).toStringUtf8();
            String separator = ((Slice) ((ScalarArgument) arguments.get("SEPARATOR")).getValue()).toStringUtf8();
            checkFunctionArgument(separator.length() == 1, "Separator must be a single character");

            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            CsvFileReader fileReader = new CsvFileReader(separator.charAt(0));
            List<StorageColumnHandle> columns = fileReader.getColumns(location, path -> {
                try {
                    return fileSystem.newInputFile(Location.of(path)).newStream();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            Descriptor returnedType = new Descriptor(columns.stream()
                    .map(column -> new Descriptor.Field(column.name(), Optional.of(column.type())))
                    .collect(toImmutableList()));

            ReadCsvFunctionHandle handle = new ReadCsvFunctionHandle(new ReadCsvTableHandle(location, separator.charAt(0), columns), columns);

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    public record ReadCsvTableHandle(String location, char separator, List<StorageColumnHandle> columns)
            implements ConnectorTableHandle
    {
        public ReadCsvTableHandle
        {
            requireNonNull(location, "location is null");
            columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        }
    }

    public record ReadCsvFunctionHandle(ReadCsvTableHandle tableHandle, List<StorageColumnHandle> columns)
            implements ConnectorTableFunctionHandle
    {
        public ReadCsvFunctionHandle
        {
            requireNonNull(tableHandle, "tableHandle is null");
            columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        }
    }
}
