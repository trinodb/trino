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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.plugin.storage.StorageColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
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
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class ListFilesTableFunction
        implements Provider<ConnectorTableFunction>
{
    private static final Map<String, Type> COLUMN_TYPES = ImmutableMap.of(
            "file_modified_time", TIMESTAMP_TZ_MILLIS,
            "size", BIGINT,
            "name", VARCHAR);
    public static final List<ColumnMetadata> LIST_FILES_COLUMNS_METADATA = COLUMN_TYPES.entrySet().stream()
            .map(column -> new ColumnMetadata(column.getKey(), column.getValue()))
            .collect(toImmutableList());
    public static final List<StorageColumnHandle> LIST_FILES_COLUMN_HANDLES = COLUMN_TYPES.entrySet().stream()
            .map(column -> new StorageColumnHandle(column.getKey(), column.getValue()))
            .collect(toImmutableList());

    @Override
    public ConnectorTableFunction get()
    {
        return new QueryFunction();
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        public QueryFunction()
        {
            super(
                    "system",
                    "list_files",
                    ImmutableList.<ArgumentSpecification>builder()
                            .add(ScalarArgumentSpecification.builder()
                                    .name("LOCATION")
                                    .type(VARCHAR)
                                    .build())
                            .build(),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
        {
            String location = ((Slice) ((ScalarArgument) arguments.get("LOCATION")).getValue()).toStringUtf8();

            Descriptor returnedType = new Descriptor(COLUMN_TYPES.entrySet().stream()
                    .map(column -> new Descriptor.Field(column.getKey(), Optional.of(column.getValue())))
                    .collect(toImmutableList()));

            ListFilesFunctionHandle handle = new ListFilesFunctionHandle(new ListFilesTableHandle(location));

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    public record ListFilesTableHandle(String location)
            implements ConnectorTableHandle
    {
        public ListFilesTableHandle
        {
            requireNonNull(location, "location is null");
        }
    }

    public record ListFilesFunctionHandle(ListFilesTableHandle tableHandle)
            implements ConnectorTableFunctionHandle
    {
        public ListFilesFunctionHandle
        {
            requireNonNull(tableHandle, "tableHandle is null");
        }
    }
}
