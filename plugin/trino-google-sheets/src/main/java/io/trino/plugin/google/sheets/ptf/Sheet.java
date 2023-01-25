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
package io.trino.plugin.google.sheets.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.google.sheets.SheetsClient;
import io.trino.plugin.google.sheets.SheetsColumnHandle;
import io.trino.plugin.google.sheets.SheetsConnectorTableHandle;
import io.trino.plugin.google.sheets.SheetsMetadata;
import io.trino.plugin.google.sheets.SheetsSheetTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.ptf.AbstractConnectorTableFunction;
import io.trino.spi.ptf.Argument;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;
import io.trino.spi.ptf.Descriptor;
import io.trino.spi.ptf.ScalarArgument;
import io.trino.spi.ptf.ScalarArgumentSpecification;
import io.trino.spi.ptf.TableFunctionAnalysis;

import javax.inject.Provider;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.google.sheets.SheetsClient.DEFAULT_RANGE;
import static io.trino.plugin.google.sheets.SheetsClient.RANGE_SEPARATOR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class Sheet
        implements Provider<ConnectorTableFunction>
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "sheet";
    public static final String ID_ARGUMENT = "ID";
    public static final String RANGE_ARGUMENT = "RANGE";

    private final SheetsMetadata metadata;

    @Inject
    public Sheet(SheetsClient client)
    {
        this.metadata = new SheetsMetadata(requireNonNull(client, "client is null"));
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new SheetFunction(metadata);
    }

    public static class SheetFunction
            extends AbstractConnectorTableFunction
    {
        private final SheetsMetadata metadata;

        public SheetFunction(SheetsMetadata metadata)
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name(ID_ARGUMENT)
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(RANGE_ARGUMENT)
                                    .type(VARCHAR)
                                    .defaultValue(utf8Slice(DEFAULT_RANGE))
                                    .build()),
                    GENERIC_TABLE);
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            String sheetId = ((Slice) ((ScalarArgument) arguments.get(ID_ARGUMENT)).getValue()).toStringUtf8();
            validateSheetId(sheetId);
            String rangeArgument = ((Slice) ((ScalarArgument) arguments.get(RANGE_ARGUMENT)).getValue()).toStringUtf8();

            SheetsConnectorTableHandle tableHandle = new SheetsSheetTableHandle(sheetId, rangeArgument);
            SheetFunctionHandle handle = new SheetFunctionHandle(tableHandle);

            List<Descriptor.Field> fields = metadata.getColumnHandles(session, tableHandle).entrySet().stream()
                    .map(entry -> new Descriptor.Field(
                            entry.getKey(),
                            Optional.of(((SheetsColumnHandle) (entry.getValue())).getColumnType())))
                    .collect(toImmutableList());

            Descriptor returnedType = new Descriptor(fields);

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    private static void validateSheetId(String sheetId)
    {
        // Sheet ids cannot contain "#", if a "#" is present it is because a range is present
        // Ranges should be provided through the range argument
        // https://developers.google.com/sheets/api/guides/concepts
        if (sheetId.contains(RANGE_SEPARATOR)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Google sheet ID %s cannot contain '#'. Provide a range through the 'range' argument.".formatted(sheetId));
        }
    }

    public static class SheetFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final SheetsConnectorTableHandle tableHandle;

        @JsonCreator
        public SheetFunctionHandle(@JsonProperty("tableHandle") SheetsConnectorTableHandle tableHandle)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        }

        @JsonProperty
        public ConnectorTableHandle getTableHandle()
        {
            return tableHandle;
        }
    }
}
