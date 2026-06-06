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
package io.trino.plugin.hive.functions.unload;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableArgument;
import io.trino.spi.function.table.TableArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;

public class UnloadFunction
        extends AbstractConnectorTableFunction
{
    private static final String FUNCTION_NAME = "unload";
    private static final String INPUT_ARGUMENT = "INPUT";
    private static final String LOCATION_ARGUMENT = "LOCATION";
    private static final String FORMAT_ARGUMENT = "FORMAT";

    private final boolean unloadEnabled;

    public UnloadFunction(boolean unloadEnabled)
    {
        super(
                "system",
                FUNCTION_NAME,
                ImmutableList.of(
                        TableArgumentSpecification.builder()
                                .name(INPUT_ARGUMENT)
                                .rowSemantics()
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(LOCATION_ARGUMENT)
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(FORMAT_ARGUMENT)
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build()),
                GENERIC_TABLE);
        this.unloadEnabled = unloadEnabled;
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments,
            ConnectorAccessControl accessControl)
    {
        if (!unloadEnabled) {
            throw new TrinoException(PERMISSION_DENIED, "unload table function is disabled");
        }

        String location = getStringArgument(arguments, LOCATION_ARGUMENT);
        if (location.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "location must not be empty");
        }

        HiveStorageFormat storageFormat = getStorageFormat(arguments);

        TableArgument inputArgument = (TableArgument) arguments.get(INPUT_ARGUMENT);
        List<RowType.Field> inputSchema = inputArgument.getRowType().getFields();

        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        ImmutableList.Builder<Integer> requiredColumns = ImmutableList.builder();

        for (int i = 0; i < inputSchema.size(); i++) {
            RowType.Field field = inputSchema.get(i);
            columnNames.add(field.getName().orElse("_col" + i));
            columnTypes.add(field.getType());
            requiredColumns.add(i);
        }

        ImmutableList.Builder<Descriptor.Field> returnedColumns = ImmutableList.builder();
        returnedColumns.add(new Descriptor.Field("path", Optional.of(VARCHAR)));
        returnedColumns.add(new Descriptor.Field("rows_written", Optional.of(BIGINT)));
        returnedColumns.add(new Descriptor.Field("bytes_written", Optional.of(BIGINT)));

        return TableFunctionAnalysis.builder()
                .requiredColumns(INPUT_ARGUMENT, requiredColumns.build())
                .returnedType(new Descriptor(returnedColumns.build()))
                .handle(new UnloadFunctionHandle(
                        location,
                        storageFormat,
                        columnNames.build(),
                        columnTypes.build()))
                .build();
    }

    private static String getStringArgument(Map<String, Argument> arguments, String name)
    {
        Object value = ((ScalarArgument) arguments.get(name)).getValue();
        if (value == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, name + " must not be null");
        }
        return ((Slice) value).toStringUtf8();
    }

    private static HiveStorageFormat getStorageFormat(Map<String, Argument> arguments)
    {
        Object value = ((ScalarArgument) arguments.get(FORMAT_ARGUMENT)).getValue();
        if (value == null) {
            return HiveStorageFormat.PARQUET;
        }
        String formatName = ((Slice) value).toStringUtf8().toUpperCase(ENGLISH);
        try {
            HiveStorageFormat format = HiveStorageFormat.valueOf(formatName);
            return switch (format) {
                case ORC, PARQUET, AVRO, CSV, JSON, OPENX_JSON, TEXTFILE -> format;
                case SEQUENCEFILE, SEQUENCEFILE_PROTOBUF, RCBINARY, RCTEXT, REGEX, ESRI ->
                        throw new TrinoException(INVALID_FUNCTION_ARGUMENT,
                                "Unsupported format for unload: " + formatName + ". Supported formats: ORC, PARQUET, AVRO, CSV, JSON, OPENX_JSON, TEXTFILE");
            };
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT,
                    "Unknown format: " + formatName + ". Supported formats: ORC, PARQUET, AVRO, CSV, JSON, OPENX_JSON, TEXTFILE");
        }
    }
}
