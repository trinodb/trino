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
package io.trino.plugin.iceberg.functions.tablefiles;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TableFilesFunction
        extends AbstractConnectorTableFunction
{
    private static final String FUNCTION_NAME = "table_files";
    private static final String SCHEMA_NAME_VAR_NAME = "SCHEMA_NAME";
    private static final String TABLE_NAME_VAR_NAME = "TABLE_NAME";

    static final Type CONTENT_TYPE = INTEGER;
    static final Type FILE_PATH_TYPE = VARCHAR;
    static final Type FILE_FORMAT_TYPE = VARCHAR;
    static final Type SPEC_ID_TYPE = INTEGER;
    static final Type PARTITION_TYPE = VARCHAR;
    static final Type RECORD_COUNT_TYPE = BIGINT;
    static final Type FILE_SIZE_IN_BYTES_TYPE = BIGINT;

    private static final ReturnTypeSpecification RETURN_TYPE_SPECIFICATION = new ReturnTypeSpecification.DescribedTable(new Descriptor(ImmutableList.of(
            new Descriptor.Field("content", Optional.of(CONTENT_TYPE)),
            new Descriptor.Field("file_path", Optional.of(FILE_PATH_TYPE)),
            new Descriptor.Field("file_format", Optional.of(FILE_FORMAT_TYPE)),
            new Descriptor.Field("spec_id", Optional.of(SPEC_ID_TYPE)),
            new Descriptor.Field("partition", Optional.of(PARTITION_TYPE)),
            new Descriptor.Field("record_count", Optional.of(RECORD_COUNT_TYPE)),
            new Descriptor.Field("file_size_in_bytes", Optional.of(FILE_SIZE_IN_BYTES_TYPE)))));

    public TableFilesFunction()
    {
        super(
                "system",
                FUNCTION_NAME,
                ImmutableList.of(
                        ScalarArgumentSpecification.builder()
                                .name(SCHEMA_NAME_VAR_NAME)
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(TABLE_NAME_VAR_NAME)
                                .type(VARCHAR)
                                .build()),
                RETURN_TYPE_SPECIFICATION);
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
    {
        String schema = getSchemaName(arguments);
        String table = getTableName(arguments);

        return TableFunctionAnalysis.builder()
                .handle(new TableFilesFunctionHandle(new SchemaTableName(schema, table)))
                .build();
    }

    private static String getSchemaName(Map<String, Argument> arguments)
    {
        if (argumentExists(arguments, SCHEMA_NAME_VAR_NAME)) {
            return ((Slice) checkNonNull(((ScalarArgument) arguments.get(SCHEMA_NAME_VAR_NAME)).getValue())).toStringUtf8();
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, SCHEMA_NAME_VAR_NAME + " argument not found");
    }

    private static String getTableName(Map<String, Argument> arguments)
    {
        if (argumentExists(arguments, TABLE_NAME_VAR_NAME)) {
            return ((Slice) checkNonNull(((ScalarArgument) arguments.get(TABLE_NAME_VAR_NAME)).getValue())).toStringUtf8();
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, TABLE_NAME_VAR_NAME + " argument not found");
    }

    private static boolean argumentExists(Map<String, Argument> arguments, String key)
    {
        Argument argument = arguments.get(key);
        if (argument instanceof ScalarArgument scalarArgument) {
            return !scalarArgument.getNullableValue().isNull();
        }
        throw new IllegalArgumentException("Unsupported argument type: " + argument);
    }

    private static Object checkNonNull(Object argumentValue)
    {
        if (argumentValue == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, FUNCTION_NAME + " arguments may not be null");
        }
        return argumentValue;
    }
}
