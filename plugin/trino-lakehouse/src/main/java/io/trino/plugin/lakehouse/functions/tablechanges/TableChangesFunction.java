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
package io.trino.plugin.lakehouse.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.deltalake.metastore.NotADeltaLakeTableException;
import io.trino.plugin.iceberg.UnknownTableTypeException;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TableChangesFunction
        extends AbstractConnectorTableFunction
{
    private static final String FUNCTION_NAME = "table_changes";
    private static final String SCHEMA_NAME = "system";
    private static final String NAME = "table_changes";
    public static final String SCHEMA_NAME_ARGUMENT = "SCHEMA_NAME";
    private static final String TABLE_NAME_ARGUMENT = "TABLE_NAME";
    private static final String START_SNAPSHOT_VAR_NAME = "START_SNAPSHOT_ID";
    private static final String END_SNAPSHOT_VAR_NAME = "END_SNAPSHOT_ID";
    private static final String SINCE_VERSION_ARGUMENT = "SINCE_VERSION";

    private final io.trino.plugin.deltalake.functions.tablechanges.TableChangesFunction deltaLakeTableChangesFunction;
    private final io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunction icebergTableChangesFunction;

    public TableChangesFunction(
            io.trino.plugin.deltalake.functions.tablechanges.TableChangesFunction deltaLakeTableChangesFunction,
            io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunction icebergTableChangesFunction)
    {
        super(
                SCHEMA_NAME,
                NAME,
                ImmutableList.of(
                        ScalarArgumentSpecification.builder().name(SCHEMA_NAME_ARGUMENT).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(TABLE_NAME_ARGUMENT).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(START_SNAPSHOT_VAR_NAME).type(BIGINT).defaultValue(null).build(),
                        ScalarArgumentSpecification.builder().name(END_SNAPSHOT_VAR_NAME).type(BIGINT).defaultValue(null).build(),
                        ScalarArgumentSpecification.builder().name(SINCE_VERSION_ARGUMENT).type(BIGINT).defaultValue(null).build()),
                GENERIC_TABLE);
        this.deltaLakeTableChangesFunction = deltaLakeTableChangesFunction;
        this.icebergTableChangesFunction = icebergTableChangesFunction;
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments,
            ConnectorAccessControl accessControl)
    {
        try {
            return deltaLakeTableChangesFunction.analyze(session, transaction, arguments, accessControl);
        }
        catch (NotADeltaLakeTableException _) {
            checkNonNull(arguments.get(START_SNAPSHOT_VAR_NAME), START_SNAPSHOT_VAR_NAME);
            checkNonNull(arguments.get(END_SNAPSHOT_VAR_NAME), END_SNAPSHOT_VAR_NAME);
            try {
                return icebergTableChangesFunction.analyze(session, transaction, arguments, accessControl);
            }
            catch (UnknownTableTypeException e) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "table_changes function is not supported for the given table type");
            }
        }
    }

    private void checkNonNull(Object argumentValue, String argumentName)
    {
        if (argumentValue == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, FUNCTION_NAME + " argument " + argumentName + " may not be null");
        }
    }
}
