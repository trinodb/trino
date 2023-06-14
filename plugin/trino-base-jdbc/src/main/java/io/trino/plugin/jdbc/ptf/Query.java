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
package io.trino.plugin.jdbc.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcMetadata;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTransactionManager;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.Descriptor.Field;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class Query
        implements Provider<ConnectorTableFunction>
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "query";

    private final JdbcTransactionManager transactionManager;

    @Inject
    public Query(JdbcTransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new QueryFunction(transactionManager), getClass().getClassLoader());
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        private final JdbcTransactionManager transactionManager;

        public QueryFunction(JdbcTransactionManager transactionManager)
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    List.of(ScalarArgumentSpecification.builder()
                            .name("QUERY")
                            .type(VARCHAR)
                            .build()),
                    GENERIC_TABLE);
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl)
        {
            ScalarArgument argument = (ScalarArgument) getOnlyElement(arguments.values());
            String query = ((Slice) argument.getValue()).toStringUtf8();
            PreparedQuery preparedQuery = new PreparedQuery(query, ImmutableList.of());

            JdbcMetadata metadata = transactionManager.getMetadata(transaction);
            JdbcTableHandle tableHandle = metadata.getTableHandle(session, preparedQuery);
            List<JdbcColumnHandle> columns = tableHandle.getColumns().orElseThrow(() -> new IllegalStateException("Handle doesn't have columns info"));
            Descriptor returnedType = new Descriptor(columns.stream()
                    .map(column -> new Field(column.getColumnName(), Optional.of(column.getColumnType())))
                    .collect(toImmutableList()));

            QueryFunctionHandle handle = new QueryFunctionHandle(tableHandle);

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    public static class QueryFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final JdbcTableHandle tableHandle;

        @JsonCreator
        public QueryFunctionHandle(@JsonProperty("tableHandle") JdbcTableHandle tableHandle)
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
