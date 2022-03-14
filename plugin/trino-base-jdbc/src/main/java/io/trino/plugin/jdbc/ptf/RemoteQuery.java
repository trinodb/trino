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
import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcMetadata;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTransactionManager;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.ptf.Argument;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;
import io.trino.spi.ptf.Descriptor;
import io.trino.spi.ptf.Descriptor.Field;
import io.trino.spi.ptf.ScalarArgument;
import io.trino.spi.ptf.ScalarArgumentSpecification;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.ptf.DescriptorMapping.EMPTY_MAPPING;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RemoteQuery
        implements Provider<ConnectorTableFunction>
{
    public static final String NAME = "remote_query";

    private final JdbcTransactionManager transactionManager;

    @Inject
    public RemoteQuery(JdbcTransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        // TODO wrap in ClassLoaderSafeConnectorTableFunction? (see also TestClassLoaderSafeWrappers)
        return new RemoteQueryFunction(transactionManager);
    }

    public static class RemoteQueryFunction
            extends ConnectorTableFunction
    {
        public static final String SCHEMA_NAME = "system";

        private final JdbcTransactionManager transactionManager;

        public RemoteQueryFunction(JdbcTransactionManager transactionManager)
        {
            super(SCHEMA_NAME, NAME, List.of(new ScalarArgumentSpecification("query", VARCHAR)), GENERIC_TABLE);
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        }

        @Override
        public Analysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            ScalarArgument argument = (ScalarArgument) getOnlyElement(arguments.values());
            String query = ((Slice) argument.getValue()).toStringUtf8();
            PreparedQuery preparedQuery = new PreparedQuery(query, ImmutableList.of());

            JdbcMetadata metadata = transactionManager.getMetadata(transaction);
            JdbcTableHandle tableHandle = metadata.getTableHandle(session, preparedQuery);
            List<JdbcColumnHandle> columns = tableHandle.getColumns().orElseThrow(() -> new IllegalStateException("Query result has no columns"));
            Descriptor returnedType = new Descriptor(columns.stream()
                    .map(column -> new Field(column.getColumnName(), Optional.of(column.getColumnType())))
                    .collect(toList()));

            RemoteQueryHandle handle = new RemoteQueryHandle(tableHandle);

            return new Analysis(Optional.of(returnedType), EMPTY_MAPPING, handle);
        }
    }

    public static class RemoteQueryHandle
            implements ConnectorTableFunctionHandle
    {
        private final JdbcTableHandle tableHandle;

        @JsonCreator
        public RemoteQueryHandle(@JsonProperty("tableHandle") JdbcTableHandle tableHandle)
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
