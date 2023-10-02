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
package io.trino.plugin.pinot.query.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.plugin.pinot.PinotMetadata;
import io.trino.plugin.pinot.PinotTableHandle;
import io.trino.plugin.pinot.PinotTypeConverter;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.query.DynamicTable;
import io.trino.plugin.pinot.query.DynamicTableBuilder;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class Query
        implements Provider<ConnectorTableFunction>
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "query";

    private final PinotMetadata metadata;
    private final PinotClient client;

    private final PinotTypeConverter pinotTypeConverter;

    @Inject
    public Query(PinotMetadata metadata, PinotClient client, PinotTypeConverter pinotTypeConverter)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.client = requireNonNull(client, "client is null");
        this.pinotTypeConverter = requireNonNull(pinotTypeConverter, "pinotTypeConverter is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new QueryFunction(metadata, client, pinotTypeConverter);
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        private final PinotMetadata metadata;
        private final PinotClient client;
        private final PinotTypeConverter pinotTypeConverter;

        public QueryFunction(PinotMetadata metadata, PinotClient client, PinotTypeConverter pinotTypeConverter)
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    List.of(ScalarArgumentSpecification.builder()
                            .name("QUERY")
                            .type(VARCHAR)
                            .build()),
                    GENERIC_TABLE);
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.client = requireNonNull(client, "client is null");
            this.pinotTypeConverter = requireNonNull(pinotTypeConverter, "pinotTypeConverter is null");
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl)
        {
            requireNonNull(arguments, "arguments is null");
            String query = ((Slice) ((ScalarArgument) arguments.get("QUERY")).getValue()).toStringUtf8();
            DynamicTable dynamicTable = DynamicTableBuilder.buildFromPql(metadata, query, client, pinotTypeConverter);
            Descriptor returnedType = new Descriptor(dynamicTable.getColumnHandlesForSelect()
                    .map(columnHandle -> new Descriptor.Field(columnHandle.getColumnName(), Optional.of(columnHandle.getDataType())))
                    .collect(toList()));
            PinotTableHandle tableHandle = new PinotTableHandle(SCHEMA_NAME, dynamicTable.getTableName(), TupleDomain.all(), OptionalLong.empty(), Optional.of(dynamicTable));
            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(new QueryFunctionHandle(tableHandle))
                    .build();
        }
    }

    public static class QueryFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final PinotTableHandle tableHandle;

        @JsonCreator
        public QueryFunctionHandle(@JsonProperty("tableHandle") PinotTableHandle tableHandle)
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
