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
package io.trino.plugin.bigquery.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.bigquery.Schema;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.bigquery.BigQueryClient;
import io.trino.plugin.bigquery.BigQueryClientFactory;
import io.trino.plugin.bigquery.BigQueryColumnHandle;
import io.trino.plugin.bigquery.BigQueryQueryRelationHandle;
import io.trino.plugin.bigquery.BigQueryTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.ptf.AbstractConnectorTableFunction;
import io.trino.spi.ptf.Argument;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;
import io.trino.spi.ptf.Descriptor;
import io.trino.spi.ptf.Descriptor.Field;
import io.trino.spi.ptf.ScalarArgument;
import io.trino.spi.ptf.ScalarArgumentSpecification;
import io.trino.spi.ptf.TableFunctionAnalysis;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.bigquery.Conversions.isSupportedType;
import static io.trino.plugin.bigquery.Conversions.toColumnHandle;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class Query
        implements Provider<ConnectorTableFunction>
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "query";

    private final BigQueryClientFactory clientFactory;

    @Inject
    public Query(BigQueryClientFactory clientFactory)
    {
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new QueryFunction(clientFactory), getClass().getClassLoader());
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        private final BigQueryClientFactory clientFactory;

        public QueryFunction(BigQueryClientFactory clientFactory)
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    List.of(ScalarArgumentSpecification.builder()
                            .name("QUERY")
                            .type(VARCHAR)
                            .build()),
                    GENERIC_TABLE);
            this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            ScalarArgument argument = (ScalarArgument) getOnlyElement(arguments.values());
            String query = ((Slice) argument.getValue()).toStringUtf8();

            BigQueryClient client = clientFactory.create(session);
            Schema schema = client.getSchema(query);

            BigQueryQueryRelationHandle queryRelationHandle = new BigQueryQueryRelationHandle(query);
            BigQueryTableHandle tableHandle = new BigQueryTableHandle(queryRelationHandle);

            ImmutableList.Builder<BigQueryColumnHandle> columnsBuilder = ImmutableList.builderWithExpectedSize(schema.getFields().size());
            for (com.google.cloud.bigquery.Field field : schema.getFields()) {
                if (!isSupportedType(field)) {
                    throw new UnsupportedOperationException("Unsupported type: " + field.getType());
                }
                columnsBuilder.add(toColumnHandle(field));
            }

            Descriptor returnedType = new Descriptor(columnsBuilder.build().stream()
                    .map(column -> new Field(column.getName(), Optional.of(column.getTrinoType())))
                    .collect(toList()));

            QueryHandle handle = new QueryHandle(tableHandle.withProjectedColumns(columnsBuilder.build()));

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    public static class QueryHandle
            implements ConnectorTableFunctionHandle
    {
        private final BigQueryTableHandle tableHandle;

        @JsonCreator
        public QueryHandle(@JsonProperty("tableHandle") BigQueryTableHandle tableHandle)
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
