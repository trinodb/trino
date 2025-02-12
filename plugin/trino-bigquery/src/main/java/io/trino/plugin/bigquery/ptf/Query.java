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
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.bigquery.BigQueryClient;
import io.trino.plugin.bigquery.BigQueryClientFactory;
import io.trino.plugin.bigquery.BigQueryColumnHandle;
import io.trino.plugin.bigquery.BigQueryQueryRelationHandle;
import io.trino.plugin.bigquery.BigQueryTableHandle;
import io.trino.plugin.bigquery.BigQueryTypeManager;
import io.trino.plugin.bigquery.RemoteTableName;
import io.trino.spi.TrinoException;
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
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.bigquery.ViewMaterializationCache.TEMP_TABLE_PREFIX;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;

public class Query
        implements Provider<ConnectorTableFunction>
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "query";

    private final BigQueryClientFactory clientFactory;
    private final BigQueryTypeManager typeManager;

    @Inject
    public Query(BigQueryClientFactory clientFactory, BigQueryTypeManager typeManager)
    {
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new QueryFunction(clientFactory, typeManager), getClass().getClassLoader());
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        private final BigQueryClientFactory clientFactory;
        private final BigQueryTypeManager typeManager;

        public QueryFunction(BigQueryClientFactory clientFactory, BigQueryTypeManager typeManager)
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
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
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

            BigQueryClient client = clientFactory.create(session);
            TableId destinationTable = buildDestinationTable(client.getDestinationTable(query));
            boolean useStorageApi = client.useStorageApi(query, destinationTable);
            Schema schema = client.getSchema(query);

            BigQueryQueryRelationHandle queryRelationHandle = new BigQueryQueryRelationHandle(query, new RemoteTableName(destinationTable), useStorageApi);
            BigQueryTableHandle tableHandle = new BigQueryTableHandle(queryRelationHandle, TupleDomain.all(), Optional.empty(), OptionalLong.empty());

            ImmutableList.Builder<BigQueryColumnHandle> columnsBuilder = ImmutableList.builderWithExpectedSize(schema.getFields().size());
            for (com.google.cloud.bigquery.Field field : schema.getFields()) {
                if (!typeManager.isSupportedType(field, useStorageApi)) {
                    // TODO: Skip unsupported type instead of throwing an exception
                    throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + field.getType());
                }
                columnsBuilder.add(typeManager.toColumnHandle(field, useStorageApi));
            }

            Descriptor returnedType = new Descriptor(columnsBuilder.build().stream()
                    .map(column -> new Field(column.name(), Optional.of(column.trinoType())))
                    .collect(toList()));

            QueryHandle handle = new QueryHandle(tableHandle.withProjectedColumns(columnsBuilder.build()));

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    private static TableId buildDestinationTable(TableId remoteTableId)
    {
        String project = remoteTableId.getProject();
        String dataset = remoteTableId.getDataset();

        String name = format("%s%s", TEMP_TABLE_PREFIX, randomUUID().toString().toLowerCase(ENGLISH).replace("-", ""));
        return TableId.of(project, dataset, name);
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
