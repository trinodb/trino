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
package io.trino.plugin.elasticsearch.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.plugin.elasticsearch.ElasticsearchColumnHandle;
import io.trino.plugin.elasticsearch.ElasticsearchMetadata;
import io.trino.plugin.elasticsearch.ElasticsearchTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.QUERY;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RawQuery
        implements Provider<ConnectorTableFunction>
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "raw_query";

    private final ElasticsearchMetadata metadata;

    @Inject
    public RawQuery(ElasticsearchMetadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new RawQueryFunction(metadata);
    }

    public static class RawQueryFunction
            extends AbstractConnectorTableFunction
    {
        private final ElasticsearchMetadata metadata;

        public RawQueryFunction(ElasticsearchMetadata metadata)
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    List.of(
                            ScalarArgumentSpecification.builder()
                                    .name("SCHEMA")
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("INDEX")
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("QUERY")
                                    .type(VARCHAR)
                                    .build()),
                    GENERIC_TABLE);
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl)
        {
            String schema = ((Slice) ((ScalarArgument) arguments.get("SCHEMA")).getValue()).toStringUtf8();
            String index = ((Slice) ((ScalarArgument) arguments.get("INDEX")).getValue()).toStringUtf8();
            String query = ((Slice) ((ScalarArgument) arguments.get("QUERY")).getValue()).toStringUtf8();

            ElasticsearchTableHandle tableHandle = new ElasticsearchTableHandle(QUERY, schema, index, Optional.of(query));
            ConnectorTableSchema tableSchema = metadata.getTableSchema(session, tableHandle);
            Map<String, ColumnHandle> columnsByName = metadata.getColumnHandles(session, tableHandle);
            List<ColumnHandle> columns = tableSchema.getColumns().stream()
                    .map(ColumnSchema::getName)
                    .map(columnsByName::get)
                    .collect(toImmutableList());

            Descriptor returnedType = new Descriptor(columns.stream()
                    .map(ElasticsearchColumnHandle.class::cast)
                    .map(column -> new Descriptor.Field(column.getName(), Optional.of(column.getType())))
                    .collect(toList()));

            RawQueryFunctionHandle handle = new RawQueryFunctionHandle(tableHandle);

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    public static class RawQueryFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final ElasticsearchTableHandle tableHandle;

        @JsonCreator
        public RawQueryFunctionHandle(@JsonProperty("tableHandle") ElasticsearchTableHandle tableHandle)
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
