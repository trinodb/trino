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
import io.airlift.slice.Slice;
import io.trino.plugin.elasticsearch.ElasticsearchColumnHandle;
import io.trino.plugin.elasticsearch.ElasticsearchMetadata;
import io.trino.plugin.elasticsearch.ElasticsearchTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.ptf.Argument;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;
import io.trino.spi.ptf.Descriptor;
import io.trino.spi.ptf.ScalarArgument;
import io.trino.spi.ptf.ScalarArgumentSpecification;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.QUERY;
import static io.trino.spi.ptf.DescriptorMapping.EMPTY_MAPPING;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RemoteQuery
        implements Provider<ConnectorTableFunction>
{
    public static final String NAME = "remote_query";

    private final ElasticsearchMetadata metadata;

    @Inject
    public RemoteQuery(ElasticsearchMetadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new RemoteQueryFunction(metadata);
    }

    public static class RemoteQueryFunction
            extends ConnectorTableFunction
    {
        public static final String SCHEMA_NAME = "system";

        private final ElasticsearchMetadata metadata;

        public RemoteQueryFunction(ElasticsearchMetadata metadata)
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    List.of(
                            new ScalarArgumentSpecification("schema", VARCHAR),
                            new ScalarArgumentSpecification("index", VARCHAR),
                            new ScalarArgumentSpecification("query", VARCHAR)),
                    GENERIC_TABLE);
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public Analysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            String schema = ((Slice) ((ScalarArgument) arguments.get("schema")).getValue()).toStringUtf8();
            String index = ((Slice) ((ScalarArgument) arguments.get("index")).getValue()).toStringUtf8();
            String query = ((Slice) ((ScalarArgument) arguments.get("query")).getValue()).toStringUtf8();

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

            RemoteQueryHandle handle = new RemoteQueryHandle(tableHandle);

            return new Analysis(Optional.of(returnedType), EMPTY_MAPPING, handle);
        }
    }

    public static class RemoteQueryHandle
            implements ConnectorTableFunctionHandle
    {
        private final ElasticsearchTableHandle tableHandle;

        @JsonCreator
        public RemoteQueryHandle(@JsonProperty("tableHandle") ElasticsearchTableHandle tableHandle)
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
