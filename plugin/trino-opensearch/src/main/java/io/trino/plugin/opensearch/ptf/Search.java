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
package io.trino.plugin.opensearch.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.plugin.opensearch.OpenSearchColumnHandle;
import io.trino.plugin.opensearch.OpenSearchMetadata;
import io.trino.plugin.opensearch.OpenSearchTableHandle;
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
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Search
        implements Provider<ConnectorTableFunction>
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "search";

    // ObjectMapper is thread-safe for reads once configured; we never mutate it after construction.
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final Set<String> RESERVED_SEARCH_BODY_KEYS = ImmutableSet.of(
            "size",
            "from",
            "sort",
            "aggs",
            "aggregations",
            "highlight",
            "_source",
            "track_total_hits",
            "search_after",
            "timeout",
            "terminate_after");

    private final OpenSearchMetadata metadata;

    @Inject
    public Search(OpenSearchMetadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new SearchFunction(metadata);
    }

    public static class SearchFunction
            extends AbstractConnectorTableFunction
    {
        private final OpenSearchMetadata metadata;

        public SearchFunction(OpenSearchMetadata metadata)
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
            String schema = readArgument(arguments, "SCHEMA");
            String index = readArgument(arguments, "INDEX");
            String query = readArgument(arguments, "QUERY");

            validateQueryJson(query);

            OpenSearchTableHandle tableHandle = new OpenSearchTableHandle(
                    OpenSearchTableHandle.Type.SCAN,
                    schema,
                    index,
                    Optional.of(query));

            // ColumnMetadata lowercases names, while the handle map preserves the OpenSearch mapping case,
            // so resolving ColumnSchema names against the map would miss any mixed-case field (e.g. `createdAt`).
            List<OpenSearchColumnHandle> columns = metadata.getColumnHandles(session, tableHandle).values().stream()
                    .map(OpenSearchColumnHandle.class::cast)
                    .collect(toImmutableList());

            if (columns.isEmpty()) {
                throw new TrinoException(
                        INVALID_FUNCTION_ARGUMENT,
                        format("Index '%s' has no mapped fields", index));
            }

            Descriptor returnedType = new Descriptor(columns.stream()
                    .map(column -> new Descriptor.Field(column.name(), Optional.of(column.type())))
                    .collect(toImmutableList()));

            SearchFunctionHandle handle = new SearchFunctionHandle(tableHandle);

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }

        private static String readArgument(Map<String, Argument> arguments, String name)
        {
            Argument argument = arguments.get(name);
            if (argument == null) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Argument '%s' is required", name));
            }
            Object value = ((ScalarArgument) argument).getValue();
            if (value == null) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Argument '%s' must not be null", name));
            }
            String text = ((Slice) value).toStringUtf8();
            if (text.isEmpty()) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Argument '%s' must not be empty", name));
            }
            return text;
        }

        private static void validateQueryJson(String query)
        {
            JsonNode node;
            try {
                node = JSON.readTree(query);
            }
            catch (JsonProcessingException e) {
                throw new TrinoException(
                        INVALID_FUNCTION_ARGUMENT,
                        format("'query' must be a valid JSON object: %s", e.getOriginalMessage()));
            }
            if (node == null || !node.isObject()) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'query' must be a JSON object");
            }
            if (node.isEmpty()) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'query' must not be empty");
            }
            node.fieldNames().forEachRemaining(field -> {
                if (RESERVED_SEARCH_BODY_KEYS.contains(field)) {
                    throw new TrinoException(
                            INVALID_FUNCTION_ARGUMENT,
                            format(
                                    "'query' must be a query object, not a full _search body; unexpected key '%s' (use raw_query for full _search bodies)",
                                    field));
                }
            });
        }
    }

    public static class SearchFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final OpenSearchTableHandle tableHandle;

        @JsonCreator
        public SearchFunctionHandle(@JsonProperty("tableHandle") OpenSearchTableHandle tableHandle)
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
