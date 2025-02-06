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
package io.trino.plugin.neo4j.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.plugin.neo4j.Neo4jQueryRelationHandle;
import io.trino.plugin.neo4j.Neo4jTypeManager;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.DescriptorArgument;
import io.trino.spi.function.table.DescriptorArgumentSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class Query
        implements Provider<ConnectorTableFunction>
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "query";
    private final Neo4jTypeManager typeManager;

    @Inject
    public Query(Neo4jTypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new QueryFunction(this.typeManager);
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        private static final String DATABASE_ARGUMENT = "DATABASE";
        private static final String QUERY_ARGUMENT = "QUERY";
        private static final String SCHEMA_ARGUMENT = "SCHEMA";

        private final Neo4jTypeManager typeManager;

        public QueryFunction(Neo4jTypeManager typeManager)
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name(QUERY_ARGUMENT)
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name(DATABASE_ARGUMENT)
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            DescriptorArgumentSpecification.builder()
                                    .name(SCHEMA_ARGUMENT)
                                    .defaultValue(null)
                                    .build()),

                    GENERIC_TABLE);
            this.typeManager = typeManager;
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl)
        {
            ScalarArgument queryArgument = (ScalarArgument) arguments.get(QUERY_ARGUMENT);
            String query = ((Slice) queryArgument.getValue()).toStringUtf8();

            ScalarArgument databaseArgument = ((ScalarArgument) arguments.get(DATABASE_ARGUMENT));
            Optional<String> databaseName = Optional.ofNullable((Slice) databaseArgument.getValue())
                    .map(Slice::toStringUtf8);

            Descriptor descriptor = ((DescriptorArgument) arguments.get(SCHEMA_ARGUMENT))
                    .getDescriptor()
                    .orElse(typeManager.getDynamicResultDescriptor());

            Neo4jQueryRelationHandle queryHandle = new Neo4jQueryRelationHandle(query, descriptor, databaseName);

            return TableFunctionAnalysis.builder()
                    .handle(new QueryFunctionHandle(queryHandle))
                    .returnedType(descriptor)
                    .build();
        }
    }

    public static class QueryFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final Neo4jQueryRelationHandle queryHandle;

        @JsonCreator
        public QueryFunctionHandle(
                @JsonProperty("queryHandle") Neo4jQueryRelationHandle queryHandle)
        {
            this.queryHandle = requireNonNull(queryHandle, "queryHandle is null");
        }

        @JsonProperty
        public Neo4jQueryRelationHandle getQueryHandle()
        {
            return queryHandle;
        }
    }
}
