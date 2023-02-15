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
package io.trino.plugin.cassandra.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.cassandra.CassandraColumnHandle;
import io.trino.plugin.cassandra.CassandraMetadata;
import io.trino.plugin.cassandra.CassandraQueryRelationHandle;
import io.trino.plugin.cassandra.CassandraTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class Query
        implements Provider<ConnectorTableFunction>
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "query";

    private final CassandraMetadata cassandraMetadata;

    @Inject
    public Query(CassandraMetadata cassandraMetadata)
    {
        this.cassandraMetadata = requireNonNull(cassandraMetadata, "cassandraMetadata is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new QueryFunction(cassandraMetadata), getClass().getClassLoader());
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        private final CassandraMetadata cassandraMetadata;

        public QueryFunction(CassandraMetadata cassandraMetadata)
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    ImmutableList.of(ScalarArgumentSpecification.builder()
                            .name("QUERY")
                            .type(VARCHAR)
                            .build()),
                    GENERIC_TABLE);
            this.cassandraMetadata = requireNonNull(cassandraMetadata, "metadata is null");
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            ScalarArgument argument = (ScalarArgument) getOnlyElement(arguments.values());
            String query = ((Slice) argument.getValue()).toStringUtf8();

            CassandraQueryRelationHandle queryRelationHandle = new CassandraQueryRelationHandle(query);
            List<ColumnHandle> columnHandles = cassandraMetadata.getColumnHandles(query);
            checkState(!columnHandles.isEmpty(), "Handle doesn't have columns info");
            Descriptor returnedType = new Descriptor(columnHandles.stream()
                    .map(CassandraColumnHandle.class::cast)
                    .map(column -> new Field(column.getName(), Optional.of(column.getType())))
                    .collect(toImmutableList()));

            QueryHandle handle = new QueryHandle(new CassandraTableHandle(queryRelationHandle));

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    public static class QueryHandle
            implements ConnectorTableFunctionHandle
    {
        private final CassandraTableHandle tableHandle;

        @JsonCreator
        public QueryHandle(@JsonProperty("tableHandle") CassandraTableHandle tableHandle)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        }

        @JsonProperty
        public CassandraTableHandle getTableHandle()
        {
            return tableHandle;
        }
    }
}
