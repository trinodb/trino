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
package io.trino.plugin.mongodb.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.plugin.mongodb.MongoColumnHandle;
import io.trino.plugin.mongodb.MongoMetadata;
import io.trino.plugin.mongodb.MongoTableHandle;
import io.trino.plugin.mongodb.MongoTransactionManager;
import io.trino.plugin.mongodb.RemoteTableName;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import org.bson.Document;
import org.bson.json.JsonParseException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class Query
        implements Provider<ConnectorTableFunction>
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "query";

    private final MongoTransactionManager transactionManager;

    @Inject
    public Query(MongoTransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new QueryFunction(transactionManager);
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        private final MongoTransactionManager transactionManager;

        public QueryFunction(MongoTransactionManager transactionManager)
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name("DATABASE")
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("COLLECTION")
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("FILTER")
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
            MongoMetadata metadata = transactionManager.getMetadata(transaction);
            String database = ((Slice) ((ScalarArgument) arguments.get("DATABASE")).getValue()).toStringUtf8();
            String collection = ((Slice) ((ScalarArgument) arguments.get("COLLECTION")).getValue()).toStringUtf8();
            String filter = ((Slice) ((ScalarArgument) arguments.get("FILTER")).getValue()).toStringUtf8();
            if (!database.equals(database.toLowerCase(ENGLISH))) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Only lowercase database name is supported");
            }
            if (!collection.equals(collection.toLowerCase(ENGLISH))) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Only lowercase collection name is supported");
            }
            SchemaTableName schemaTableName = new SchemaTableName(database, collection);
            MongoTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName);
            if (tableHandle == null) {
                throw new TableNotFoundException(schemaTableName);
            }

            RemoteTableName remoteTableName = tableHandle.getRemoteTableName();
            // Don't store Document object to MongoTableHandle for avoiding serialization issue
            parseFilter(filter);

            tableHandle = new MongoTableHandle(schemaTableName, remoteTableName, Optional.of(filter));
            ConnectorTableSchema tableSchema = metadata.getTableSchema(session, tableHandle);
            Map<String, ColumnHandle> columnsByName = metadata.getColumnHandles(session, tableHandle);
            List<ColumnHandle> columns = tableSchema.getColumns().stream()
                    .filter(column -> !column.isHidden())
                    .map(ColumnSchema::getName)
                    .map(columnsByName::get)
                    .collect(toImmutableList());

            Descriptor returnedType = new Descriptor(columns.stream()
                    .map(MongoColumnHandle.class::cast)
                    .map(column -> new Descriptor.Field(column.getBaseName(), Optional.of(column.getType())))
                    .collect(toImmutableList()));

            QueryFunctionHandle handle = new QueryFunctionHandle(tableHandle);

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    public static Document parseFilter(String filter)
    {
        try {
            return Document.parse(filter);
        }
        catch (JsonParseException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Can't parse 'filter' argument as json", e);
        }
    }

    public static class QueryFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final MongoTableHandle tableHandle;

        @JsonCreator
        public QueryFunctionHandle(@JsonProperty("tableHandle") MongoTableHandle tableHandle)
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
