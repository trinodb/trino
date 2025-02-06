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
package io.trino.plugin.neo4j;

import com.google.inject.Inject;
import io.trino.plugin.neo4j.ptf.Query;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class Neo4jMetadata
        implements ConnectorMetadata
{
    private final Neo4jClient client;
    private final Neo4jTypeManager typeManager;

    public static final Neo4jColumnHandle ELEMENT_ID_COLUMN = new Neo4jColumnHandle(
            "$elementId", VARCHAR, true);

    @Inject
    public Neo4jMetadata(Neo4jClient client, Neo4jTypeManager typeManager)
    {
        this.client = requireNonNull(client, "client is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return this.client.listSchemaNames(session);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return this.client.listTables(session, schemaName);
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        return this.client.getTable(schemaTableName).getTableHandle();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Neo4jTableHandle handle = (Neo4jTableHandle) tableHandle;
        Neo4jRelationHandle relationHandle = handle.getRelationHandle();

        if (relationHandle instanceof Neo4jQueryRelationHandle queryRelationHandle) {
            List<ColumnMetadata> columnMetadata = queryRelationHandle.getDescriptor()
                    .getFields()
                    .stream()
                    .map(f -> new ColumnMetadata(f.getName().orElseThrow(), f.getType().orElseThrow()))
                    .collect(toImmutableList());

            return new ConnectorTableMetadata(
                    new SchemaTableName("_generated", "_generated_query"),
                    columnMetadata);
        }
        else if (relationHandle instanceof Neo4jNamedRelationHandle namedRelationHandle) {
            Neo4jTable table = this.client.getTable(namedRelationHandle.getSchemaTableName());

            return new ConnectorTableMetadata(
                    table.getTableHandle().getRequiredNamedRelation().getSchemaTableName(),
                    table.getColumns().stream().map(Neo4jColumnHandle::toColumnMetadata).collect(toImmutableList()));
        }

        throw new IllegalStateException("Unknown Neo4jRelationHandle: %s".formatted(relationHandle.getClass()));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Neo4jTable table = this.client.getTable(((Neo4jTableHandle) tableHandle).getRequiredNamedRelation().getSchemaTableName());

        return table.getColumns()
                .stream()
                .collect(toImmutableMap(Neo4jColumnHandle::getColumnName, c -> c));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((Neo4jColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        // TupleDomain<ColumnHandle> summary = constraint.getSummary();

        return ConnectorMetadata.super.applyFilter(session, handle, constraint);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle tableHandle, long limit)
    {
        Neo4jTableHandle handle = (Neo4jTableHandle) tableHandle;

        return handle.getNamedRelation()
                .map(r -> {
                    Neo4jTableHandle tableHandleWithLimit = new Neo4jTableHandle(new Neo4jNamedRelationHandle(
                            r.getSchemaTableName(),
                            r.getRemoteTableName(),
                            r.getTableType(),
                            OptionalLong.of(limit)));
                    return new LimitApplicationResult<>(tableHandleWithLimit, true, true);
                });
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (!(handle instanceof Query.QueryFunctionHandle queryFunctionHandle)) {
            return Optional.empty();
        }

        Neo4jQueryRelationHandle queryHandle = queryFunctionHandle.getQueryHandle();

        if (this.typeManager.isDynamicResultDescriptor(queryHandle.getDescriptor())) {
            return Optional.of(new TableFunctionApplicationResult<>(new Neo4jTableHandle(queryHandle),
                    List.of(this.typeManager.getDynamicResultColumn())));
        }
        else {
            List<ColumnHandle> columnHandles = queryHandle.getDescriptor()
                    .getFields().stream()
                    .map(f -> new Neo4jColumnHandle(f.getName().orElseThrow(), f.getType().orElseThrow(), true))
                    .collect(toImmutableList());

            return Optional.of(new TableFunctionApplicationResult<>(new Neo4jTableHandle(queryHandle), columnHandles));
        }
    }
}
