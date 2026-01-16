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
package io.trino.plugin.weaviate;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaTableName;
import jakarta.annotation.Nullable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class WeaviateMetadata
        implements ConnectorMetadata
{
    private final WeaviateService weaviateService;

    static final String DEFAULT_SCHEMA = "default";

    @Inject
    public WeaviateMetadata(WeaviateService weaviateService)
    {
        this.weaviateService = requireNonNull(weaviateService, "weaviateService is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return weaviateService.listTenants();
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName name,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        requireNonNull(name, "name is null");

        WeaviateTableHandle tableHandle = weaviateService.getTableHandle(name.getTableName());
        if (name.getSchemaName().equals(DEFAULT_SCHEMA)) {
            return tableHandle;
        }

        String tenant = name.getSchemaName();
        if (!weaviateService.listTenants().contains(tenant)) {
            return null;
        }
        return tableHandle.withTenant(tenant);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle connectorTableHandle)
    {
        requireNonNull(connectorTableHandle, "connectorTableHandle is null");
        return ((WeaviateTableHandle) connectorTableHandle).tableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        requireNonNull(schemaName, "schemaName is null");

        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();

        String schema = schemaName.orElse(null);
        weaviateService.listTableHandles().forEach(table -> tables.add(new SchemaTableName(schema, table.tableName())));
        weaviateService.listAliases().forEach(alias -> tables.add(new SchemaTableName(schema, alias.alias())));

        return tables.build();
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        requireNonNull(schemaName, "schemaName is null");

        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = new HashMap<>();

        listTables(session, schemaName).forEach(name -> {
            WeaviateTableHandle table = weaviateService.getTableHandle(name.getTableName());
            relationColumns.put(name, RelationColumnsMetadata.forTable(name, table.columnsMetadata()));
        });

        return relationFilter.apply(relationColumns.keySet()).stream().map(relationColumns::get).iterator();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle connectorTableHandle)
    {
        requireNonNull(connectorTableHandle, "connectorTableHandle is null");
        WeaviateTableHandle table = (WeaviateTableHandle) connectorTableHandle;
        return table.columns().stream().collect(toImmutableMap(WeaviateColumnHandle::name, Function.identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        return ((WeaviateColumnHandle) columnHandle).columnMetadata();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle connectorTableHandle, long limit)
    {
        requireNonNull(connectorTableHandle, "connectorTableHandle is null");

        WeaviateTableHandle table = (WeaviateTableHandle) connectorTableHandle;

        if (table.limit().isPresent() && table.limit().getAsLong() <= limit) {
            return Optional.empty();
        }

        table = table.withLimit(limit);
        return Optional.of(new LimitApplicationResult<>(table, false, false));
    }
}
