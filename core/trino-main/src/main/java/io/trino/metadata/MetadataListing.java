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
package io.trino.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.security.GrantInfo;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.TABLE_REDIRECTION_ERROR;

public final class MetadataListing
{
    private MetadataListing() {}

    public static SortedMap<String, CatalogName> listCatalogs(Session session, Metadata metadata, AccessControl accessControl)
    {
        return listCatalogs(session, metadata, accessControl, Optional.empty());
    }

    public static SortedMap<String, CatalogName> listCatalogs(Session session, Metadata metadata, AccessControl accessControl, Optional<String> catalogName)
    {
        Map<String, CatalogName> catalogNames;
        if (catalogName.isPresent()) {
            Optional<CatalogName> catalogHandle = metadata.getCatalogHandle(session, catalogName.get());
            if (catalogHandle.isEmpty()) {
                return ImmutableSortedMap.of();
            }
            catalogNames = ImmutableSortedMap.of(catalogName.get(), catalogHandle.get());
        }
        else {
            catalogNames = metadata.getCatalogNames(session);
        }
        Set<String> allowedCatalogs = accessControl.filterCatalogs(session.getIdentity(), catalogNames.keySet());

        ImmutableSortedMap.Builder<String, CatalogName> result = ImmutableSortedMap.naturalOrder();
        for (Map.Entry<String, CatalogName> entry : catalogNames.entrySet()) {
            if (allowedCatalogs.contains(entry.getKey())) {
                result.put(entry);
            }
        }
        return result.build();
    }

    public static SortedSet<String> listSchemas(Session session, Metadata metadata, AccessControl accessControl, String catalogName)
    {
        return listSchemas(session, metadata, accessControl, catalogName, Optional.empty());
    }

    public static SortedSet<String> listSchemas(Session session, Metadata metadata, AccessControl accessControl, String catalogName, Optional<String> schemaName)
    {
        Set<String> schemaNames = ImmutableSet.copyOf(metadata.listSchemaNames(session, catalogName));
        if (schemaName.isPresent()) {
            // we don't use metadata.schemaExists(), because this would change semantics of the method (all vs visible schemas)
            if (!schemaNames.contains(schemaName.get())) {
                return ImmutableSortedSet.of();
            }
            schemaNames = ImmutableSet.of(schemaName.get());
        }
        return ImmutableSortedSet.copyOf(accessControl.filterSchemas(session.toSecurityContext(), catalogName, schemaNames));
    }

    public static Set<SchemaTableName> listTables(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        Set<SchemaTableName> tableNames = metadata.listTables(session, prefix).stream()
                .map(QualifiedObjectName::asSchemaTableName)
                .collect(toImmutableSet());
        return accessControl.filterTables(session.toSecurityContext(), prefix.getCatalogName(), tableNames);
    }

    public static Set<SchemaTableName> listViews(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        Set<SchemaTableName> tableNames = metadata.listViews(session, prefix).stream()
                .map(QualifiedObjectName::asSchemaTableName)
                .collect(toImmutableSet());
        return accessControl.filterTables(session.toSecurityContext(), prefix.getCatalogName(), tableNames);
    }

    public static Map<SchemaTableName, ConnectorViewDefinition> getViews(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(session, prefix).entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().asSchemaTableName(), Entry::getValue));

        Set<SchemaTableName> accessible = accessControl.filterTables(session.toSecurityContext(), prefix.getCatalogName(), views.keySet());

        return views.entrySet().stream()
                .filter(entry -> accessible.contains(entry.getKey()))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }

    public static Set<SchemaTableName> listMaterializedViews(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        Set<SchemaTableName> tableNames = metadata.listMaterializedViews(session, prefix).stream()
                .map(QualifiedObjectName::asSchemaTableName)
                .collect(toImmutableSet());
        return accessControl.filterTables(session.toSecurityContext(), prefix.getCatalogName(), tableNames);
    }

    public static Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        Map<SchemaTableName, ConnectorMaterializedViewDefinition> materializedViews = metadata.getMaterializedViews(session, prefix).entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().asSchemaTableName(), Entry::getValue));

        Set<SchemaTableName> accessible = accessControl.filterTables(session.toSecurityContext(), prefix.getCatalogName(), materializedViews.keySet());

        return materializedViews.entrySet().stream()
                .filter(entry -> accessible.contains(entry.getKey()))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }

    public static Set<GrantInfo> listTablePrivileges(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        List<GrantInfo> grants = metadata.listTablePrivileges(session, prefix);
        Set<SchemaTableName> allowedTables = accessControl.filterTables(
                session.toSecurityContext(),
                prefix.getCatalogName(),
                grants.stream().map(GrantInfo::getSchemaTableName).collect(toImmutableSet()));

        return grants.stream()
                .filter(grantInfo -> allowedTables.contains(grantInfo.getSchemaTableName()))
                .collect(toImmutableSet());
    }

    public static Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        List<TableColumnsMetadata> catalogColumns = getOnlyElement(metadata.listTableColumns(session, prefix).values(), List.of());

        Map<SchemaTableName, Optional<List<ColumnMetadata>>> tableColumns = catalogColumns.stream()
                .collect(toImmutableMap(TableColumnsMetadata::getTable, TableColumnsMetadata::getColumns));

        Set<SchemaTableName> allowedTables = accessControl.filterTables(
                session.toSecurityContext(),
                prefix.getCatalogName(),
                tableColumns.keySet());

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> result = ImmutableMap.builder();

        tableColumns.forEach((table, columnsOptional) -> {
            if (!allowedTables.contains(table)) {
                return;
            }

            QualifiedObjectName originalTableName = new QualifiedObjectName(prefix.getCatalogName(), table.getSchemaName(), table.getTableName());
            List<ColumnMetadata> columns;
            Optional<QualifiedObjectName> targetTableName = Optional.empty();

            if (columnsOptional.isPresent()) {
                columns = columnsOptional.get();
            }
            else {
                TableHandle targetTableHandle = null;
                boolean redirectionSucceeded = false;

                try {
                    // Handle redirection before filterColumns check
                    RedirectionAwareTableHandle redirection = metadata.getRedirectionAwareTableHandle(session, originalTableName);
                    targetTableName = redirection.getRedirectedTableName();

                    // The target table name should be non-empty. If it is empty, it means that there is an
                    // inconsistency in the connector's implementation of ConnectorMetadata#streamTableColumns and
                    // ConnectorMetadata#redirectTable.
                    if (targetTableName.isPresent()) {
                        redirectionSucceeded = true;
                        targetTableHandle = redirection.getTableHandle().orElseThrow();
                    }
                }
                catch (TrinoException e) {
                    // Ignore redirection errors
                    if (!e.getErrorCode().equals(TABLE_REDIRECTION_ERROR.toErrorCode())) {
                        throw e;
                    }
                }

                if (redirectionSucceeded == false) {
                    return;
                }

                columns = metadata.getTableMetadata(session, targetTableHandle).getColumns();
            }

            Set<String> allowedColumns = accessControl.filterColumns(
                    session.toSecurityContext(),
                    // Use redirected table name for applying column filters
                    targetTableName.orElse(originalTableName).asCatalogSchemaTableName(),
                    columns.stream()
                            .map(ColumnMetadata::getName)
                            .collect(toImmutableSet()));
            result.put(
                    table,
                    columns.stream()
                            .filter(column -> allowedColumns.contains(column.getName()))
                            .collect(toImmutableList()));
        });

        return result.build();
    }
}
