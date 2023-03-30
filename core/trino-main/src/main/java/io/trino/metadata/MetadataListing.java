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
import com.google.common.collect.ImmutableSortedSet;
import io.trino.Session;
import io.trino.security.AccessControl;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.security.GrantInfo;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_REDIRECTION_ERROR;

public final class MetadataListing
{
    private MetadataListing() {}

    public static SortedSet<String> listCatalogNames(Session session, Metadata metadata, AccessControl accessControl)
    {
        return listCatalogNames(session, metadata, accessControl, Optional.empty());
    }

    public static SortedSet<String> listCatalogNames(Session session, Metadata metadata, AccessControl accessControl, Optional<String> catalogName)
    {
        Set<String> catalogs;
        if (catalogName.isPresent()) {
            Optional<CatalogHandle> catalogHandle = metadata.getCatalogHandle(session, catalogName.get());
            if (catalogHandle.isEmpty()) {
                return ImmutableSortedSet.of();
            }
            catalogs = ImmutableSet.of(catalogName.get());
        }
        else {
            catalogs = metadata.listCatalogs(session).stream()
                    .map(CatalogInfo::getCatalogName)
                    .collect(toImmutableSet());
        }
        return ImmutableSortedSet.copyOf(accessControl.filterCatalogs(session.toSecurityContext(), catalogs));
    }

    public static List<CatalogInfo> listCatalogs(Session session, Metadata metadata, AccessControl accessControl)
    {
        List<CatalogInfo> catalogs = metadata.listCatalogs(session);
        Set<String> catalogNames = catalogs.stream()
                .map(CatalogInfo::getCatalogName)
                .collect(toImmutableSet());
        Set<String> allowedCatalogs = accessControl.filterCatalogs(session.toSecurityContext(), catalogNames);
        return catalogs.stream()
                .filter(catalogInfo -> allowedCatalogs.contains(catalogInfo.getCatalogName()))
                .collect(toImmutableList());
    }

    public static SortedSet<String> listSchemas(Session session, Metadata metadata, AccessControl accessControl, String catalogName)
    {
        return listSchemas(session, metadata, accessControl, catalogName, Optional.empty());
    }

    public static SortedSet<String> listSchemas(Session session, Metadata metadata, AccessControl accessControl, String catalogName, Optional<String> schemaName)
    {
        try {
            return doListSchemas(session, metadata, accessControl, catalogName, schemaName);
        }
        catch (RuntimeException exception) {
            throw handleListingException(exception, "schemas", catalogName);
        }
    }

    private static SortedSet<String> doListSchemas(Session session, Metadata metadata, AccessControl accessControl, String catalogName, Optional<String> schemaName)
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
        try {
            return doListTables(session, metadata, accessControl, prefix);
        }
        catch (RuntimeException exception) {
            throw handleListingException(exception, "tables", prefix.getCatalogName());
        }
    }

    private static Set<SchemaTableName> doListTables(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        Set<SchemaTableName> tableNames = metadata.listTables(session, prefix).stream()
                .map(QualifiedObjectName::asSchemaTableName)
                .collect(toImmutableSet());

        // Table listing operation only involves getting table names, but not any metadata. So redirected tables are not
        // handled any differently. The target table or catalog are not involved. Thus the following filter is only called
        // for the source catalog on source table names.
        return accessControl.filterTables(session.toSecurityContext(), prefix.getCatalogName(), tableNames);
    }

    public static Set<SchemaTableName> listViews(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        try {
            return doListViews(session, metadata, accessControl, prefix);
        }
        catch (RuntimeException exception) {
            throw handleListingException(exception, "views", prefix.getCatalogName());
        }
    }

    private static Set<SchemaTableName> doListViews(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        Set<SchemaTableName> tableNames = metadata.listViews(session, prefix).stream()
                .map(QualifiedObjectName::asSchemaTableName)
                .collect(toImmutableSet());
        return accessControl.filterTables(session.toSecurityContext(), prefix.getCatalogName(), tableNames);
    }

    public static Map<SchemaTableName, ViewInfo> getViews(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        try {
            return doGetViews(session, metadata, accessControl, prefix);
        }
        catch (RuntimeException exception) {
            throw handleListingException(exception, "views", prefix.getCatalogName());
        }
    }

    private static Map<SchemaTableName, ViewInfo> doGetViews(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        Map<SchemaTableName, ViewInfo> views = metadata.getViews(session, prefix).entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().asSchemaTableName(), Entry::getValue));

        Set<SchemaTableName> accessible = accessControl.filterTables(session.toSecurityContext(), prefix.getCatalogName(), views.keySet());

        return views.entrySet().stream()
                .filter(entry -> accessible.contains(entry.getKey()))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }

    public static Set<SchemaTableName> listMaterializedViews(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        try {
            return doListMaterializedViews(session, metadata, accessControl, prefix);
        }
        catch (RuntimeException exception) {
            throw handleListingException(exception, "materialized views", prefix.getCatalogName());
        }
    }

    private static Set<SchemaTableName> doListMaterializedViews(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        Set<SchemaTableName> tableNames = metadata.listMaterializedViews(session, prefix).stream()
                .map(QualifiedObjectName::asSchemaTableName)
                .collect(toImmutableSet());
        return accessControl.filterTables(session.toSecurityContext(), prefix.getCatalogName(), tableNames);
    }

    public static Map<SchemaTableName, ViewInfo> getMaterializedViews(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        try {
            return doGetMaterializedViews(session, metadata, accessControl, prefix);
        }
        catch (RuntimeException exception) {
            throw handleListingException(exception, "materialized views", prefix.getCatalogName());
        }
    }

    private static Map<SchemaTableName, ViewInfo> doGetMaterializedViews(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        Map<SchemaTableName, ViewInfo> materializedViews = metadata.getMaterializedViews(session, prefix).entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().asSchemaTableName(), Entry::getValue));

        Set<SchemaTableName> accessible = accessControl.filterTables(session.toSecurityContext(), prefix.getCatalogName(), materializedViews.keySet());

        return materializedViews.entrySet().stream()
                .filter(entry -> accessible.contains(entry.getKey()))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }

    public static Set<GrantInfo> listTablePrivileges(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        try {
            return doListTablePrivileges(session, metadata, accessControl, prefix);
        }
        catch (RuntimeException exception) {
            throw handleListingException(exception, "table privileges", prefix.getCatalogName());
        }
    }

    private static Set<GrantInfo> doListTablePrivileges(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
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
        try {
            return doListTableColumns(session, metadata, accessControl, prefix);
        }
        catch (RuntimeException exception) {
            throw handleListingException(exception, "table columns", prefix.getCatalogName());
        }
    }

    private static Map<SchemaTableName, List<ColumnMetadata>> doListTableColumns(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        List<TableColumnsMetadata> catalogColumns = metadata.listTableColumns(session, prefix);

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
                    // For redirected tables, column listing requires special handling, because the column metadata is unavailable
                    // at the source table, and needs to be fetched from the target table.
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

                if (!redirectionSucceeded) {
                    return;
                }

                columns = metadata.getTableMetadata(session, targetTableHandle).getColumns();
            }

            Set<String> allowedColumns = accessControl.filterColumns(
                    session.toSecurityContext(),
                    // Use redirected table name for applying column filters, since the source does not know the column metadata
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

        return result.buildOrThrow();
    }

    private static TrinoException handleListingException(RuntimeException exception, String type, String catalogName)
    {
        ErrorCodeSupplier result = GENERIC_INTERNAL_ERROR;
        if (exception instanceof TrinoException trinoException) {
            result = trinoException::getErrorCode;
        }
        return new TrinoException(
                result,
                "Error listing %s for catalog %s: %s".formatted(type, catalogName, exception.getMessage()),
                exception);
    }
}
