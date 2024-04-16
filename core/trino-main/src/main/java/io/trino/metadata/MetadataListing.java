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
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_REDIRECTION_ERROR;
import static java.util.function.Function.identity;

public final class MetadataListing
{
    private MetadataListing() {}

    public static SortedSet<String> listCatalogNames(Session session, Metadata metadata, AccessControl accessControl, Domain catalogDomain)
    {
        Optional<String> catalogName = tryGetSingleVarcharValue(catalogDomain);
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
                    .map(CatalogInfo::catalogName)
                    .filter(stringFilter(catalogDomain))
                    .collect(toImmutableSet());
        }
        return ImmutableSortedSet.copyOf(accessControl.filterCatalogs(session.toSecurityContext(), catalogs));
    }

    public static List<CatalogInfo> listCatalogs(Session session, Metadata metadata, AccessControl accessControl)
    {
        List<CatalogInfo> catalogs = metadata.listCatalogs(session);
        Set<String> catalogNames = catalogs.stream()
                .map(CatalogInfo::catalogName)
                .collect(toImmutableSet());
        Set<String> allowedCatalogs = accessControl.filterCatalogs(session.toSecurityContext(), catalogNames);
        return catalogs.stream()
                .filter(catalogInfo -> allowedCatalogs.contains(catalogInfo.catalogName()))
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

    public static Map<SchemaTableName, RelationType> getRelationTypes(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        try {
            return doGetRelationTypes(session, metadata, accessControl, prefix);
        }
        catch (RuntimeException exception) {
            throw handleListingException(exception, "tables", prefix.getCatalogName());
        }
    }

    private static Map<SchemaTableName, RelationType> doGetRelationTypes(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        Map<SchemaTableName, RelationType> relationTypes = metadata.getRelationTypes(session, prefix);

        // Table listing operation only involves getting table names, but not any metadata. So redirected tables are not
        // handled any differently. The target table or catalog are not involved. Thus the following filter is only called
        // for the source catalog on source table names.
        Set<SchemaTableName> accessibleNames = accessControl.filterTables(session.toSecurityContext(), prefix.getCatalogName(), relationTypes.keySet());
        if (accessibleNames.equals(relationTypes.keySet())) {
            return relationTypes;
        }
        return accessibleNames.stream()
                .collect(toImmutableMap(identity(), relationTypes::get));
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
        AtomicInteger filteredCount = new AtomicInteger();
        List<TableColumnsMetadata> catalogColumns = metadata.listTableColumns(
                session,
                prefix,
                relationNames -> {
                    Set<SchemaTableName> filtered = accessControl.filterTables(session.toSecurityContext(), prefix.getCatalogName(), relationNames);
                    filteredCount.addAndGet(filtered.size());
                    return filtered;
                });
        checkState(
                // Inequality because relationFilter can be invoked more than once on a set of names.
                filteredCount.get() >= catalogColumns.size(),
                "relationFilter is mandatory, but it has not been called for some of returned relations: returned %s relations, %s passed the filter",
                catalogColumns.size(),
                filteredCount.get());

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> result = ImmutableMap.builder();

        // Process tables without redirect
        Map<SchemaTableName, Set<String>> columnNamesByTable = catalogColumns.stream()
                .filter(tableColumnsMetadata -> tableColumnsMetadata.getColumns().isPresent())
                .collect(toImmutableMap(
                        TableColumnsMetadata::getTable,
                        tableColumnsMetadata -> tableColumnsMetadata.getColumns().orElseThrow().stream()
                                .map(ColumnMetadata::getName)
                                .collect(toImmutableSet())));
        Map<SchemaTableName, Set<String>> catalogAllowedColumns = accessControl.filterColumns(session.toSecurityContext(), prefix.getCatalogName(), columnNamesByTable);
        catalogColumns.stream()
                .filter(tableColumnsMetadata -> tableColumnsMetadata.getColumns().isPresent())
                .forEach(tableColumnsMetadata -> {
                    Set<String> allowedTableColumns = catalogAllowedColumns.getOrDefault(tableColumnsMetadata.getTable(), ImmutableSet.of());
                    result.put(
                            tableColumnsMetadata.getTable(),
                            tableColumnsMetadata.getColumns().get().stream()
                                    .filter(column -> allowedTableColumns.contains(column.getName()))
                                    .collect(toImmutableList()));
                });

        // Process redirects
        catalogColumns.stream()
                .filter(tableColumnsMetadata -> tableColumnsMetadata.getColumns().isEmpty())
                .forEach(tableColumnsMetadata -> {
                    SchemaTableName table = tableColumnsMetadata.getTable();
                    QualifiedObjectName originalTableName = new QualifiedObjectName(prefix.getCatalogName(), table.getSchemaName(), table.getTableName());
                    QualifiedObjectName actualTableName;
                    TableHandle targetTableHandle;
                    try {
                        // For redirected tables, column listing requires special handling, because the column metadata is unavailable
                        // at the source table, and needs to be fetched from the target table.
                        RedirectionAwareTableHandle redirection = metadata.getRedirectionAwareTableHandle(session, originalTableName);

                        // The target table name should be non-empty. If it is empty, it means that there is an
                        // inconsistency in the connector's implementation of ConnectorMetadata#streamTableColumns and
                        // ConnectorMetadata#redirectTable.
                        if (redirection.redirectedTableName().isEmpty()) {
                            return;
                        }
                        actualTableName = redirection.redirectedTableName().get();
                        targetTableHandle = redirection.tableHandle().orElseThrow();
                    }
                    catch (TrinoException e) {
                        // Ignore redirection errors
                        if (e.getErrorCode().equals(TABLE_REDIRECTION_ERROR.toErrorCode())) {
                            return;
                        }
                        throw e;
                    }

                    List<ColumnMetadata> columns = metadata.getTableMetadata(session, targetTableHandle).columns();

                    Set<String> allowedColumns = accessControl.filterColumns(
                                    session.toSecurityContext(),
                                    actualTableName.asCatalogSchemaTableName().getCatalogName(),
                                    ImmutableMap.of(
                                            // Use redirected table name for applying column filters, since the source does not know the column metadata
                                            actualTableName.asSchemaTableName(),
                                            columns.stream()
                                                    .map(ColumnMetadata::getName)
                                                    .collect(toImmutableSet())))
                            .getOrDefault(actualTableName.asSchemaTableName(), ImmutableSet.of());
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

    private static Predicate<String> stringFilter(Domain varcharDomain)
    {
        checkArgument(varcharDomain.getType() instanceof VarcharType, "Invalid domain type: %s", varcharDomain.getType());
        if (varcharDomain.isAll()) {
            return value -> true;
        }
        return value -> varcharDomain.includesNullableValue(value == null ? null : utf8Slice(value));
    }
}
