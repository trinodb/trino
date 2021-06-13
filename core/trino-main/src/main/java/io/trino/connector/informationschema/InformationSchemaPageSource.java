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
package io.trino.connector.informationschema;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.Type;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.union;
import static io.trino.SystemSessionProperties.isOmitDateTimeTypePrecision;
import static io.trino.connector.informationschema.InformationSchemaMetadata.defaultPrefixes;
import static io.trino.connector.informationschema.InformationSchemaMetadata.isTablesEnumeratingTable;
import static io.trino.metadata.MetadataListing.getViews;
import static io.trino.metadata.MetadataListing.listSchemas;
import static io.trino.metadata.MetadataListing.listTableColumns;
import static io.trino.metadata.MetadataListing.listTablePrivileges;
import static io.trino.metadata.MetadataListing.listTables;
import static io.trino.metadata.MetadataListing.listViews;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.type.TypeUtils.getDisplayLabel;
import static java.util.Objects.requireNonNull;

public class InformationSchemaPageSource
        implements ConnectorPageSource
{
    private final Session session;
    private final Metadata metadata;
    private final AccessControl accessControl;

    private final String catalogName;
    private final InformationSchemaTable table;
    private final Supplier<Iterator<QualifiedTablePrefix>> prefixIterator;
    private final OptionalLong limit;

    private final List<Type> types;
    private final Queue<Page> pages = new ArrayDeque<>();
    private final PageBuilder pageBuilder;
    private final Function<Page, Page> projection;

    private final Optional<Set<String>> roles;
    private final Optional<Set<String>> grantees;
    private long recordCount;
    private long completedBytes;
    private long memoryUsageBytes;
    private boolean closed;

    public InformationSchemaPageSource(
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            InformationSchemaTableHandle tableHandle,
            List<ColumnHandle> columns)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columns, "columns is null");

        catalogName = tableHandle.getCatalogName();
        table = tableHandle.getTable();
        prefixIterator = Suppliers.memoize(() -> {
            Set<QualifiedTablePrefix> prefixes = tableHandle.getPrefixes();
            if (tableHandle.getLimit().isEmpty()) {
                // no limit is used, therefore it doesn't make sense to split information schema query into smaller ones
                return prefixes.iterator();
            }

            if (isTablesEnumeratingTable(table)) {
                if (prefixes.equals(defaultPrefixes(catalogName))) {
                    prefixes = metadata.listSchemaNames(session, catalogName).stream()
                            .map(schema -> new QualifiedTablePrefix(catalogName, schema))
                            .collect(toImmutableSet());
                }
            }
            else {
                checkArgument(prefixes.equals(defaultPrefixes(catalogName)), "Catalog-wise tables have prefixes other than the default one");
            }
            return prefixes.iterator();
        });
        limit = tableHandle.getLimit();

        roles = tableHandle.getRoles();
        grantees = tableHandle.getGrantees();

        List<ColumnMetadata> columnMetadata = table.getTableMetadata().getColumns();

        types = columnMetadata.stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());

        pageBuilder = new PageBuilder(types);

        Map<String, Integer> columnNameToChannel = IntStream.range(0, columnMetadata.size())
                .boxed()
                .collect(toImmutableMap(i -> columnMetadata.get(i).getName(), Function.identity()));

        List<Integer> channels = columns.stream()
                .map(columnHandle -> (InformationSchemaColumnHandle) columnHandle)
                .map(columnHandle -> columnNameToChannel.get(columnHandle.getColumnName()))
                .collect(toImmutableList());

        projection = page -> {
            Block[] blocks = new Block[channels.size()];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = page.getBlock(channels.get(i));
            }
            return new Page(page.getPositionCount(), blocks);
        };
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return closed ||
                (pages.isEmpty() && (!prefixIterator.get().hasNext() || isLimitExhausted()));
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }

        if (pages.isEmpty()) {
            buildPages();
        }

        Page page = pages.poll();

        if (page == null) {
            return null;
        }

        memoryUsageBytes -= page.getRetainedSizeInBytes();
        Page outputPage = projection.apply(page);
        completedBytes += outputPage.getSizeInBytes();
        return outputPage;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return memoryUsageBytes + pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void close()
    {
        closed = true;
    }

    private void buildPages()
    {
        while (pages.isEmpty() && prefixIterator.get().hasNext() && !closed && !isLimitExhausted()) {
            QualifiedTablePrefix prefix = prefixIterator.get().next();
            switch (table) {
                case COLUMNS:
                    addColumnsRecords(prefix);
                    break;
                case TABLES:
                    addTablesRecords(prefix);
                    break;
                case VIEWS:
                    addViewsRecords(prefix);
                    break;
                case SCHEMATA:
                    addSchemataRecords();
                    break;
                case TABLE_PRIVILEGES:
                    addTablePrivilegesRecords(prefix);
                    break;
                case ROLES:
                    addRolesRecords();
                    break;
                case APPLICABLE_ROLES:
                    addApplicableRolesRecords();
                    break;
                case ENABLED_ROLES:
                    addEnabledRolesRecords();
                    break;
                case ROLE_AUTHORIZATION_DESCRIPTORS:
                    addRoleAuthorizationDescriptorRecords();
                    break;
            }
        }
        if (!prefixIterator.get().hasNext() || isLimitExhausted()) {
            flushPageBuilder();
        }
    }

    private void addColumnsRecords(QualifiedTablePrefix prefix)
    {
        for (Map.Entry<SchemaTableName, List<ColumnMetadata>> entry : listTableColumns(session, metadata, accessControl, prefix).entrySet()) {
            SchemaTableName tableName = entry.getKey();
            int ordinalPosition = 1;

            for (ColumnMetadata column : entry.getValue()) {
                if (column.isHidden()) {
                    continue;
                }
                addRecord(
                        prefix.getCatalogName(),
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        column.getName(),
                        ordinalPosition,
                        null,
                        "YES",
                        getDisplayLabel(column.getType(), isOmitDateTimeTypePrecision(session)),
                        column.getComment(),
                        column.getExtraInfo(),
                        column.getComment());
                ordinalPosition++;
                if (isLimitExhausted()) {
                    return;
                }
            }
        }
    }

    private void addTablesRecords(QualifiedTablePrefix prefix)
    {
        Set<SchemaTableName> tables = listTables(session, metadata, accessControl, prefix);
        Set<SchemaTableName> views = listViews(session, metadata, accessControl, prefix);
        // TODO (https://github.com/trinodb/trino/issues/8207) define a type for materialized views

        for (SchemaTableName name : union(tables, views)) {
            // if table and view names overlap, the view wins
            String type = views.contains(name) ? "VIEW" : "BASE TABLE";
            addRecord(
                    prefix.getCatalogName(),
                    name.getSchemaName(),
                    name.getTableName(),
                    type,
                    null);
            if (isLimitExhausted()) {
                return;
            }
        }
    }

    private void addViewsRecords(QualifiedTablePrefix prefix)
    {
        for (Map.Entry<SchemaTableName, ConnectorViewDefinition> entry : getViews(session, metadata, accessControl, prefix).entrySet()) {
            addRecord(
                    prefix.getCatalogName(),
                    entry.getKey().getSchemaName(),
                    entry.getKey().getTableName(),
                    entry.getValue().getOriginalSql());
            if (isLimitExhausted()) {
                return;
            }
        }
    }

    private void addSchemataRecords()
    {
        for (String schema : listSchemas(session, metadata, accessControl, catalogName)) {
            addRecord(catalogName, schema);
            if (isLimitExhausted()) {
                return;
            }
        }
    }

    private void addTablePrivilegesRecords(QualifiedTablePrefix prefix)
    {
        List<GrantInfo> grants = ImmutableList.copyOf(listTablePrivileges(session, metadata, accessControl, prefix));
        for (GrantInfo grant : grants) {
            addRecord(
                    grant.getGrantor().map(TrinoPrincipal::getName).orElse(null),
                    grant.getGrantor().map(principal -> principal.getType().toString()).orElse(null),
                    grant.getGrantee().getName(),
                    grant.getGrantee().getType().toString(),
                    prefix.getCatalogName(),
                    grant.getSchemaTableName().getSchemaName(),
                    grant.getSchemaTableName().getTableName(),
                    grant.getPrivilegeInfo().getPrivilege().name(),
                    grant.getPrivilegeInfo().isGrantOption() ? "YES" : "NO",
                    grant.getWithHierarchy().map(withHierarchy -> withHierarchy ? "YES" : "NO").orElse(null));
            if (isLimitExhausted()) {
                return;
            }
        }
    }

    private void addRolesRecords()
    {
        try {
            accessControl.checkCanShowRoles(session.toSecurityContext(), catalogName);
        }
        catch (AccessDeniedException exception) {
            return;
        }

        for (String role : metadata.listRoles(session, catalogName)) {
            addRecord(role);
            if (isLimitExhausted()) {
                return;
            }
        }
    }

    private void addRoleAuthorizationDescriptorRecords()
    {
        try {
            accessControl.checkCanShowRoleAuthorizationDescriptors(session.toSecurityContext(), catalogName);
        }
        catch (AccessDeniedException exception) {
            return;
        }

        for (RoleGrant grant : metadata.listAllRoleGrants(session, catalogName, roles, grantees, limit)) {
            addRecord(
                    grant.getRoleName(),
                    null, // grantor
                    null, // grantor type
                    grant.getGrantee().getName(),
                    grant.getGrantee().getType().toString(),
                    grant.isGrantable() ? "YES" : "NO");
            if (isLimitExhausted()) {
                return;
            }
        }
    }

    private void addApplicableRolesRecords()
    {
        for (RoleGrant grant : metadata.listApplicableRoles(session, new TrinoPrincipal(USER, session.getUser()), catalogName)) {
            addRecord(
                    grant.getGrantee().getName(),
                    grant.getGrantee().getType().toString(),
                    grant.getRoleName(),
                    grant.isGrantable() ? "YES" : "NO");
            if (isLimitExhausted()) {
                return;
            }
        }
    }

    private void addEnabledRolesRecords()
    {
        for (String role : metadata.listEnabledRoles(session, catalogName)) {
            addRecord(role);
            if (isLimitExhausted()) {
                return;
            }
        }
    }

    private void addRecord(Object... values)
    {
        pageBuilder.declarePosition();
        for (int i = 0; i < types.size(); i++) {
            writeNativeValue(types.get(i), pageBuilder.getBlockBuilder(i), values[i]);
        }
        if (pageBuilder.isFull()) {
            flushPageBuilder();
        }
        recordCount++;
    }

    private void flushPageBuilder()
    {
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
            memoryUsageBytes += pageBuilder.getRetainedSizeInBytes();
            pageBuilder.reset();
        }
    }

    private boolean isLimitExhausted()
    {
        return limit.isPresent() && recordCount >= limit.getAsLong();
    }
}
