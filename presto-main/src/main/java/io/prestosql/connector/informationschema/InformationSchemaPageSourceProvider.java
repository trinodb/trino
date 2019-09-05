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
package io.prestosql.connector.informationschema;

import com.google.common.collect.ImmutableList;
import io.prestosql.FullConnectorSession;
import io.prestosql.Session;
import io.prestosql.metadata.InternalTable;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.QualifiedTablePrefix;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.GrantInfo;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.RoleGrant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.Sets.union;
import static io.prestosql.connector.informationschema.InformationSchemaTable.APPLICABLE_ROLES;
import static io.prestosql.connector.informationschema.InformationSchemaTable.COLUMNS;
import static io.prestosql.connector.informationschema.InformationSchemaTable.ENABLED_ROLES;
import static io.prestosql.connector.informationschema.InformationSchemaTable.ROLES;
import static io.prestosql.connector.informationschema.InformationSchemaTable.SCHEMATA;
import static io.prestosql.connector.informationschema.InformationSchemaTable.TABLES;
import static io.prestosql.connector.informationschema.InformationSchemaTable.TABLE_PRIVILEGES;
import static io.prestosql.connector.informationschema.InformationSchemaTable.VIEWS;
import static io.prestosql.metadata.MetadataListing.listSchemas;
import static io.prestosql.metadata.MetadataListing.listTableColumns;
import static io.prestosql.metadata.MetadataListing.listTablePrivileges;
import static io.prestosql.metadata.MetadataListing.listTables;
import static io.prestosql.metadata.MetadataListing.listViews;
import static io.prestosql.spi.security.PrincipalType.USER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class InformationSchemaPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    public InformationSchemaPageSourceProvider(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns)
    {
        InternalTable table = getInternalTable(session, tableHandle);

        List<Integer> channels = new ArrayList<>();
        for (ColumnHandle column : columns) {
            String columnName = ((InformationSchemaColumnHandle) column).getColumnName();
            int columnIndex = table.getColumnIndex(columnName);
            channels.add(columnIndex);
        }

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        for (Page page : table.getPages()) {
            Block[] blocks = new Block[channels.size()];
            for (int index = 0; index < blocks.length; index++) {
                blocks[index] = page.getBlock(channels.get(index));
            }
            pages.add(new Page(page.getPositionCount(), blocks));
        }
        return new FixedPageSource(pages.build());
    }

    private InternalTable getInternalTable(ConnectorSession connectorSession, ConnectorTableHandle tablehandle)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        InformationSchemaTableHandle handle = (InformationSchemaTableHandle) tablehandle;
        Set<QualifiedTablePrefix> prefixes = handle.getPrefixes();

        return getInformationSchemaTable(session, handle.getCatalogName(), handle.getTable(), prefixes, handle.getLimit());
    }

    public InternalTable getInformationSchemaTable(Session session, String catalog, InformationSchemaTable table, Set<QualifiedTablePrefix> prefixes, OptionalLong limit)
    {
        switch (table) {
            case COLUMNS:
                return buildColumns(session, prefixes, limit);
            case TABLES:
                return buildTables(session, prefixes, limit);
            case VIEWS:
                return buildViews(session, prefixes, limit);
            case SCHEMATA:
                return buildSchemata(session, catalog, limit);
            case TABLE_PRIVILEGES:
                return buildTablePrivileges(session, prefixes, limit);
            case ROLES:
                return buildRoles(session, catalog, limit);
            case APPLICABLE_ROLES:
                return buildApplicableRoles(session, catalog, limit);
            case ENABLED_ROLES:
                return buildEnabledRoles(session, catalog, limit);
        }
        throw new IllegalArgumentException(format("table does not exist: %s", table.getSchemaTableName()));
    }

    private InternalTable buildColumns(Session session, Set<QualifiedTablePrefix> prefixes, OptionalLong limit)
    {
        InternalTable.Builder table = InternalTable.builder(COLUMNS.getTableMetadata().getColumns());
        for (QualifiedTablePrefix prefix : prefixes) {
            for (Entry<SchemaTableName, List<ColumnMetadata>> entry : listTableColumns(session, metadata, accessControl, prefix).entrySet()) {
                SchemaTableName tableName = entry.getKey();
                int ordinalPosition = 1;
                for (ColumnMetadata column : entry.getValue()) {
                    if (column.isHidden()) {
                        continue;
                    }
                    table.add(
                            prefix.getCatalogName(),
                            tableName.getSchemaName(),
                            tableName.getTableName(),
                            column.getName(),
                            ordinalPosition,
                            null,
                            "YES",
                            column.getType().getDisplayName(),
                            column.getComment(),
                            column.getExtraInfo(),
                            column.getComment());
                    ordinalPosition++;
                    if (table.atLimit(limit)) {
                        return table.build();
                    }
                }
            }
        }
        return table.build();
    }

    private InternalTable buildTables(Session session, Set<QualifiedTablePrefix> prefixes, OptionalLong limit)
    {
        InternalTable.Builder table = InternalTable.builder(TABLES.getTableMetadata().getColumns());
        for (QualifiedTablePrefix prefix : prefixes) {
            Set<SchemaTableName> tables = listTables(session, metadata, accessControl, prefix);
            Set<SchemaTableName> views = listViews(session, metadata, accessControl, prefix);

            for (SchemaTableName name : union(tables, views)) {
                // if table and view names overlap, the view wins
                String type = views.contains(name) ? "VIEW" : "BASE TABLE";
                table.add(
                        prefix.getCatalogName(),
                        name.getSchemaName(),
                        name.getTableName(),
                        type,
                        null);
                if (table.atLimit(limit)) {
                    return table.build();
                }
            }
        }
        return table.build();
    }

    private InternalTable buildTablePrivileges(Session session, Set<QualifiedTablePrefix> prefixes, OptionalLong limit)
    {
        InternalTable.Builder table = InternalTable.builder(TABLE_PRIVILEGES.getTableMetadata().getColumns());
        for (QualifiedTablePrefix prefix : prefixes) {
            List<GrantInfo> grants = ImmutableList.copyOf(listTablePrivileges(session, metadata, accessControl, prefix));
            for (GrantInfo grant : grants) {
                table.add(
                        grant.getGrantor().map(PrestoPrincipal::getName).orElse(null),
                        grant.getGrantor().map(principal -> principal.getType().toString()).orElse(null),
                        grant.getGrantee().getName(),
                        grant.getGrantee().getType().toString(),
                        prefix.getCatalogName(),
                        grant.getSchemaTableName().getSchemaName(),
                        grant.getSchemaTableName().getTableName(),
                        grant.getPrivilegeInfo().getPrivilege().name(),
                        grant.getPrivilegeInfo().isGrantOption() ? "YES" : "NO",
                        grant.getWithHierarchy().map(withHierarchy -> withHierarchy ? "YES" : "NO").orElse(null));
                if (table.atLimit(limit)) {
                    return table.build();
                }
            }
        }
        return table.build();
    }

    private InternalTable buildViews(Session session, Set<QualifiedTablePrefix> prefixes, OptionalLong limit)
    {
        InternalTable.Builder table = InternalTable.builder(VIEWS.getTableMetadata().getColumns());
        for (QualifiedTablePrefix prefix : prefixes) {
            for (Entry<QualifiedObjectName, ConnectorViewDefinition> entry : metadata.getViews(session, prefix).entrySet()) {
                table.add(
                        entry.getKey().getCatalogName(),
                        entry.getKey().getSchemaName(),
                        entry.getKey().getObjectName(),
                        entry.getValue().getOriginalSql());
                if (table.atLimit(limit)) {
                    return table.build();
                }
            }
        }
        return table.build();
    }

    private InternalTable buildSchemata(Session session, String catalogName, OptionalLong limit)
    {
        InternalTable.Builder table = InternalTable.builder(SCHEMATA.getTableMetadata().getColumns());
        for (String schema : listSchemas(session, metadata, accessControl, catalogName)) {
            table.add(catalogName, schema);
            if (table.atLimit(limit)) {
                return table.build();
            }
        }
        return table.build();
    }

    private InternalTable buildRoles(Session session, String catalog, OptionalLong limit)
    {
        InternalTable.Builder table = InternalTable.builder(ROLES.getTableMetadata().getColumns());

        try {
            accessControl.checkCanShowRoles(session.toSecurityContext(), catalog);
        }
        catch (AccessDeniedException exception) {
            return table.build();
        }

        for (String role : metadata.listRoles(session, catalog)) {
            table.add(role);
            if (table.atLimit(limit)) {
                return table.build();
            }
        }
        return table.build();
    }

    private InternalTable buildApplicableRoles(Session session, String catalog, OptionalLong limit)
    {
        InternalTable.Builder table = InternalTable.builder(APPLICABLE_ROLES.getTableMetadata().getColumns());
        for (RoleGrant grant : metadata.listApplicableRoles(session, new PrestoPrincipal(USER, session.getUser()), catalog)) {
            PrestoPrincipal grantee = grant.getGrantee();
            table.add(
                    grantee.getName(),
                    grantee.getType().toString(),
                    grant.getRoleName(),
                    grant.isGrantable() ? "YES" : "NO");
            if (table.atLimit(limit)) {
                return table.build();
            }
        }
        return table.build();
    }

    private InternalTable buildEnabledRoles(Session session, String catalog, OptionalLong limit)
    {
        InternalTable.Builder table = InternalTable.builder(ENABLED_ROLES.getTableMetadata().getColumns());
        for (String role : metadata.listEnabledRoles(session, catalog)) {
            table.add(role);
            if (table.atLimit(limit)) {
                return table.build();
            }
        }
        return table.build();
    }
}
