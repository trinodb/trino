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
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.QualifiedTablePrefix;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.GrantInfo;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.type.Type;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.union;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.TABLES;
import static io.prestosql.metadata.MetadataListing.listSchemas;
import static io.prestosql.metadata.MetadataListing.listTableColumns;
import static io.prestosql.metadata.MetadataListing.listTablePrivileges;
import static io.prestosql.metadata.MetadataListing.listTables;
import static io.prestosql.metadata.MetadataListing.listViews;
import static io.prestosql.spi.security.PrincipalType.USER;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class InformationSchemaPageSource
        implements ConnectorPageSource
{
    private enum InformationSchemaTableType
    {
        TABLE_COLUMNS,
        TABLE_TABLES,
        TABLE_VIEWS,
        TABLE_SCHEMATA,
        TABLE_TABLE_PRIVILEGES,
        TABLE_ROLES,
        TABLE_APPLICABLE_ROLES,
        TABLE_ENABLED_ROLES;

        private static InformationSchemaTableType of(SchemaTableName schemaTableName)
        {
            if (schemaTableName.equals(InformationSchemaMetadata.TABLE_COLUMNS)) {
                return InformationSchemaTableType.TABLE_COLUMNS;
            }
            if (schemaTableName.equals(InformationSchemaMetadata.TABLE_TABLES)) {
                return InformationSchemaTableType.TABLE_TABLES;
            }
            if (schemaTableName.equals(InformationSchemaMetadata.TABLE_VIEWS)) {
                return InformationSchemaTableType.TABLE_VIEWS;
            }
            if (schemaTableName.equals(InformationSchemaMetadata.TABLE_SCHEMATA)) {
                return InformationSchemaTableType.TABLE_SCHEMATA;
            }
            if (schemaTableName.equals(InformationSchemaMetadata.TABLE_TABLE_PRIVILEGES)) {
                return InformationSchemaTableType.TABLE_TABLE_PRIVILEGES;
            }
            if (schemaTableName.equals(InformationSchemaMetadata.TABLE_ROLES)) {
                return InformationSchemaTableType.TABLE_ROLES;
            }
            if (schemaTableName.equals(InformationSchemaMetadata.TABLE_APPLICABLE_ROLES)) {
                return InformationSchemaTableType.TABLE_APPLICABLE_ROLES;
            }
            if (schemaTableName.equals(InformationSchemaMetadata.TABLE_ENABLED_ROLES)) {
                return InformationSchemaTableType.TABLE_ENABLED_ROLES;
            }
            throw new IllegalArgumentException("table does not exist: " + schemaTableName);
        }
    }

    private final Session session;
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final String catalogName;
    private final Optional<Iterator<String>> schemasIterator;
    private final Optional<Set<String>> tables;

    private final InformationSchemaTableType tableType;

    private final List<Type> columnTypes;
    private final Queue<Page> rawPages = new ArrayDeque<>();
    private final PageBuilder rawPageBuilder;
    private final List<Integer> outputPageChannels;

    // Used only for tables "schemata", "roles", "applicable_roles" and "enabled_roles", which are catalog-wise
    // and don't need a set of table prefixes
    private boolean noMoreBuild;

    private long completedBytes;
    private long memoryUsageBytes;
    private boolean closed;

    public InformationSchemaPageSource(
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            InformationSchemaTableHandle tableHandle,
            List<ColumnHandle> outputColumns)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");

        requireNonNull(tableHandle, "tableHandle is null");
        catalogName = tableHandle.getCatalogName();

        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();

        tableType = InformationSchemaTableType.of(schemaTableName);

        checkState(isTablesEnumeratingTable(tableType) == tableHandle.getSchemas().isPresent());

        schemasIterator = tableHandle.getSchemas().map(Set::iterator);
        tables = tableHandle.getTables();

        requireNonNull(outputColumns, "outputColumns is null");

        List<ColumnMetadata> columnMetadata = TABLES.get(schemaTableName).getColumns();
        columnTypes = columnMetadata.stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());

        rawPageBuilder = new PageBuilder(columnTypes);

        Map<String, Integer> columnNameToChannelMapping = IntStream.range(0, columnMetadata.size())
                .boxed()
                .collect(toImmutableMap(i -> columnMetadata.get(i).getName(), Function.identity()));

        outputPageChannels = outputColumns.stream()
                .map(columnHandle -> (InformationSchemaColumnHandle) columnHandle)
                .map(columnHandle -> columnNameToChannelMapping.get(columnHandle.getColumnName()))
                .collect(toImmutableList());
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
        if (isTablesEnumeratingTable(tableType)) {
            return closed || (rawPages.isEmpty() && !schemasIterator.get().hasNext());
        }
        return closed || (rawPages.isEmpty() && noMoreBuild);
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }
        if (rawPages.isEmpty()) {
            buildRawPages();
        }

        Page rawPage = rawPages.poll();
        if (rawPage == null) {
            return null;
        }
        memoryUsageBytes -= rawPage.getRetainedSizeInBytes();
        Block[] blocks = new Block[outputPageChannels.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = rawPage.getBlock(outputPageChannels.get(i));
        }
        Page outputPage = new Page(rawPage.getPositionCount(), blocks);
        completedBytes += outputPage.getSizeInBytes();
        return outputPage;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return memoryUsageBytes + rawPageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
    }

    private void buildRawPages()
    {
        if (isTablesEnumeratingTable(tableType)) {
            while (rawPages.isEmpty() && schemasIterator.get().hasNext()) {
                String schemaName = schemasIterator.get().next();
                switch (tableType) {
                    case TABLE_COLUMNS:
                        addColumnsRecordsFor(schemaName);
                        break;
                    case TABLE_TABLE_PRIVILEGES:
                        addTablePrivilegesRecordsFor(schemaName);
                        break;
                    case TABLE_TABLES:
                        addTablesRecordsFor(schemaName);
                        break;
                    case TABLE_VIEWS:
                        addViewsRecordsFor(schemaName);
                        break;
                }
            }
            // Flush the residual page
            if (!schemasIterator.get().hasNext()) {
                flushPageBuilder();
            }
            return;
        }

        if (noMoreBuild) {
            return;
        }
        switch (tableType) {
            case TABLE_SCHEMATA:
                addSchemataRecords();
                break;
            case TABLE_ROLES:
                addRolesRecords();
                break;
            case TABLE_APPLICABLE_ROLES:
                addApplicableRolesRecords();
                break;
            case TABLE_ENABLED_ROLES:
                addEnabledRolesRecords();
                break;
        }
        flushPageBuilder();
        noMoreBuild = true;
    }

    private void addSchemataRecords()
    {
        for (String schema : listSchemas(session, metadata, accessControl, catalogName)) {
            addRecord(catalogName, schema);
        }
    }

    private void addRolesRecords()
    {
        try {
            accessControl.checkCanShowRoles(session.getRequiredTransactionId(), session.getIdentity(), catalogName);
        }
        catch (AccessDeniedException exception) {
            return;
        }

        for (String role : metadata.listRoles(session, catalogName)) {
            addRecord(role);
        }
    }

    private void addApplicableRolesRecords()
    {
        for (RoleGrant grant : metadata.listApplicableRoles(session, new PrestoPrincipal(USER, session.getUser()), catalogName)) {
            PrestoPrincipal grantee = grant.getGrantee();
            addRecord(
                    grantee.getName(),
                    grantee.getType().toString(),
                    grant.getRoleName(),
                    grant.isGrantable() ? "YES" : "NO");
        }
    }

    private void addEnabledRolesRecords()
    {
        for (String role : metadata.listEnabledRoles(session, catalogName)) {
            addRecord(role);
        }
    }

    private void addColumnsRecordsFor(String schemaName)
    {
        Set<QualifiedTablePrefix> prefixes = ImmutableSet.of(new QualifiedTablePrefix(catalogName, schemaName));
        if (tables.isPresent()) {
            prefixes = tables.get().stream()
                    .map(tableName -> tableName.toLowerCase(ENGLISH))
                    .map(tableName -> new QualifiedTablePrefix(catalogName, schemaName, tableName))
                    .collect(toImmutableSet());
        }
        for (QualifiedTablePrefix prefix : prefixes) {
            for (Map.Entry<SchemaTableName, List<ColumnMetadata>> entry : listTableColumns(session, metadata, accessControl, prefix).entrySet()) {
                SchemaTableName tableName = entry.getKey();
                int ordinalPosition = 1;
                for (ColumnMetadata column : entry.getValue()) {
                    if (column.isHidden()) {
                        continue;
                    }
                    addRecord(
                            catalogName,
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
                }
            }
        }
    }

    private void addTablePrivilegesRecordsFor(String schemaName)
    {
        Set<QualifiedTablePrefix> prefixes = ImmutableSet.of(new QualifiedTablePrefix(catalogName, schemaName));
        if (tables.isPresent()) {
            prefixes = tables.get().stream()
                    .map(tableName -> tableName.toLowerCase(ENGLISH))
                    .map(tableName -> new QualifiedTablePrefix(catalogName, schemaName, tableName))
                    .collect(toImmutableSet());
        }
        for (QualifiedTablePrefix prefix : prefixes) {
            List<GrantInfo> grants = ImmutableList.copyOf(listTablePrivileges(session, metadata, accessControl, prefix));
            for (GrantInfo grant : grants) {
                addRecord(
                        grant.getGrantor().map(PrestoPrincipal::getName).orElse(null),
                        grant.getGrantor().map(principal -> principal.getType().toString()).orElse(null),
                        grant.getGrantee().getName(),
                        grant.getGrantee().getType().toString(),
                        schemaName,
                        grant.getSchemaTableName().getSchemaName(),
                        grant.getSchemaTableName().getTableName(),
                        grant.getPrivilegeInfo().getPrivilege().name(),
                        grant.getPrivilegeInfo().isGrantOption() ? "YES" : "NO",
                        grant.getWithHierarchy().map(withHierarchy -> withHierarchy ? "YES" : "NO").orElse(null));
            }
        }
    }

    private void addTablesRecordsFor(String schemaName)
    {
        if (tables.isPresent()) {
            for (String tableName : tables.get()) {
                QualifiedObjectName objectName = new QualifiedObjectName(catalogName, schemaName, tableName);
                if (metadata.getView(session, objectName).isPresent()) {
                    addRecord(
                            catalogName,
                            schemaName,
                            tableName,
                            "VIEW",
                            null);
                }
                else if (metadata.getTableHandle(session, objectName).isPresent()) {
                    addRecord(
                            catalogName,
                            schemaName,
                            tableName,
                            "BASE TABLE",
                            null);
                }
            }
            return;
        }
        QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalogName, schemaName);
        Set<SchemaTableName> tables = listTables(session, metadata, accessControl, prefix);
        Set<SchemaTableName> views = listViews(session, metadata, accessControl, prefix);

        for (SchemaTableName name : union(tables, views)) {
            // if table and view names overlap, the view wins
            String type = views.contains(name) ? "VIEW" : "BASE TABLE";
            addRecord(
                    catalogName,
                    name.getSchemaName(),
                    name.getTableName(),
                    type,
                    null);
        }
    }

    private void addViewsRecordsFor(String schemaName)
    {
        if (tables.isPresent()) {
            for (String tableName : tables.get()) {
                QualifiedObjectName objectName = new QualifiedObjectName(catalogName, schemaName, tableName);
                Optional<ConnectorViewDefinition> viewDefinition = metadata.getView(session, objectName);
                viewDefinition.ifPresent(definition ->
                        addRecord(
                                catalogName,
                                schemaName,
                                tableName,
                                definition.getOriginalSql()));
            }
            return;
        }
        for (Map.Entry<QualifiedObjectName, ConnectorViewDefinition> entry : metadata.getViews(session, new QualifiedTablePrefix(catalogName, schemaName)).entrySet()) {
            addRecord(
                    entry.getKey().getCatalogName(),
                    entry.getKey().getSchemaName(),
                    entry.getKey().getObjectName(),
                    entry.getValue().getOriginalSql());
        }
    }

    private void addRecord(Object... values)
    {
        rawPageBuilder.declarePosition();
        for (int i = 0; i < columnTypes.size(); i++) {
            writeNativeValue(columnTypes.get(i), rawPageBuilder.getBlockBuilder(i), values[i]);
        }

        if (rawPageBuilder.isFull()) {
            flushPageBuilder();
        }
    }

    private void flushPageBuilder()
    {
        if (!rawPageBuilder.isEmpty()) {
            rawPages.add(rawPageBuilder.build());
            memoryUsageBytes += rawPageBuilder.getRetainedSizeInBytes();
            rawPageBuilder.reset();
        }
    }

    private static boolean isTablesEnumeratingTable(InformationSchemaTableType tableType)
    {
        return (tableType == InformationSchemaTableType.TABLE_COLUMNS
                || tableType == InformationSchemaTableType.TABLE_TABLES
                || tableType == InformationSchemaTableType.TABLE_VIEWS
                || tableType == InformationSchemaTableType.TABLE_TABLE_PRIVILEGES);
    }
}
