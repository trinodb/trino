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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveSchemaProperties;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.ViewAlreadyExistsException;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveMetadata.STORAGE_TABLE;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.ViewReaderUtil.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.encodeViewData;
import static io.trino.plugin.hive.ViewReaderUtil.isHiveOrPrestoView;
import static io.trino.plugin.hive.ViewReaderUtil.isPrestoView;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoMaterializedView;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.trino.plugin.hive.util.HiveUtil.isHiveSystemSchema;
import static io.trino.plugin.hive.util.HiveWriteUtils.getTableDefaultLocation;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.encodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.fromConnectorMaterializedViewDefinition;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.getSchemaLocation;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getHiveCatalogName;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableWithMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.isIcebergTable;
import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.plugin.iceberg.IcebergUtil.validateTableCanBeDropped;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;
import static org.apache.iceberg.CatalogUtil.dropTableData;

public class TrinoHiveCatalog
        extends AbstractTrinoCatalog
{
    private static final Logger log = Logger.get(TrinoHiveCatalog.class);
    public static final String DEPENDS_ON_TABLES = "dependsOnTables";

    private final CachingHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final boolean isUsingSystemSecurity;
    private final boolean deleteSchemaLocationsFallback;

    private final Map<SchemaTableName, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();

    public TrinoHiveCatalog(
            CatalogName catalogName,
            CachingHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            String trinoVersion,
            boolean useUniqueTableLocation,
            boolean isUsingSystemSecurity,
            boolean deleteSchemaLocationsFallback)
    {
        super(catalogName, typeManager, tableOperationsProvider, trinoVersion, useUniqueTableLocation);
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.isUsingSystemSecurity = isUsingSystemSecurity;
        this.deleteSchemaLocationsFallback = deleteSchemaLocationsFallback;
    }

    public HiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        return metastore.getAllDatabases().stream()
                .filter(schemaName -> !HiveUtil.isHiveSystemSchema(schemaName))
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        Optional<Database> db = metastore.getDatabase(namespace);
        if (db.isPresent()) {
            return HiveSchemaProperties.fromDatabase(db.get());
        }

        throw new SchemaNotFoundException(namespace);
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        Optional<Database> database = metastore.getDatabase(namespace);
        if (database.isPresent()) {
            return database.flatMap(db -> db.getOwnerName().map(ownerName -> new TrinoPrincipal(db.getOwnerType().orElseThrow(), ownerName)));
        }

        throw new SchemaNotFoundException(namespace);
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        Optional<String> location = getSchemaLocation(properties).map(uri -> {
            try {
                hdfsEnvironment.getFileSystem(new HdfsContext(session), new Path(uri));
            }
            catch (IOException | IllegalArgumentException e) {
                throw new TrinoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + uri, e);
            }
            return uri;
        });

        Database database = Database.builder()
                .setDatabaseName(namespace)
                .setLocation(location)
                .setOwnerType(isUsingSystemSecurity ? Optional.empty() : Optional.of(owner.getType()))
                .setOwnerName(isUsingSystemSecurity ? Optional.empty() : Optional.of(owner.getName()))
                .build();

        metastore.createDatabase(database);
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        // basic sanity check to provide a better error message
        if (!listTables(session, Optional.of(namespace)).isEmpty()) {
            throw new TrinoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + namespace);
        }

        Optional<Path> location = metastore.getDatabase(namespace)
                .orElseThrow(() -> new SchemaNotFoundException(namespace))
                .getLocation()
                .map(Path::new);

        // If we see files in the schema location, don't delete it.
        // If we see no files, request deletion.
        // If we fail to check the schema location, behave according to fallback.
        boolean deleteData = location.map(path -> {
            HdfsContext context = new HdfsContext(session);
            try (FileSystem fs = hdfsEnvironment.getFileSystem(context, path)) {
                return !fs.listLocatedStatus(path).hasNext();
            }
            catch (IOException e) {
                log.warn(e, "Could not check schema directory '%s'", path);
                return deleteSchemaLocationsFallback;
            }
        }).orElse(deleteSchemaLocationsFallback);

        metastore.dropDatabase(namespace, deleteData);
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        metastore.renameDatabase(source, target);
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        metastore.setDatabaseOwner(namespace, HivePrincipal.from(principal));
    }

    @Override
    public Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            String location,
            Map<String, String> properties)
    {
        return newCreateTableTransaction(
                session,
                schemaTableName,
                schema,
                partitionSpec,
                location,
                properties,
                isUsingSystemSecurity ? Optional.empty() : Optional.of(session.getUser()));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        ImmutableSet.Builder<SchemaTableName> tablesListBuilder = ImmutableSet.builder();
        for (String schemaName : listNamespaces(session, namespace)) {
            metastore.getAllTables(schemaName).forEach(tableName -> tablesListBuilder.add(new SchemaTableName(schemaName, tableName)));
        }
        return tablesListBuilder.build().asList();
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        BaseTable table = (BaseTable) loadTable(session, schemaTableName);
        TableMetadata metadata = table.operations().current();
        validateTableCanBeDropped(table);

        io.trino.plugin.hive.metastore.Table metastoreTable = metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        metastore.dropTable(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                false /* do not delete data */);
        // Use the Iceberg routine for dropping the table data because the data files
        // of the Iceberg table may be located in different locations
        dropTableData(table.io(), metadata);
        deleteTableDirectory(session, schemaTableName, hdfsEnvironment, new Path(metastoreTable.getStorage().getLocation()));
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        metastore.renameTable(from.getSchemaName(), from.getTableName(), to.getSchemaName(), to.getTableName());
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableMetadata metadata = tableMetadataCache.computeIfAbsent(
                schemaTableName,
                ignore -> ((BaseTable) loadIcebergTable(this, tableOperationsProvider, session, schemaTableName)).operations().current());

        return getIcebergTableWithMetadata(this, tableOperationsProvider, session, schemaTableName, metadata);
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        metastore.commentTable(schemaTableName.getSchemaName(), schemaTableName.getTableName(), comment);
        super.updateTableComment(session, schemaTableName, comment);
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        metastore.commentColumn(schemaTableName.getSchemaName(), schemaTableName.getTableName(), columnIdentity.getName(), comment);
        super.updateColumnComment(session, schemaTableName, columnIdentity, comment);
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Database database = metastore.getDatabase(schemaTableName.getSchemaName())
                .orElseThrow(() -> new SchemaNotFoundException(schemaTableName.getSchemaName()));
        String tableNameForLocation = createNewTableName(schemaTableName.getTableName());
        return getTableDefaultLocation(database, new HdfsEnvironment.HdfsContext(session), hdfsEnvironment,
                schemaTableName.getSchemaName(), tableNameForLocation).toString();
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting an owner on a table");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        if (isUsingSystemSecurity) {
            definition = definition.withoutOwner();
        }

        io.trino.plugin.hive.metastore.Table.Builder tableBuilder = io.trino.plugin.hive.metastore.Table.builder()
                .setDatabaseName(schemaViewName.getSchemaName())
                .setTableName(schemaViewName.getTableName())
                .setOwner(isUsingSystemSecurity ? Optional.empty() : Optional.of(session.getUser()))
                .setTableType(org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW.name())
                .setDataColumns(ImmutableList.of(new Column("dummy", HIVE_STRING, Optional.empty())))
                .setPartitionColumns(ImmutableList.of())
                .setParameters(createViewProperties(session))
                .setViewOriginalText(Optional.of(encodeViewData(definition)))
                .setViewExpandedText(Optional.of(PRESTO_VIEW_EXPANDED_TEXT_MARKER));

        tableBuilder.getStorageBuilder()
                .setStorageFormat(VIEW_STORAGE_FORMAT)
                .setLocation("");
        io.trino.plugin.hive.metastore.Table table = tableBuilder.build();
        PrincipalPrivileges principalPrivileges = isUsingSystemSecurity ? NO_PRIVILEGES : buildInitialPrivilegeSet(session.getUser());

        Optional<io.trino.plugin.hive.metastore.Table> existing = metastore.getTable(schemaViewName.getSchemaName(), schemaViewName.getTableName());
        if (existing.isPresent()) {
            if (!replace || !isPrestoView(existing.get())) {
                throw new ViewAlreadyExistsException(schemaViewName);
            }

            metastore.replaceTable(schemaViewName.getSchemaName(), schemaViewName.getTableName(), table, principalPrivileges);
            return;
        }

        try {
            metastore.createTable(table, principalPrivileges);
        }
        catch (TableAlreadyExistsException e) {
            throw new ViewAlreadyExistsException(e.getTableName());
        }
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        // Not checking if source view exists as this is already done in RenameViewTask
        metastore.renameTable(source.getSchemaName(), source.getTableName(), target.getSchemaName(), target.getTableName());
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        // Not checking if view exists as this is already done in SetViewAuthorizationTask
        setTablePrincipal(session, schemaViewName, principal);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        if (getView(session, schemaViewName).isEmpty()) {
            throw new ViewNotFoundException(schemaViewName);
        }

        try {
            metastore.dropTable(schemaViewName.getSchemaName(), schemaViewName.getTableName(), true);
        }
        catch (TableNotFoundException e) {
            throw new ViewNotFoundException(e.getTableName());
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return listNamespaces(session, namespace).stream()
                .flatMap(this::listViews)
                .collect(toImmutableList());
    }

    private Stream<SchemaTableName> listViews(String schema)
    {
        // Filter on PRESTO_VIEW_COMMENT to distinguish from materialized views
        return metastore.getTablesWithParameter(schema, TABLE_COMMENT, PRESTO_VIEW_COMMENT).stream()
                .map(table -> new SchemaTableName(schema, table));
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        if (isHiveSystemSchema(viewName.getSchemaName())) {
            return Optional.empty();
        }
        return metastore.getTable(viewName.getSchemaName(), viewName.getTableName())
                .flatMap(view -> getView(
                        viewName,
                        view.getViewOriginalText(),
                        view.getTableType(),
                        view.getParameters(),
                        view.getOwner()));
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        // Filter on ICEBERG_MATERIALIZED_VIEW_COMMENT is used to avoid listing hive views in case of a shared HMS and to distinguish from standard views
        return listNamespaces(session, namespace).stream()
                .flatMap(schema -> metastore.getTablesWithParameter(schema, TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT).stream()
                        .map(table -> new SchemaTableName(schema, table)))
                .collect(toImmutableList());
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            boolean replace,
            boolean ignoreExisting)
    {
        Optional<io.trino.plugin.hive.metastore.Table> existing = metastore.getTable(viewName.getSchemaName(), viewName.getTableName());

        // It's a create command where the materialized view already exists and 'if not exists' clause is not specified
        if (!replace && existing.isPresent()) {
            if (ignoreExisting) {
                return;
            }
            throw new TrinoException(ALREADY_EXISTS, "Materialized view already exists: " + viewName);
        }

        SchemaTableName storageTable = createMaterializedViewStorageTable(session, viewName, definition);

        // Create a view indicating the storage table
        Map<String, String> viewProperties = createMaterializedViewProperties(session, storageTable.getTableName());
        Column dummyColumn = new Column("dummy", HIVE_STRING, Optional.empty());

        io.trino.plugin.hive.metastore.Table.Builder tableBuilder = io.trino.plugin.hive.metastore.Table.builder()
                .setDatabaseName(viewName.getSchemaName())
                .setTableName(viewName.getTableName())
                .setOwner(isUsingSystemSecurity ? Optional.empty() : Optional.of(session.getUser()))
                .setTableType(VIRTUAL_VIEW.name())
                .setDataColumns(ImmutableList.of(dummyColumn))
                .setPartitionColumns(ImmutableList.of())
                .setParameters(viewProperties)
                .withStorage(storage -> storage.setStorageFormat(VIEW_STORAGE_FORMAT))
                .withStorage(storage -> storage.setLocation(""))
                .setViewOriginalText(Optional.of(
                        encodeMaterializedViewData(fromConnectorMaterializedViewDefinition(definition))))
                .setViewExpandedText(Optional.of("/* " + ICEBERG_MATERIALIZED_VIEW_COMMENT + " */"));
        io.trino.plugin.hive.metastore.Table table = tableBuilder.build();
        PrincipalPrivileges principalPrivileges = isUsingSystemSecurity ? NO_PRIVILEGES : buildInitialPrivilegeSet(session.getUser());
        if (existing.isPresent() && replace) {
            // drop the current storage table
            String oldStorageTable = existing.get().getParameters().get(STORAGE_TABLE);
            if (oldStorageTable != null) {
                metastore.dropTable(viewName.getSchemaName(), oldStorageTable, true);
            }
            // Replace the existing view definition
            metastore.replaceTable(viewName.getSchemaName(), viewName.getTableName(), table, principalPrivileges);
            return;
        }
        // create the view definition
        metastore.createTable(table, principalPrivileges);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        io.trino.plugin.hive.metastore.Table view = metastore.getTable(viewName.getSchemaName(), viewName.getTableName())
                .orElseThrow(() -> new MaterializedViewNotFoundException(viewName));

        String storageTableName = view.getParameters().get(STORAGE_TABLE);
        if (storageTableName != null) {
            try {
                metastore.dropTable(viewName.getSchemaName(), storageTableName, true);
            }
            catch (TrinoException e) {
                log.warn(e, "Failed to drop storage table '%s' for materialized view '%s'", storageTableName, viewName);
            }
        }
        metastore.dropTable(viewName.getSchemaName(), viewName.getTableName(), true);
    }

    @Override
    protected Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        Optional<io.trino.plugin.hive.metastore.Table> tableOptional = metastore.getTable(viewName.getSchemaName(), viewName.getTableName());
        if (tableOptional.isEmpty()) {
            return Optional.empty();
        }

        io.trino.plugin.hive.metastore.Table table = tableOptional.get();
        if (!isTrinoMaterializedView(table.getTableType(), table.getParameters())) {
            return Optional.empty();
        }

        io.trino.plugin.hive.metastore.Table materializedView = tableOptional.get();
        String storageTable = materializedView.getParameters().get(STORAGE_TABLE);
        checkState(storageTable != null, "Storage table missing in definition of materialized view " + viewName);

        Table icebergTable;
        try {
            icebergTable = loadTable(session, new SchemaTableName(viewName.getSchemaName(), storageTable));
        }
        catch (RuntimeException e) {
            // The materialized view could be removed concurrently. This may manifest in a number of ways, e.g.
            // - io.trino.spi.connector.TableNotFoundException
            // - org.apache.iceberg.exceptions.NotFoundException when accessing manifest file
            // - other failures when reading storage table's metadata files
            // Retry, as we're catching broadly.
            metastore.invalidateTable(viewName.getSchemaName(), viewName.getTableName());
            metastore.invalidateTable(viewName.getSchemaName(), storageTable);
            throw new MaterializedViewMayBeBeingRemovedException(e);
        }
        return Optional.of(getMaterializedViewDefinition(
                viewName,
                icebergTable,
                table.getOwner(),
                materializedView.getViewOriginalText()
                        .orElseThrow(() -> new TrinoException(HIVE_INVALID_METADATA, "No view original text: " + viewName)),
                storageTable));
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        metastore.renameTable(source.getSchemaName(), source.getTableName(), target.getSchemaName(), target.getTableName());
    }

    private List<String> listNamespaces(ConnectorSession session, Optional<String> namespace)
    {
        if (namespace.isPresent()) {
            if (isHiveSystemSchema(namespace.get())) {
                return ImmutableList.of();
            }
            return ImmutableList.of(namespace.get());
        }
        return listNamespaces(session);
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        Optional<String> targetCatalogName = getHiveCatalogName(session);
        if (targetCatalogName.isEmpty()) {
            return Optional.empty();
        }
        if (isHiveSystemSchema(tableName.getSchemaName())) {
            return Optional.empty();
        }

        // we need to chop off any "$partitions" and similar suffixes from table name while querying the metastore for the Table object
        int metadataMarkerIndex = tableName.getTableName().lastIndexOf('$');
        SchemaTableName tableNameBase = (metadataMarkerIndex == -1) ? tableName : schemaTableName(
                tableName.getSchemaName(),
                tableName.getTableName().substring(0, metadataMarkerIndex));

        Optional<io.trino.plugin.hive.metastore.Table> table = metastore.getTable(tableNameBase.getSchemaName(), tableNameBase.getTableName());

        if (table.isEmpty() || isHiveOrPrestoView(table.get().getTableType())) {
            return Optional.empty();
        }
        if (!isIcebergTable(table.get())) {
            // After redirecting, use the original table name, with "$partitions" and similar suffixes
            return targetCatalogName.map(catalog -> new CatalogSchemaTableName(catalog, tableName));
        }
        return Optional.empty();
    }

    private static class MaterializedViewMayBeBeingRemovedException
            extends RuntimeException
    {
        public MaterializedViewMayBeBeingRemovedException(Throwable cause)
        {
            super(requireNonNull(cause, "cause is null"));
        }
    }
}
