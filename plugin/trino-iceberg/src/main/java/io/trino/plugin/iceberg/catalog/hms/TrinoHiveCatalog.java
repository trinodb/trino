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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HiveSchemaProperties;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.iceberg.UnknownTableTypeException;
import io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.filesystem.Locations.appendPath;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveMetadata.STORAGE_TABLE;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.ViewReaderUtil.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.encodeViewData;
import static io.trino.plugin.hive.ViewReaderUtil.isSomeKindOfAView;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoMaterializedView;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.trino.plugin.hive.util.HiveUtil.isHiveSystemSchema;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.iceberg.IcebergMaterializedViewAdditionalProperties.STORAGE_SCHEMA;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.encodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.fromConnectorMaterializedViewDefinition;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableWithMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.plugin.iceberg.IcebergUtil.validateTableCanBeDropped;
import static io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations.ICEBERG_METASTORE_STORAGE_FORMAT;
import static io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations.toHiveColumns;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.CatalogUtil.dropTableData;

public class TrinoHiveCatalog
        extends AbstractTrinoCatalog
{
    private static final Logger log = Logger.get(TrinoHiveCatalog.class);
    public static final String DEPENDS_ON_TABLES = "dependsOnTables";
    // Value should be ISO-8601 formatted time instant
    public static final String TRINO_QUERY_START_TIME = "trino-query-start-time";

    private final CachingHiveMetastore metastore;
    private final TrinoViewHiveMetastore trinoViewHiveMetastore;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final boolean isUsingSystemSecurity;
    private final boolean deleteSchemaLocationsFallback;

    private final Map<SchemaTableName, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();

    public TrinoHiveCatalog(
            CatalogName catalogName,
            CachingHiveMetastore metastore,
            TrinoViewHiveMetastore trinoViewHiveMetastore,
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            boolean useUniqueTableLocation,
            boolean isUsingSystemSecurity,
            boolean deleteSchemaLocationsFallback)
    {
        super(catalogName, typeManager, tableOperationsProvider, useUniqueTableLocation);
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.trinoViewHiveMetastore = requireNonNull(trinoViewHiveMetastore, "trinoViewHiveMetastore is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.isUsingSystemSecurity = isUsingSystemSecurity;
        this.deleteSchemaLocationsFallback = deleteSchemaLocationsFallback;
    }

    public CachingHiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        if (!namespace.equals(namespace.toLowerCase(ENGLISH))) {
            // Currently, Trino schemas are always lowercase, so this one cannot exist (https://github.com/trinodb/trino/issues/17)
            return false;
        }
        if (HiveUtil.isHiveSystemSchema(namespace)) {
            return false;
        }
        return metastore.getDatabase(namespace).isPresent();
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
        Database.Builder database = Database.builder()
                .setDatabaseName(namespace)
                .setOwnerType(isUsingSystemSecurity ? Optional.empty() : Optional.of(owner.getType()))
                .setOwnerName(isUsingSystemSecurity ? Optional.empty() : Optional.of(owner.getName()));

        properties.forEach((property, value) -> {
            switch (property) {
                case LOCATION_PROPERTY -> {
                    String location = (String) value;
                    try {
                        fileSystemFactory.create(session).directoryExists(Location.of(location));
                    }
                    catch (IOException | IllegalArgumentException e) {
                        throw new TrinoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + location, e);
                    }
                    database.setLocation(Optional.of(location));
                }
                default -> throw new IllegalArgumentException("Unrecognized property: " + property);
            }
        });

        metastore.createDatabase(database.build());
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        // basic sanity check to provide a better error message
        if (!listTables(session, Optional.of(namespace)).isEmpty()) {
            throw new TrinoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + namespace);
        }

        Optional<String> location = metastore.getDatabase(namespace)
                .orElseThrow(() -> new SchemaNotFoundException(namespace))
                .getLocation();

        // If we see files in the schema location, don't delete it.
        // If we see no files, request deletion.
        // If we fail to check the schema location, behave according to fallback.
        boolean deleteData = location.map(path -> {
            try {
                return !fileSystemFactory.create(session).listFiles(Location.of(path)).hasNext();
            }
            catch (IOException | RuntimeException e) {
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
            SortOrder sortOrder,
            String location,
            Map<String, String> properties)
    {
        return newCreateTableTransaction(
                session,
                schemaTableName,
                schema,
                partitionSpec,
                sortOrder,
                location,
                properties,
                isUsingSystemSecurity ? Optional.empty() : Optional.of(session.getUser()));
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName schemaTableName, TableMetadata tableMetadata)
            throws TrinoException
    {
        Optional<String> owner = isUsingSystemSecurity ? Optional.empty() : Optional.of(session.getUser());

        io.trino.plugin.hive.metastore.Table.Builder builder = io.trino.plugin.hive.metastore.Table.builder()
                .setDatabaseName(schemaTableName.getSchemaName())
                .setTableName(schemaTableName.getTableName())
                .setOwner(owner)
                .setDataColumns(toHiveColumns(tableMetadata.schema().columns()))
                // Table needs to be EXTERNAL, otherwise table rename in HMS would rename table directory and break table contents.
                .setTableType(EXTERNAL_TABLE.name())
                .withStorage(storage -> storage.setLocation(tableMetadata.location()))
                .withStorage(storage -> storage.setStorageFormat(ICEBERG_METASTORE_STORAGE_FORMAT))
                // This is a must-have property for the EXTERNAL_TABLE table type
                .setParameter("EXTERNAL", "TRUE")
                .setParameter(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(ENGLISH))
                .setParameter(METADATA_LOCATION_PROP, tableMetadata.metadataFileLocation());

        PrincipalPrivileges privileges = owner.map(MetastoreUtil::buildInitialPrivilegeSet).orElse(NO_PRIVILEGES);
        metastore.createTable(builder.build(), privileges);
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        dropTableFromMetastore(schemaTableName);
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
    public Optional<Iterator<RelationColumnsMetadata>> streamRelationColumns(
            ConnectorSession session,
            Optional<String> namespace,
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            Predicate<SchemaTableName> isRedirected)
    {
        return Optional.empty();
    }

    @Override
    public Optional<Iterator<RelationCommentMetadata>> streamRelationComments(
            ConnectorSession session,
            Optional<String> namespace,
            UnaryOperator<Set<SchemaTableName>> relationFilter,
            Predicate<SchemaTableName> isRedirected)
    {
        return Optional.empty();
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
        try {
            // Use the Iceberg routine for dropping the table data because the data files
            // of the Iceberg table may be located in different locations
            dropTableData(table.io(), metadata);
        }
        catch (RuntimeException e) {
            // If the snapshot file is not found, an exception will be thrown by the dropTableData function.
            // So log the exception and continue with deleting the table location
            log.warn(e, "Failed to delete table data referenced by metadata");
        }
        deleteTableDirectory(fileSystemFactory.create(session), schemaTableName, metastoreTable.getStorage().getLocation());
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        io.trino.plugin.hive.metastore.Table table = dropTableFromMetastore(schemaTableName);
        deleteTableDirectory(fileSystemFactory.create(session), schemaTableName, table.getStorage().getLocation());
    }

    private io.trino.plugin.hive.metastore.Table dropTableFromMetastore(SchemaTableName schemaTableName)
    {
        io.trino.plugin.hive.metastore.Table table = metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        if (!isIcebergTable(table)) {
            throw new UnknownTableTypeException(schemaTableName);
        }

        metastore.dropTable(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                false /* do not delete data */);
        return table;
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
    public Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables)
    {
        return ImmutableMap.of();
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        trinoViewHiveMetastore.updateViewComment(session, viewName, comment);
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        trinoViewHiveMetastore.updateViewColumnComment(session, viewName, columnName, comment);
    }

    private void replaceView(ConnectorSession session, SchemaTableName viewName, io.trino.plugin.hive.metastore.Table view, ConnectorViewDefinition newDefinition)
    {
        io.trino.plugin.hive.metastore.Table.Builder viewBuilder = io.trino.plugin.hive.metastore.Table.builder(view)
                .setViewOriginalText(Optional.of(encodeViewData(newDefinition)));

        PrincipalPrivileges principalPrivileges = isUsingSystemSecurity ? NO_PRIVILEGES : buildInitialPrivilegeSet(session.getUser());

        metastore.replaceTable(viewName.getSchemaName(), viewName.getTableName(), viewBuilder.build(), principalPrivileges);
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Database database = metastore.getDatabase(schemaTableName.getSchemaName())
                .orElseThrow(() -> new SchemaNotFoundException(schemaTableName.getSchemaName()));
        String tableNameForLocation = createNewTableName(schemaTableName.getTableName());
        String location = database.getLocation().orElseThrow(() ->
                new TrinoException(HIVE_DATABASE_LOCATION_ERROR, format("Database '%s' location is not set", schemaTableName.getSchemaName())));
        return appendPath(location, tableNameForLocation);
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting an owner on a table");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        trinoViewHiveMetastore.createView(session, schemaViewName, definition, replace);
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
        trinoViewHiveMetastore.dropView(schemaViewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return trinoViewHiveMetastore.listViews(namespace);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return trinoViewHiveMetastore.getView(viewName);
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

        if (existing.isPresent()) {
            if (!isTrinoMaterializedView(existing.get().getTableType(), existing.get().getParameters())) {
                throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Existing table is not a Materialized View: " + viewName);
            }
            if (!replace) {
                if (ignoreExisting) {
                    return;
                }
                throw new TrinoException(ALREADY_EXISTS, "Materialized view already exists: " + viewName);
            }
        }

        SchemaTableName storageTable = createMaterializedViewStorageTable(session, viewName, definition);

        // Create a view indicating the storage table
        Map<String, String> viewProperties = createMaterializedViewProperties(session, storageTable);
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
        if (existing.isPresent()) {
            // drop the current storage table
            String oldStorageTable = existing.get().getParameters().get(STORAGE_TABLE);
            if (oldStorageTable != null) {
                String storageSchema = Optional.ofNullable(existing.get().getParameters().get(STORAGE_SCHEMA))
                        .orElse(viewName.getSchemaName());
                metastore.dropTable(storageSchema, oldStorageTable, true);
            }
            // Replace the existing view definition
            metastore.replaceTable(viewName.getSchemaName(), viewName.getTableName(), table, principalPrivileges);
            return;
        }
        // create the view definition
        metastore.createTable(table, principalPrivileges);
    }

    @Override
    public void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        io.trino.plugin.hive.metastore.Table existing = metastore.getTable(viewName.getSchemaName(), viewName.getTableName())
                .orElseThrow(() -> new ViewNotFoundException(viewName));

        if (!isTrinoMaterializedView(existing.getTableType(), existing.getParameters())) {
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Existing table is not a Materialized View: " + viewName);
        }
        ConnectorMaterializedViewDefinition definition = doGetMaterializedView(session, viewName)
                .orElseThrow(() -> new ViewNotFoundException(viewName));

        ConnectorMaterializedViewDefinition newDefinition = new ConnectorMaterializedViewDefinition(
                definition.getOriginalSql(),
                definition.getStorageTable(),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns().stream()
                        .map(currentViewColumn -> Objects.equals(columnName, currentViewColumn.getName())
                                ? new ConnectorMaterializedViewDefinition.Column(currentViewColumn.getName(), currentViewColumn.getType(), comment)
                                : currentViewColumn)
                        .collect(toImmutableList()),
                definition.getGracePeriod(),
                definition.getComment(),
                definition.getOwner(),
                definition.getProperties());

        replaceMaterializedView(session, viewName, existing, newDefinition);
    }

    private void replaceMaterializedView(ConnectorSession session, SchemaTableName viewName, io.trino.plugin.hive.metastore.Table view, ConnectorMaterializedViewDefinition newDefinition)
    {
        io.trino.plugin.hive.metastore.Table.Builder viewBuilder = io.trino.plugin.hive.metastore.Table.builder(view)
                .setViewOriginalText(Optional.of(
                        encodeMaterializedViewData(fromConnectorMaterializedViewDefinition(newDefinition))));

        PrincipalPrivileges principalPrivileges = isUsingSystemSecurity ? NO_PRIVILEGES : buildInitialPrivilegeSet(session.getUser());

        metastore.replaceTable(viewName.getSchemaName(), viewName.getTableName(), viewBuilder.build(), principalPrivileges);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        io.trino.plugin.hive.metastore.Table view = metastore.getTable(viewName.getSchemaName(), viewName.getTableName())
                .orElseThrow(() -> new MaterializedViewNotFoundException(viewName));

        if (!isTrinoMaterializedView(view.getTableType(), view.getParameters())) {
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Not a Materialized View: " + viewName);
        }

        String storageTableName = view.getParameters().get(STORAGE_TABLE);
        if (storageTableName != null) {
            String storageSchema = Optional.ofNullable(view.getParameters().get(STORAGE_SCHEMA))
                    .orElse(viewName.getSchemaName());
            try {
                metastore.dropTable(storageSchema, storageTableName, true);
            }
            catch (TrinoException e) {
                log.warn(e, "Failed to drop storage table '%s.%s' for materialized view '%s'", storageSchema, storageTableName, viewName);
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
        String storageSchema = Optional.ofNullable(materializedView.getParameters().get(STORAGE_SCHEMA))
                .orElse(viewName.getSchemaName());
        SchemaTableName storageTableName = new SchemaTableName(storageSchema, storageTable);

        Table icebergTable;
        try {
            icebergTable = loadTable(session, storageTableName);
        }
        catch (RuntimeException e) {
            // The materialized view could be removed concurrently. This may manifest in a number of ways, e.g.
            // - io.trino.spi.connector.TableNotFoundException
            // - org.apache.iceberg.exceptions.NotFoundException when accessing manifest file
            // - other failures when reading storage table's metadata files
            // Retry, as we're catching broadly.
            metastore.invalidateTable(viewName.getSchemaName(), viewName.getTableName());
            metastore.invalidateTable(storageSchema, storageTable);
            throw new MaterializedViewMayBeBeingRemovedException(e);
        }
        return Optional.of(getMaterializedViewDefinition(
                icebergTable,
                table.getOwner(),
                materializedView.getViewOriginalText()
                        .orElseThrow(() -> new TrinoException(HIVE_INVALID_METADATA, "No view original text: " + viewName)),
                storageTableName));
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
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName, String hiveCatalogName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(hiveCatalogName, "hiveCatalogName is null");

        if (isHiveSystemSchema(tableName.getSchemaName())) {
            return Optional.empty();
        }

        // we need to chop off any "$partitions" and similar suffixes from table name while querying the metastore for the Table object
        int metadataMarkerIndex = tableName.getTableName().lastIndexOf('$');
        SchemaTableName tableNameBase = (metadataMarkerIndex == -1) ? tableName : schemaTableName(
                tableName.getSchemaName(),
                tableName.getTableName().substring(0, metadataMarkerIndex));

        Optional<io.trino.plugin.hive.metastore.Table> table = metastore.getTable(tableNameBase.getSchemaName(), tableNameBase.getTableName());

        if (table.isEmpty() || isSomeKindOfAView(table.get())) {
            return Optional.empty();
        }
        if (!isIcebergTable(table.get())) {
            // After redirecting, use the original table name, with "$partitions" and similar suffixes
            return Optional.of(new CatalogSchemaTableName(hiveCatalogName, tableName));
        }
        return Optional.empty();
    }
}
