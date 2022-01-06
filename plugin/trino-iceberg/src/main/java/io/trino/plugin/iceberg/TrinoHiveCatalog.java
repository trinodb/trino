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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveMetadata;
import io.trino.plugin.hive.HiveSchemaProperties;
import io.trino.plugin.hive.HiveViewNotSupportedException;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.ViewAlreadyExistsException;
import io.trino.plugin.hive.ViewReaderUtil;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
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
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.ViewReaderUtil.encodeViewData;
import static io.trino.plugin.hive.ViewReaderUtil.isHiveOrPrestoView;
import static io.trino.plugin.hive.ViewReaderUtil.isPrestoView;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.trino.plugin.hive.util.HiveUtil.isHiveSystemSchema;
import static io.trino.plugin.hive.util.HiveWriteUtils.getTableDefaultLocation;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.decodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.encodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.fromConnectorMaterializedViewDefinition;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.getSchemaLocation;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableWithMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.plugin.iceberg.PartitionFields.toPartitionFields;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.OBJECT_STORE_PATH;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;
import static org.apache.iceberg.TableProperties.WRITE_NEW_DATA_LOCATION;
import static org.apache.iceberg.Transactions.createTableTransaction;

class TrinoHiveCatalog
        implements TrinoCatalog
{
    private static final Logger log = Logger.get(TrinoHiveCatalog.class);
    private static final String ICEBERG_MATERIALIZED_VIEW_COMMENT = "Presto Materialized View";
    public static final String DEPENDS_ON_TABLES = "dependsOnTables";

    // Be compatible with views defined by the Hive connector, which can be useful under certain conditions.
    private static final String TRINO_CREATED_BY = HiveMetadata.TRINO_CREATED_BY;
    private static final String TRINO_CREATED_BY_VALUE = "Trino Iceberg connector";
    private static final String PRESTO_VIEW_COMMENT = HiveMetadata.PRESTO_VIEW_COMMENT;
    private static final String PRESTO_VERSION_NAME = HiveMetadata.PRESTO_VERSION_NAME;
    private static final String PRESTO_QUERY_ID_NAME = HiveMetadata.PRESTO_QUERY_ID_NAME;
    private static final String PRESTO_VIEW_EXPANDED_TEXT_MARKER = HiveMetadata.PRESTO_VIEW_EXPANDED_TEXT_MARKER;

    private final CatalogName catalogName;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final String trinoVersion;
    private final boolean useUniqueTableLocation;
    private final boolean isUsingSystemSecurity;
    private final boolean deleteSchemaLocationsFallback;

    private final Map<SchemaTableName, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();
    private final ViewReaderUtil.PrestoViewReader viewReader = new ViewReaderUtil.PrestoViewReader();

    public TrinoHiveCatalog(
            CatalogName catalogName,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            String trinoVersion,
            boolean useUniqueTableLocation,
            boolean isUsingSystemSecurity,
            boolean deleteSchemaLocationsFallback)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.useUniqueTableLocation = useUniqueTableLocation;
        this.isUsingSystemSecurity = isUsingSystemSecurity;
        this.deleteSchemaLocationsFallback = deleteSchemaLocationsFallback;
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

        metastore.createDatabase(new HiveIdentity(session), database);
    }

    @Override
    public boolean dropNamespace(ConnectorSession session, String namespace)
    {
        // basic sanity check to provide a better error message
        if (!listTables(session, Optional.of(namespace)).isEmpty() ||
                !listViews(session, Optional.of(namespace)).isEmpty()) {
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

        metastore.dropDatabase(new HiveIdentity(session), namespace, deleteData);
        return true;
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        metastore.renameDatabase(new HiveIdentity(session), source, target);
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        metastore.setDatabaseOwner(new HiveIdentity(session), namespace, HivePrincipal.from(principal));
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName,
            Schema schema, PartitionSpec partitionSpec, String location, Map<String, String> properties)
    {
        TableMetadata metadata = newTableMetadata(schema, partitionSpec, location, properties);
        TableOperations ops = tableOperationsProvider.createTableOperations(
                metastore,
                session,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                isUsingSystemSecurity ? Optional.empty() : Optional.of(session.getUser()),
                Optional.of(location));
        return createTableTransaction(schemaTableName.toString(), ops, metadata);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        ImmutableList.Builder<SchemaTableName> tablesListBuilder = ImmutableList.builder();
        listNamespaces(session, namespace)
                .stream()
                .flatMap(schema -> Stream.concat(
                        // Get tables with parameter table_type set to  "ICEBERG" or "iceberg". This is required because
                        // Trino uses lowercase value whereas Spark and Flink use uppercase.
                        // TODO: use one metastore call to pass both the filters: https://github.com/trinodb/trino/issues/7710
                        metastore.getTablesWithParameter(schema, TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toLowerCase(Locale.ENGLISH)).stream()
                                .map(table -> new SchemaTableName(schema, table)),
                        metastore.getTablesWithParameter(schema, TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH)).stream()
                                .map(table -> new SchemaTableName(schema, table)))
                        .distinct())  // distinct() to avoid duplicates for case-insensitive HMS backends
                .forEach(tablesListBuilder::add);

        tablesListBuilder.addAll(listMaterializedViews(session, namespace));
        return tablesListBuilder.build();
    }

    @Override
    public boolean dropTable(ConnectorSession session, SchemaTableName schemaTableName, boolean purgeData)
    {
        // TODO: support path override in Iceberg table creation: https://github.com/trinodb/trino/issues/8861
        Table table = loadTable(session, schemaTableName);
        if (table.properties().containsKey(OBJECT_STORE_PATH) ||
                table.properties().containsKey(WRITE_NEW_DATA_LOCATION) ||
                table.properties().containsKey(WRITE_METADATA_LOCATION)) {
            throw new TrinoException(NOT_SUPPORTED, "Table " + schemaTableName + " contains Iceberg path override properties and cannot be dropped from Trino");
        }
        metastore.dropTable(new HiveIdentity(session), schemaTableName.getSchemaName(), schemaTableName.getTableName(), purgeData);
        return true;
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        metastore.renameTable(new HiveIdentity(session), from.getSchemaName(), from.getTableName(), to.getSchemaName(), to.getTableName());
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableMetadata metadata = tableMetadataCache.computeIfAbsent(
                schemaTableName,
                ignore -> ((BaseTable) loadIcebergTable(metastore, tableOperationsProvider, session, schemaTableName)).operations().current());

        return getIcebergTableWithMetadata(metastore, tableOperationsProvider, session, schemaTableName, metadata);
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        metastore.commentTable(new HiveIdentity(session), schemaTableName.getSchemaName(), schemaTableName.getTableName(), comment);
        Table icebergTable = loadTable(session, schemaTableName);
        if (comment.isEmpty()) {
            icebergTable.updateProperties().remove(TABLE_COMMENT).commit();
        }
        else {
            icebergTable.updateProperties().set(TABLE_COMMENT, comment.get()).commit();
        }
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Database database = metastore.getDatabase(schemaTableName.getSchemaName())
                .orElseThrow(() -> new SchemaNotFoundException(schemaTableName.getSchemaName()));
        String tableNameForLocation = schemaTableName.getTableName();
        if (useUniqueTableLocation) {
            tableNameForLocation += "-" + randomUUID().toString().replace("-", "");
        }
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

        HiveIdentity identity = new HiveIdentity(session);
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(PRESTO_VIEW_FLAG, "true")
                .put(TRINO_CREATED_BY, TRINO_CREATED_BY_VALUE)
                .put(PRESTO_VERSION_NAME, trinoVersion)
                .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                .put(TABLE_COMMENT, PRESTO_VIEW_COMMENT)
                .build();

        io.trino.plugin.hive.metastore.Table.Builder tableBuilder = io.trino.plugin.hive.metastore.Table.builder()
                .setDatabaseName(schemaViewName.getSchemaName())
                .setTableName(schemaViewName.getTableName())
                .setOwner(isUsingSystemSecurity ? Optional.empty() : Optional.of(session.getUser()))
                .setTableType(org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW.name())
                .setDataColumns(ImmutableList.of(new Column("dummy", HIVE_STRING, Optional.empty())))
                .setPartitionColumns(ImmutableList.of())
                .setParameters(properties)
                .setViewOriginalText(Optional.of(encodeViewData(definition)))
                .setViewExpandedText(Optional.of(PRESTO_VIEW_EXPANDED_TEXT_MARKER));

        tableBuilder.getStorageBuilder()
                .setStorageFormat(VIEW_STORAGE_FORMAT)
                .setLocation("");
        io.trino.plugin.hive.metastore.Table table = tableBuilder.build();
        PrincipalPrivileges principalPrivileges = isUsingSystemSecurity ? NO_PRIVILEGES : buildInitialPrivilegeSet(session.getUser());

        Optional<io.trino.plugin.hive.metastore.Table> existing = metastore.getTable(identity, schemaViewName.getSchemaName(), schemaViewName.getTableName());
        if (existing.isPresent()) {
            if (!replace || !isPrestoView(existing.get())) {
                throw new ViewAlreadyExistsException(schemaViewName);
            }

            metastore.replaceTable(identity, schemaViewName.getSchemaName(), schemaViewName.getTableName(), table, principalPrivileges);
            return;
        }

        try {
            metastore.createTable(identity, table, principalPrivileges);
        }
        catch (TableAlreadyExistsException e) {
            throw new ViewAlreadyExistsException(e.getTableName());
        }
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        // Not checking if source view exists as this is already done in RenameViewTask
        metastore.renameTable(new HiveIdentity(session), source.getSchemaName(), source.getTableName(), target.getSchemaName(), target.getTableName());
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
            metastore.dropTable(new HiveIdentity(session), schemaViewName.getSchemaName(), schemaViewName.getTableName(), true);
        }
        catch (TableNotFoundException e) {
            throw new ViewNotFoundException(e.getTableName());
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        // Filter on PRESTO_VIEW_COMMENT to distinguish from materialized views
        return listNamespaces(session, namespace).stream()
                .flatMap(schema ->
                        metastore.getTablesWithParameter(schema, TABLE_COMMENT, PRESTO_VIEW_COMMENT).stream()
                                .map(table -> new SchemaTableName(schema, table)))
                .collect(toImmutableList());
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        for (SchemaTableName name : listViews(session, namespace)) {
            try {
                getView(session, name).ifPresent(view -> views.put(name, view));
            }
            catch (TrinoException e) {
                if (e.getErrorCode().equals(TABLE_NOT_FOUND.toErrorCode())) {
                    // Ignore view that was dropped during query execution (race condition)
                }
                else {
                    throw e;
                }
            }
        }
        return views.build();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewIdentifier)
    {
        if (isHiveSystemSchema(viewIdentifier.getSchemaName())) {
            return Optional.empty();
        }
        return metastore.getTable(new HiveIdentity(session), viewIdentifier.getSchemaName(), viewIdentifier.getTableName())
                .filter(table -> HiveMetadata.PRESTO_VIEW_COMMENT.equals(table.getParameters().get(TABLE_COMMENT))) // filter out materialized views
                .filter(ViewReaderUtil::canDecodeView)
                .map(view -> {
                    if (!isPrestoView(view)) {
                        throw new HiveViewNotSupportedException(viewIdentifier);
                    }

                    ConnectorViewDefinition definition = viewReader
                            .decodeViewData(view.getViewOriginalText().get(), view, catalogName);
                    // use owner from table metadata if it exists
                    if (view.getOwner().isPresent() && !definition.isRunAsInvoker()) {
                        definition = new ConnectorViewDefinition(
                                definition.getOriginalSql(),
                                definition.getCatalog(),
                                definition.getSchema(),
                                definition.getColumns(),
                                definition.getComment(),
                                view.getOwner(),
                                false);
                    }
                    return definition;
                });
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
    public void createMaterializedView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorMaterializedViewDefinition definition,
            boolean replace, boolean ignoreExisting)
    {
        HiveIdentity identity = new HiveIdentity(session);
        Optional<io.trino.plugin.hive.metastore.Table> existing = metastore.getTable(identity, schemaViewName.getSchemaName(), schemaViewName.getTableName());

        // It's a create command where the materialized view already exists and 'if not exists' clause is not specified
        if (!replace && existing.isPresent()) {
            if (ignoreExisting) {
                return;
            }
            throw new TrinoException(ALREADY_EXISTS, "Materialized view already exists: " + schemaViewName);
        }

        // Generate a storage table name and create a storage table. The properties in the definition are table properties for the
        // storage table as indicated in the materialized view definition.
        String storageTableName = "st_" + randomUUID().toString().replace("-", "");
        Map<String, Object> storageTableProperties = new HashMap<>(definition.getProperties());
        storageTableProperties.putIfAbsent(FILE_FORMAT_PROPERTY, DEFAULT_FILE_FORMAT_DEFAULT);

        SchemaTableName storageTable = new SchemaTableName(schemaViewName.getSchemaName(), storageTableName);
        List<ColumnMetadata> columns = definition.getColumns().stream()
                .map(column -> new ColumnMetadata(column.getName(), typeManager.getType(column.getType())))
                .collect(toImmutableList());

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(storageTable, columns, storageTableProperties, Optional.empty());
        Transaction transaction = IcebergUtil.newCreateTableTransaction(this, tableMetadata, session);
        transaction.newAppend().commit();
        transaction.commitTransaction();

        // Create a view indicating the storage table
        Map<String, String> viewProperties = ImmutableMap.<String, String>builder()
                .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                .put(STORAGE_TABLE, storageTableName)
                .put(PRESTO_VIEW_FLAG, "true")
                .put(TRINO_CREATED_BY, TRINO_CREATED_BY_VALUE)
                .put(TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT)
                .build();

        Column dummyColumn = new Column("dummy", HIVE_STRING, Optional.empty());

        io.trino.plugin.hive.metastore.Table.Builder tableBuilder = io.trino.plugin.hive.metastore.Table.builder()
                .setDatabaseName(schemaViewName.getSchemaName())
                .setTableName(schemaViewName.getTableName())
                .setOwner(isUsingSystemSecurity ? Optional.empty() : Optional.of(session.getUser()))
                .setTableType(VIRTUAL_VIEW.name())
                .setDataColumns(ImmutableList.of(dummyColumn))
                .setPartitionColumns(ImmutableList.of())
                .setParameters(viewProperties)
                .withStorage(storage -> storage.setStorageFormat(VIEW_STORAGE_FORMAT))
                .withStorage(storage -> storage.setLocation(""))
                .setViewOriginalText(Optional.of(
                        encodeMaterializedViewData(fromConnectorMaterializedViewDefinition(definition))))
                .setViewExpandedText(Optional.of("/* Presto Materialized View */"));
        io.trino.plugin.hive.metastore.Table table = tableBuilder.build();
        PrincipalPrivileges principalPrivileges = isUsingSystemSecurity ? NO_PRIVILEGES : buildInitialPrivilegeSet(session.getUser());
        if (existing.isPresent() && replace) {
            // drop the current storage table
            String oldStorageTable = existing.get().getParameters().get(STORAGE_TABLE);
            if (oldStorageTable != null) {
                metastore.dropTable(identity, schemaViewName.getSchemaName(), oldStorageTable, true);
            }
            // Replace the existing view definition
            metastore.replaceTable(identity, schemaViewName.getSchemaName(), schemaViewName.getTableName(), table, principalPrivileges);
            return;
        }
        // create the view definition
        metastore.createTable(identity, table, principalPrivileges);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        final HiveIdentity identity = new HiveIdentity(session);
        io.trino.plugin.hive.metastore.Table view = metastore.getTable(identity, schemaViewName.getSchemaName(), schemaViewName.getTableName())
                .orElseThrow(() -> new MaterializedViewNotFoundException(schemaViewName));

        String storageTableName = view.getParameters().get(STORAGE_TABLE);
        if (storageTableName != null) {
            try {
                metastore.dropTable(identity, schemaViewName.getSchemaName(), storageTableName, true);
            }
            catch (TrinoException e) {
                log.warn(e, "Failed to drop storage table '%s' for materialized view '%s'", storageTableName, schemaViewName);
            }
        }
        metastore.dropTable(identity, schemaViewName.getSchemaName(), schemaViewName.getTableName(), true);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        Optional<io.trino.plugin.hive.metastore.Table> tableOptional = metastore.getTable(new HiveIdentity(session), schemaViewName.getSchemaName(), schemaViewName.getTableName());
        if (tableOptional.isEmpty()) {
            return Optional.empty();
        }

        io.trino.plugin.hive.metastore.Table table = tableOptional.get();
        if (!isPrestoView(table) || !isHiveOrPrestoView(table) || !table.getParameters().containsKey(STORAGE_TABLE)) {
            return Optional.empty();
        }

        io.trino.plugin.hive.metastore.Table materializedView = tableOptional.get();
        String storageTable = materializedView.getParameters().get(STORAGE_TABLE);
        checkState(storageTable != null, "Storage table missing in definition of materialized view " + schemaViewName);

        IcebergMaterializedViewDefinition definition = decodeMaterializedViewData(materializedView.getViewOriginalText()
                .orElseThrow(() -> new TrinoException(HIVE_INVALID_METADATA, "No view original text: " + schemaViewName)));

        Table icebergTable = loadTable(session, new SchemaTableName(schemaViewName.getSchemaName(), storageTable));
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(FILE_FORMAT_PROPERTY, IcebergUtil.getFileFormat(icebergTable));
        if (!icebergTable.spec().fields().isEmpty()) {
            properties.put(PARTITIONING_PROPERTY, toPartitionFields(icebergTable.spec()));
        }

        return Optional.of(new ConnectorMaterializedViewDefinition(
                definition.getOriginalSql(),
                Optional.of(new CatalogSchemaTableName(catalogName.toString(), new SchemaTableName(schemaViewName.getSchemaName(), storageTable))),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns().stream()
                        .map(column -> new ConnectorMaterializedViewDefinition.Column(column.getName(), column.getType()))
                        .collect(toImmutableList()),
                definition.getComment(),
                materializedView.getOwner(),
                properties.build()));
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        metastore.renameTable(new HiveIdentity(session), source.getSchemaName(), source.getTableName(), target.getSchemaName(), target.getTableName());
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
}
