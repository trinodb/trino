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
import io.trino.plugin.hive.HiveSchemaProperties;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveMetadata.STORAGE_TABLE;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.trino.plugin.hive.util.HiveWriteUtils.getTableDefaultLocation;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.decodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.encodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.fromConnectorMaterializedViewDefinition;
import static io.trino.plugin.iceberg.IcebergMetadata.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.getSchemaLocation;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.fromTableId;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableWithMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.getNewCreateTableTransaction;
import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromTableId;
import static io.trino.plugin.iceberg.PartitionFields.toPartitionFields;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static java.util.Collections.emptyList;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.Transactions.createTableTransaction;

class TrinoHiveCatalog
        implements TrinoCatalog
{
    private static final Logger log = Logger.get(IcebergMetadata.class);

    private final CatalogName catalogName;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final HiveTableOperationsProvider tableOperationsProvider;

    private final Map<SchemaTableName, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();

    public TrinoHiveCatalog(
            CatalogName catalogName,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            HiveTableOperationsProvider tableOperationsProvider)
    {
        this.catalogName = catalogName;
        this.metastore = metastore;
        this.hdfsEnvironment = hdfsEnvironment;
        this.typeManager = typeManager;
        this.tableOperationsProvider = tableOperationsProvider;
    }

    @Override
    public String getName(ConnectorSession session)
    {
        return "trino-hive";
    }

    @Override
    public List<Namespace> listNamespaces(ConnectorSession session)
    {
        return metastore.getAllDatabases().stream().map(Namespace::of).collect(Collectors.toList());
    }

    @Override
    public Map<String, Object> loadNamespaceMetadataObjects(Namespace namespace, ConnectorSession session)
    {
        return metastore.getDatabase(namespace.toString())
                .map(HiveSchemaProperties::fromDatabase)
                .orElseThrow(() -> new NoSuchNamespaceException("Hive database %s not found", namespace));
    }

    @Override
    public TrinoPrincipal getNamespacePrincipal(Namespace namespace, ConnectorSession session)
    {
        return metastore.getDatabase(namespace.toString())
                .map(db -> new TrinoPrincipal(db.getOwnerType(), db.getOwnerName()))
                .orElseThrow(() -> new NoSuchNamespaceException("Hive database %s not found", namespace));
    }

    @Override
    public void createNamespaceWithPrincipal(Namespace namespace, Map<String, Object> map, TrinoPrincipal owner, ConnectorSession session)
    {
        Optional<String> location = getSchemaLocation(map).map(uri -> {
            try {
                hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), new Path(uri));
            }
            catch (IOException | IllegalArgumentException e) {
                throw new TrinoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + uri, e);
            }
            return uri;
        });

        Database database = Database.builder()
                .setDatabaseName(namespace.toString())
                .setLocation(location)
                .setOwnerType(owner.getType())
                .setOwnerName(owner.getName())
                .build();

        metastore.createDatabase(new HiveIdentity(session), database);
    }

    @Override
    public boolean dropNamespace(Namespace namespace, ConnectorSession session)
            throws NamespaceNotEmptyException
    {
        // basic sanity check to provide a better error message
        if (!listTables(namespace, session).isEmpty() || !listViews(namespace, session).isEmpty()) {
            throw new NamespaceNotEmptyException("Cannot delete namespace %s: not empty", namespace);
        }
        metastore.dropDatabase(new HiveIdentity(session), namespace.toString());
        return true;
    }

    @Override
    public void renameNamespace(Namespace source, Namespace target, ConnectorSession session)
    {
        metastore.renameDatabase(new HiveIdentity(session), source.toString(), target.toString());
    }

    @Override
    public void setNamespacePrincipal(Namespace namespace, TrinoPrincipal principal, ConnectorSession session)
    {
        metastore.setDatabaseOwner(new HiveIdentity(session), namespace.toString(), HivePrincipal.from(principal));
    }

    @Override
    public Transaction newCreateTableTransaction(TableIdentifier tableIdentifier, Schema schema, PartitionSpec partitionSpec, String location,
            Map<String, String> properties, ConnectorSession session)
    {
        TableMetadata metadata = newTableMetadata(schema, partitionSpec, location, properties);
        TableOperations ops = tableOperationsProvider.createTableOperations(
                new HdfsEnvironment.HdfsContext(session),
                session.getQueryId(),
                new HiveIdentity(session),
                schemaFromTableId(tableIdentifier),
                tableIdentifier.name(),
                Optional.of(session.getUser()),
                Optional.of(location));
        return createTableTransaction(tableIdentifier.toString(), ops, metadata);
    }

    @Override
    public Table loadTable(TableIdentifier tableIdentifier, ConnectorSession session)
    {
        SchemaTableName schemaTableName = fromTableId(tableIdentifier);
        TableMetadata metadata = tableMetadataCache.computeIfAbsent(
                schemaTableName,
                ignore -> ((BaseTable) loadIcebergTable(tableOperationsProvider, session, schemaTableName)).operations().current());

        return getIcebergTableWithMetadata(tableOperationsProvider, session, schemaTableName, metadata);
    }

    @Override
    public boolean tableExists(TableIdentifier identifier, ConnectorSession session)
    {
        SchemaTableName name = fromTableId(identifier);
        Optional<io.trino.plugin.hive.metastore.Table> table = metastore.getTable(new HiveIdentity(session), name.getSchemaName(), name.getTableName());
        if (table.isEmpty()) {
            return false;
        }
        if (isMaterializedView(table.get())) {
            return false;
        }
        if (!isIcebergTable(table.get())) {
            throw new UnknownTableTypeException(name);
        }
        return true;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace, ConnectorSession session)
    {
        ImmutableList.Builder<TableIdentifier> tablesListBuilder = ImmutableList.builder();
        Optional.ofNullable(namespace)
                .map(ns -> (List<String>) ImmutableList.of(ns.toString()))
                .orElseGet(metastore::getAllDatabases)
                .stream()
                .flatMap(schema -> Stream.concat(
                        // Get tables with parameter table_type set to  "ICEBERG" or "iceberg". This is required because
                        // Trino uses lowercase value whereas Spark and Flink use uppercase.
                        // TODO: use one metastore call to pass both the filters: https://github.com/trinodb/trino/issues/7710
                        metastore.getTablesWithParameter(schema, TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toLowerCase(Locale.ENGLISH)).stream()
                                .map(table -> TableIdentifier.of(schema, table)),
                        metastore.getTablesWithParameter(schema, TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH)).stream()
                                .map(table -> TableIdentifier.of(schema, table)))
                        .distinct())  // distinct() to avoid duplicates for case-insensitive HMS backends
                .forEach(tablesListBuilder::add);

        tablesListBuilder.addAll(listMaterializedViews(namespace, session));
        return tablesListBuilder.build();
    }

    @Override
    public void updateTableComment(TableIdentifier tableIdentifier, Optional<String> comment, ConnectorSession session)
    {
        metastore.commentTable(new HiveIdentity(session), schemaFromTableId(tableIdentifier), tableIdentifier.name(), comment);
        UpdateProperties update = loadTable(tableIdentifier, session).updateProperties();
        comment.ifPresentOrElse(c -> update.set(TABLE_COMMENT, c), () -> update.remove(TABLE_COMMENT));
        update.commit();
    }

    @Override
    public String defaultTableLocation(TableIdentifier tableIdentifier, ConnectorSession session)
    {
        String schemaName = schemaFromTableId(tableIdentifier);
        Database database = metastore.getDatabase(schemaName)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName));
        return getTableDefaultLocation(database, new HdfsEnvironment.HdfsContext(session), hdfsEnvironment, schemaName, tableIdentifier.name()).toString();
    }

    @Override
    public List<TableIdentifier> listViews(Namespace namespace, ConnectorSession session)
    {
        return emptyList();
    }

    @Override
    public List<TableIdentifier> listMaterializedViews(Namespace namespace, ConnectorSession session)
    {
        // Iceberg does not support VIEWs
        // Filter on ICEBERG_MATERIALIZED_VIEW_COMMENT is used to avoid listing hive views in case of a shared HMS
        return Optional.ofNullable(namespace)
                .map(ns -> (List<String>) ImmutableList.of(ns.toString()))
                .orElseGet(metastore::getAllDatabases).stream()
                .flatMap(schema -> metastore.getTablesWithParameter(schema, TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT).stream()
                        .map(table -> TableIdentifier.of(schema, table)))
                .collect(toImmutableList());
    }

    @Override
    public void createMaterializedView(TableIdentifier viewIdentifier, ConnectorMaterializedViewDefinition definition,
            boolean replace, boolean ignoreExisting, ConnectorSession session)
    {
        String schemaName = schemaFromTableId(viewIdentifier);
        String tableName = viewIdentifier.name();
        HiveIdentity identity = new HiveIdentity(session);
        Optional<io.trino.plugin.hive.metastore.Table> existing = metastore.getTable(identity, schemaName, tableName);
        if (!replace && existing.isPresent()) {
            if (ignoreExisting) {
                return;
            }
            throw new TrinoException(ALREADY_EXISTS, "Materialized view already exists: " + viewIdentifier);
        }

        // Generate a storage table name and create a storage table. The properties in the definition are table properties for the
        // storage table as indicated in the materialized view definition.
        String storageTableName = "st_" + UUID.randomUUID().toString().replace("-", "");
        Map<String, Object> storageTableProperties = new HashMap<>(definition.getProperties());
        storageTableProperties.putIfAbsent(FILE_FORMAT_PROPERTY, DEFAULT_FILE_FORMAT_DEFAULT);

        SchemaTableName storageTable = new SchemaTableName(schemaName, storageTableName);
        List<ColumnMetadata> columns = definition.getColumns().stream()
                .map(column -> new ColumnMetadata(column.getName(), typeManager.getType(column.getType())))
                .collect(toImmutableList());

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(storageTable, columns, storageTableProperties, Optional.empty());
        Transaction transaction = getNewCreateTableTransaction(this, tableMetadata, session);
        transaction.newAppend().commit();
        transaction.commitTransaction();

        // Create a view indicating the storage table
        Map<String, String> viewProperties = ImmutableMap.<String, String>builder()
                .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                .put(STORAGE_TABLE, storageTableName)
                .put(PRESTO_VIEW_FLAG, "true")
                .put(TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT)
                .build();

        Column dummyColumn = new Column("dummy", HIVE_STRING, Optional.empty());

        io.trino.plugin.hive.metastore.Table.Builder tableBuilder = io.trino.plugin.hive.metastore.Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(session.getUser())
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
        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(session.getUser());
        if (existing.isPresent() && replace) {
            // drop the current storage table
            String oldStorageTable = existing.get().getParameters().get(STORAGE_TABLE);
            if (oldStorageTable != null) {
                metastore.dropTable(identity, schemaName, oldStorageTable, true);
            }
            // Replace the existing view definition
            metastore.replaceTable(identity, schemaName, tableName, table, principalPrivileges);
            return;
        }
        // create the view definition
        metastore.createTable(identity, table, principalPrivileges);
    }

    @Override
    public void dropMaterializedView(TableIdentifier viewIdentifier, ConnectorSession session)
    {
        String schemaName = schemaFromTableId(viewIdentifier);
        String tableName = viewIdentifier.name();
        final HiveIdentity identity = new HiveIdentity(session);
        io.trino.plugin.hive.metastore.Table view = metastore.getTable(identity, schemaName, tableName)
                .orElseThrow(() -> new MaterializedViewNotFoundException(new SchemaTableName(schemaName, tableName)));

        String storageTableName = view.getParameters().get(STORAGE_TABLE);
        if (storageTableName != null) {
            try {
                metastore.dropTable(identity, schemaName, storageTableName, true);
            }
            catch (TrinoException e) {
                log.warn(e, "Failed to drop storage table '%s' for materialized view '%s'", storageTableName, viewIdentifier);
            }
        }
        metastore.dropTable(identity, schemaName, tableName, true);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(TableIdentifier viewIdentifier, ConnectorSession session)
    {
        String schemaName = schemaFromTableId(viewIdentifier);
        String tableName = viewIdentifier.name();
        Optional<io.trino.plugin.hive.metastore.Table> tableOptional = metastore.getTable(new HiveIdentity(session), schemaName, tableName);
        if (tableOptional.isEmpty()) {
            return Optional.empty();
        }

        if (!isMaterializedView(tableOptional.get())) {
            return Optional.empty();
        }

        io.trino.plugin.hive.metastore.Table materializedView = tableOptional.get();
        String storageTable = materializedView.getParameters().get(STORAGE_TABLE);
        checkState(storageTable != null, "Storage table missing in definition of materialized view " + viewIdentifier);

        IcebergMaterializedViewDefinition definition = decodeMaterializedViewData(materializedView.getViewOriginalText()
                .orElseThrow(() -> new TrinoException(HIVE_INVALID_METADATA, "No view original text: " + viewIdentifier)));

        Table icebergTable = loadTable(TableIdentifier.of(schemaName, storageTable), session);
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(FILE_FORMAT_PROPERTY, IcebergUtil.getFileFormat(icebergTable));
        if (!icebergTable.spec().fields().isEmpty()) {
            properties.put(PARTITIONING_PROPERTY, toPartitionFields(icebergTable.spec()));
        }

        return Optional.of(new ConnectorMaterializedViewDefinition(
                definition.getOriginalSql(),
                Optional.of(new CatalogSchemaTableName(catalogName.toString(), new SchemaTableName(schemaName, storageTable))),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns().stream()
                        .map(column -> new ConnectorMaterializedViewDefinition.Column(column.getName(), column.getType()))
                        .collect(toImmutableList()),
                definition.getComment(),
                materializedView.getOwner(),
                ImmutableMap.copyOf(properties.build())));
    }

    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean purge, ConnectorSession session)
    {
        metastore.dropTable(new HiveIdentity(session), schemaFromTableId(tableIdentifier), tableIdentifier.name(), purge);
        return true;
    }

    @Override
    public void renameTable(TableIdentifier tableIdentifier, TableIdentifier newTableIdentifier, ConnectorSession session)
    {
        metastore.renameTable(new HiveIdentity(session), schemaFromTableId(tableIdentifier), tableIdentifier.name(),
                schemaFromTableId(newTableIdentifier), newTableIdentifier.name());
    }

    private static boolean isIcebergTable(io.trino.plugin.hive.metastore.Table hiveTable)
    {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(hiveTable.getParameters().get(TABLE_TYPE_PROP));
    }

    private boolean isMaterializedView(io.trino.plugin.hive.metastore.Table hiveTable)
    {
        return hiveTable.getTableType().equals(VIRTUAL_VIEW.name())
                && "true".equals(hiveTable.getParameters().get(PRESTO_VIEW_FLAG))
                && hiveTable.getParameters().containsKey(STORAGE_TABLE);
    }
}
