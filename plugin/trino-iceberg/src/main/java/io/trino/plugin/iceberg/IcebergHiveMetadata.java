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
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveSchemaProperties;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorNewTableLayout;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveMetadata.STORAGE_TABLE;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.ViewReaderUtil.PrestoViewReader.decodeMaterializedViewData;
import static io.trino.plugin.hive.ViewReaderUtil.encodeMaterializedViewData;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.trino.plugin.hive.util.HiveWriteUtils.getTableDefaultLocation;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.getSchemaLocation;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.isIcebergTable;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.Transactions.createTableTransaction;

public class IcebergHiveMetadata
        extends IcebergMetadata
{
    private static final Logger log = Logger.get(IcebergHiveMetadata.class);
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;

    public IcebergHiveMetadata(
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec)
    {
        super(hdfsEnvironment, typeManager, commitTaskCodec);
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public org.apache.iceberg.Table getIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Optional<io.trino.plugin.hive.metastore.Table> hiveTable = metastore.getTable(new HiveIdentity(session), schemaTableName.getSchemaName(), schemaTableName.getTableName());
        if (hiveTable.isEmpty()) {
            throw new TableNotFoundException(schemaTableName);
        }
        if (!isIcebergTable(hiveTable.get())) {
            throw new UnknownTableTypeException(schemaTableName);
        }

        HdfsContext hdfsContext = new HdfsContext(session, schemaTableName.getSchemaName(), schemaTableName.getTableName());
        HiveIdentity identity = new HiveIdentity(session);
        TableOperations operations = new HiveTableOperations(metastore, hdfsEnvironment, hdfsContext, identity, schemaTableName.getSchemaName(), schemaTableName.getTableName());
        return new BaseTable(operations, quotedTableName(schemaTableName));
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases();
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, CatalogSchemaName schemaName)
    {
        Optional<Database> db = metastore.getDatabase(schemaName.getSchemaName());
        if (db.isPresent()) {
            return HiveSchemaProperties.fromDatabase(db.get());
        }

        throw new SchemaNotFoundException(schemaName.getSchemaName());
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, CatalogSchemaName schemaName)
    {
        Optional<Database> database = metastore.getDatabase(schemaName.getSchemaName());
        if (database.isPresent()) {
            return database.flatMap(db -> Optional.of(new TrinoPrincipal(db.getOwnerType(), db.getOwnerName())));
        }

        throw new SchemaNotFoundException(schemaName.getSchemaName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        List<SchemaTableName> tablesList = schemaName.map(Collections::singletonList)
                .orElseGet(metastore::getAllDatabases)
                .stream()
                .flatMap(schema -> metastore.getTablesWithParameter(schema, TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE).stream()
                        .map(table -> new SchemaTableName(schema, table))
                        .collect(toList())
                        .stream())
                .collect(toList());

        schemaName.map(Collections::singletonList)
                .orElseGet(metastore::getAllDatabases).stream()
                .flatMap(schema -> metastore.getAllViews(schema).stream()
                        .map(table -> new SchemaTableName(schema, table)))
                .forEach(schemaTableName -> tablesList.add(schemaTableName));
        return tablesList;
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        Optional<String> location = getSchemaLocation(properties).map(uri -> {
            try {
                hdfsEnvironment.getFileSystem(new HdfsContext(session, schemaName), new Path(uri));
            }
            catch (IOException | IllegalArgumentException e) {
                throw new TrinoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + uri, e);
            }
            return uri;
        });

        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setLocation(location)
                .setOwnerType(owner.getType())
                .setOwnerName(owner.getName())
                .build();

        metastore.createDatabase(new HiveIdentity(session), database);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        // basic sanity check to provide a better error message
        if (!listTables(session, Optional.of(schemaName)).isEmpty() ||
                !listViews(session, Optional.of(schemaName)).isEmpty()) {
            throw new TrinoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
        metastore.dropDatabase(new HiveIdentity(session), schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        metastore.renameDatabase(new HiveIdentity(session), source, target);
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String source, TrinoPrincipal principal)
    {
        metastore.setDatabaseOwner(new HiveIdentity(session), source, HivePrincipal.from(principal));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        Optional<ConnectorNewTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        metastore.commentTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName(), comment);
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());
        if (comment.isEmpty()) {
            icebergTable.updateProperties().remove(TABLE_COMMENT).commit();
        }
        else {
            icebergTable.updateProperties().set(TABLE_COMMENT, comment.get()).commit();
        }
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, String targetPath, ImmutableMap<String, String> newTableProperties)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Database database = metastore.getDatabase(schemaName)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName));

        HdfsContext hdfsContext = new HdfsContext(session, schemaName, tableName);
        HiveIdentity identity = new HiveIdentity(session);

        if (targetPath == null) {
            targetPath = getTableDefaultLocation(database, hdfsContext, hdfsEnvironment, schemaName, tableName).toString();
        }

        TableOperations operations = new HiveTableOperations(metastore, hdfsEnvironment, hdfsContext, identity, schemaName, tableName, session.getUser(), targetPath);
        if (operations.current() != null) {
            throw new TableAlreadyExistsException(schemaTableName);
        }

        TableMetadata metadata = newTableMetadata(schema, partitionSpec, targetPath, newTableProperties);

        return createTableTransaction(tableName, operations, metadata);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        metastore.dropTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName(), true);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        metastore.renameTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName(), newTable.getSchemaName(), newTable.getTableName());
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        HiveIdentity identity = new HiveIdentity(session);
        Optional<Table> existing = metastore.getTable(identity, viewName.getSchemaName(), viewName.getTableName());

        // It's a create command where the materialized view already exists and 'if not exists' clause is not specified
        if (!replace && existing.isPresent()) {
            if (ignoreExisting) {
                return;
            }
            throw new TrinoException(ALREADY_EXISTS, "Materialized view already exists: " + viewName);
        }

        // Generate a storage table name and create a storage table. The properties in the definition are table properties for the
        // storage table as indicated in the materialized view definition.
        String storageTableName = "st_" + UUID.randomUUID().toString().replace("-", "");
        Map<String, Object> storageTableProperties = new HashMap<>(definition.getProperties());
        storageTableProperties.putIfAbsent(FILE_FORMAT_PROPERTY, DEFAULT_FILE_FORMAT_DEFAULT);

        SchemaTableName storageTable = new SchemaTableName(viewName.getSchemaName(), storageTableName);
        List<ColumnMetadata> columns = definition.getColumns().stream()
                .map(column -> new ColumnMetadata(column.getName(), typeManager.getType(column.getType())))
                .collect(toImmutableList());

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(storageTable, columns, storageTableProperties, Optional.empty());
        Optional<ConnectorNewTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout), ImmutableList.of(), ImmutableList.of());

        // Create a view indicating the storage table
        Map<String, String> viewProperties = ImmutableMap.<String, String>builder()
                .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                .put(STORAGE_TABLE, storageTableName)
                .put(PRESTO_VIEW_FLAG, "true")
                .put(TABLE_COMMENT, "Presto Materialized View")
                .build();

        Column dummyColumn = new Column("dummy", HIVE_STRING, Optional.empty());

        String schemaName = viewName.getSchemaName();
        String tableName = viewName.getTableName();
        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(session.getUser())
                .setTableType(VIRTUAL_VIEW.name())
                .setDataColumns(ImmutableList.of(dummyColumn))
                .setPartitionColumns(ImmutableList.of())
                .setParameters(viewProperties)
                .withStorage(storage -> storage.setStorageFormat(VIEW_STORAGE_FORMAT))
                .withStorage(storage -> storage.setLocation(""))
                .setViewOriginalText(Optional.of(encodeMaterializedViewData(definition)))
                .setViewExpandedText(Optional.of("/* Presto Materialized View */"));
        Table table = tableBuilder.build();
        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(session.getUser());
        if (existing.isPresent() && replace) {
            // drop the current storage table
            String oldStorageTable = existing.get().getParameters().get(STORAGE_TABLE);
            if (oldStorageTable != null) {
                metastore.dropTable(identity, viewName.getSchemaName(), oldStorageTable, true);
            }
            // Replace the existing view definition
            metastore.replaceTable(identity, viewName.getSchemaName(), viewName.getTableName(), table, principalPrivileges);
            return;
        }
        // create the view definition
        metastore.createTable(identity, table, principalPrivileges);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        final HiveIdentity identity = new HiveIdentity(session);
        Table view = metastore.getTable(identity, viewName.getSchemaName(), viewName.getTableName())
                .orElseThrow(() -> new MaterializedViewNotFoundException(viewName));

        String storageTableName = view.getParameters().get(STORAGE_TABLE);
        if (storageTableName != null) {
            try {
                metastore.dropTable(identity, viewName.getSchemaName(), storageTableName, true);
            }
            catch (TrinoException e) {
                log.warn(e, "Failed to drop storage table '%s' for materialized view '%s'", storageTableName, viewName);
            }
        }
        metastore.dropTable(identity, viewName.getSchemaName(), viewName.getTableName(), true);
    }

    protected boolean isMaterializedView(ConnectorSession session, SchemaTableName schemaTableName)
    {
        final HiveIdentity identity = new HiveIdentity(session);
        if (metastore.getTable(identity, schemaTableName.getSchemaName(), schemaTableName.getTableName()).isPresent()) {
            Table table = metastore.getTable(identity, schemaTableName.getSchemaName(), schemaTableName.getTableName()).get();
            return isMaterializedView(table);
        }
        return false;
    }

    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        Optional<Table> materializedViewOptional = metastore.getTable(new HiveIdentity(session), viewName.getSchemaName(), viewName.getTableName());
        if (materializedViewOptional.isEmpty()) {
            return Optional.empty();
        }
        if (!isMaterializedView(session, viewName)) {
            return Optional.empty();
        }

        Table materializedView = materializedViewOptional.get();

        ConnectorMaterializedViewDefinition definition = decodeMaterializedViewData(materializedView.getViewOriginalText()
                .orElseThrow(() -> new TrinoException(HIVE_INVALID_METADATA, "No view original text: " + viewName)));

        String storageTable = materializedView.getParameters().getOrDefault(STORAGE_TABLE, "");
        return Optional.of(new ConnectorMaterializedViewDefinition(
            definition.getOriginalSql(),
            storageTable,
            definition.getCatalog(),
            Optional.of(viewName.getSchemaName()),
            definition.getColumns(),
            definition.getComment(),
            Optional.of(materializedView.getOwner()),
            new HashMap<>(materializedView.getParameters())));
    }
}
