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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.TableInput;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.plugin.iceberg.IcebergUtil.validateTableCanBeDropped;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toMap;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.Transactions.createTableTransaction;

public class TrinoGlueCatalog
        implements TrinoCatalog
{
    private static final Logger log = Logger.get(TrinoGlueCatalog.class);

    private final HdfsEnvironment hdfsEnvironment;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final Optional<String> defaultSchemaLocation;
    private final boolean isUniqueTableLocation;
    private final AWSGlueAsync glueClient;
    private final String catalogId;
    private final GlueMetastoreStats stats;

    private final Map<SchemaTableName, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();

    public TrinoGlueCatalog(
            HdfsEnvironment hdfsEnvironment,
            IcebergTableOperationsProvider tableOperationsProvider,
            AWSGlueAsync glueClient,
            GlueMetastoreStats stats,
            String catalogId,
            Optional<String> defaultSchemaLocation,
            boolean isUniqueTableLocation)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.catalogId = catalogId; // null is a valid catalogId, meaning the current account
        this.defaultSchemaLocation = requireNonNull(defaultSchemaLocation, "defaultSchemaLocation is null");
        this.isUniqueTableLocation = isUniqueTableLocation;
    }

    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    public String getCatalogId()
    {
        return catalogId;
    }

    public AWSGlueAsync getGlueClient()
    {
        return glueClient;
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        try {
            return stats.getGetAllDatabases().call(() -> {
                List<String> namespaces = new ArrayList<>();
                String nextToken = null;

                do {
                    GetDatabasesResult result = glueClient.getDatabases(new GetDatabasesRequest().withCatalogId(catalogId).withNextToken(nextToken));
                    nextToken = result.getNextToken();
                    result.getDatabaseList().forEach(database -> namespaces.add(database.getName()));
                }
                while (nextToken != null);

                return namespaces;
            });
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    @Override
    public boolean dropNamespace(ConnectorSession session, String namespace)
    {
        try {
            stats.getDropDatabase().call(() -> glueClient.deleteDatabase(new DeleteDatabaseRequest()
                    .withCatalogId(catalogId).withName(namespace)));
            return true;
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(namespace);
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        try {
            return stats.getGetDatabase().call(() ->
                    glueClient.getDatabase(new GetDatabaseRequest().withCatalogId(catalogId).withName(namespace))
                            .getDatabase().getParameters().entrySet().stream()
                            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(namespace);
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        return Optional.empty();
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        try {
            stats.getCreateDatabase().call(() -> glueClient.createDatabase(new CreateDatabaseRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseInput(new DatabaseInput()
                            .withName(namespace)
                            .withParameters(properties.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> e.getValue().toString()))))));
        }
        catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(namespace);
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported by Iceberg Glue catalog");
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        try {
            return stats.getGetAllTables().call(() -> {
                List<String> namespaces = namespace.isPresent() ? Lists.newArrayList(namespace.get()) : listNamespaces(session);

                List<SchemaTableName> tableNames = new ArrayList<>();
                String nextToken = null;

                for (String ns : namespaces) {
                    do {
                        GetTablesResult result = glueClient.getTables(new GetTablesRequest()
                                .withCatalogId(catalogId)
                                .withDatabaseName(ns)
                                .withNextToken(nextToken));
                        result.getTableList().stream()
                                .map(com.amazonaws.services.glue.model.Table::getName)
                                .forEach(name -> tableNames.add(new SchemaTableName(ns, name)));
                        nextToken = result.getNextToken();
                    }
                    while (nextToken != null);
                }
                return tableNames;
            });
        }
        catch (EntityNotFoundException e) {
            // database does not exist
            return ImmutableList.of();
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName table)
    {
        TableMetadata metadata = tableMetadataCache.computeIfAbsent(
                table,
                ignore -> {
                    TableOperations operations = tableOperationsProvider.createTableOperations(
                            this,
                            session,
                            table.getSchemaName(),
                            table.getTableName(),
                            Optional.empty(),
                            Optional.empty());
                    return new BaseTable(operations, quotedTableName(table)).operations().current();
                });

        IcebergTableOperations operations = tableOperationsProvider.createTableOperations(
                this,
                session,
                table.getSchemaName(),
                table.getTableName(),
                Optional.empty(),
                Optional.empty());
        operations.initializeFromMetadata(metadata);
        return new BaseTable(operations, quotedTableName(table));
    }

    @Override
    public boolean dropTable(ConnectorSession session, SchemaTableName schemaTableName, boolean purgeData)
    {
        Table table = loadTable(session, schemaTableName);
        validateTableCanBeDropped(table, schemaTableName);
        try {
            stats.getDropTable().call(() ->
                    glueClient.deleteTable(new DeleteTableRequest()
                            .withCatalogId(catalogId)
                            .withDatabaseName(schemaTableName.getSchemaName())
                            .withName(schemaTableName.getTableName())));
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }

        Path tableLocation = new Path(table.location());
        if (purgeData) {
            try {
                hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), tableLocation).delete(tableLocation, true);
            }
            catch (Exception e) {
                // don't fail if unable to delete path
                log.warn(e, "Failed to delete path: " + tableLocation);
            }
        }
        return true;
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec,
            String location, Map<String, String> properties)
    {
        TableMetadata metadata = newTableMetadata(schema, partitionSpec, location, properties);
        TableOperations ops = tableOperationsProvider.createTableOperations(
                this,
                session,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                Optional.of(session.getUser()),
                Optional.of(location));
        return createTableTransaction(schemaTableName.toString(), ops, metadata);
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        AtomicBoolean newTableCreated = new AtomicBoolean(false);
        AtomicBoolean oldTableDeleted = new AtomicBoolean(false);
        try {
            stats.getRenameTable().call(() -> {
                com.amazonaws.services.glue.model.Table table = glueClient.getTable(new GetTableRequest()
                        .withCatalogId(catalogId)
                        .withDatabaseName(from.getSchemaName())
                        .withName(from.getTableName()))
                        .getTable();

                TableInput tableInput = new TableInput()
                        .withName(to.getTableName())
                        .withTableType(table.getTableType())
                        .withOwner(table.getOwner())
                        .withParameters(table.getParameters())
                        .withDescription(table.getDescription())
                        .withTargetTable(table.getTargetTable())
                        .withLastAccessTime(table.getLastAccessTime())
                        .withLastAnalyzedTime(table.getLastAnalyzedTime())
                        .withPartitionKeys(table.getPartitionKeys())
                        .withRetention(table.getRetention())
                        .withStorageDescriptor(table.getStorageDescriptor())
                        .withViewExpandedText(table.getViewExpandedText())
                        .withViewOriginalText(table.getViewOriginalText());

                glueClient.createTable(new CreateTableRequest()
                        .withCatalogId(catalogId)
                        .withDatabaseName(to.getSchemaName())
                        .withTableInput(tableInput));
                newTableCreated.set(true);

                glueClient.deleteTable(new DeleteTableRequest()
                        .withCatalogId(catalogId)
                        .withDatabaseName(from.getSchemaName())
                        .withName(from.getTableName()));
                oldTableDeleted.set(true);
                return null;
            });
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(from);
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(to);
        }
        finally {
            if (newTableCreated.get() && !oldTableDeleted.get()) {
                glueClient.deleteTable(new DeleteTableRequest()
                        .withCatalogId(catalogId)
                        .withDatabaseName(to.getSchemaName())
                        .withName(to.getTableName()));
            }
        }
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
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
        String dbLocation = stats.getGetDatabase().call(() -> glueClient.getDatabase(new GetDatabaseRequest()
                .withCatalogId(catalogId).withName(schemaTableName.getSchemaName()))
                .getDatabase().getLocationUri());

        String location;
        if (dbLocation == null) {
            if (defaultSchemaLocation.isEmpty()) {
                throw new TrinoException(HIVE_DATABASE_LOCATION_ERROR, format("Database '%s' location cannot be determined, " +
                        "please either set 'location' when creating the database, or set 'iceberg.catalog.warehouse' " +
                        "to allow a default location at '<warehousePath>/<databaseName>.db'", schemaTableName.getSchemaName()));
            }
            location = format("%s/%s.db/%s", defaultSchemaLocation.get(), schemaTableName.getSchemaName(), schemaTableName.getTableName());
        }
        else {
            location = format("%s/%s", dbLocation, schemaTableName.getTableName());
        }

        if (isUniqueTableLocation) {
            location = location + "-" + randomUUID().toString().replace("-", "");
        }
        return location;
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTablePrincipal is not supported by Iceberg Glue catalog");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new TrinoException(NOT_SUPPORTED, "createView is not supported by Iceberg Glue catalog");
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameView is not supported by Iceberg Glue catalog");
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported by Iceberg Glue catalog");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropView is not supported by Iceberg Glue catalog");
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return ImmutableList.of();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        return ImmutableMap.of();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewIdentifier)
    {
        return Optional.empty();
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        return ImmutableList.of();
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorMaterializedViewDefinition definition,
            boolean replace, boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "createMaterializedView is not supported by Iceberg Glue catalog");
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropMaterializedView is not supported by Iceberg Glue catalog");
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        return Optional.empty();
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameMaterializedView is not supported by Iceberg Glue catalog");
    }
}
