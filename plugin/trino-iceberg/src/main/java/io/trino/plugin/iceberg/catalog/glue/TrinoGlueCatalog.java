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
import com.amazonaws.services.glue.model.Database;
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
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.getPaginatedResults;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableWithMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.plugin.iceberg.IcebergUtil.validateTableCanBeDropped;
import static io.trino.plugin.iceberg.catalog.glue.GlueIcebergUtil.getTableInput;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogUtil.dropTableData;

public class TrinoGlueCatalog
        extends AbstractTrinoCatalog
{
    private final HdfsEnvironment hdfsEnvironment;
    private final Optional<String> defaultSchemaLocation;
    private final AWSGlueAsync glueClient;
    private final GlueMetastoreStats stats;

    private final Map<SchemaTableName, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();

    public TrinoGlueCatalog(
            HdfsEnvironment hdfsEnvironment,
            IcebergTableOperationsProvider tableOperationsProvider,
            AWSGlueAsync glueClient,
            GlueMetastoreStats stats,
            Optional<String> defaultSchemaLocation,
            boolean useUniqueTableLocation)
    {
        super(tableOperationsProvider, useUniqueTableLocation);
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.defaultSchemaLocation = requireNonNull(defaultSchemaLocation, "defaultSchemaLocation is null");
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        try {
            return getPaginatedResults(
                    glueClient::getDatabases,
                    new GetDatabasesRequest(),
                    GetDatabasesRequest::setNextToken,
                    GetDatabasesResult::getNextToken,
                    stats.getGetDatabases())
                    .map(GetDatabasesResult::getDatabaseList)
                    .flatMap(List::stream)
                    .map(com.amazonaws.services.glue.model.Database::getName)
                    .collect(toImmutableList());
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        try {
            stats.getDeleteDatabase().call(() ->
                    glueClient.deleteDatabase(new DeleteDatabaseRequest().withName(namespace)));
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
            GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest().withName(namespace);
            Database database = stats.getGetDatabase().call(() ->
                    glueClient.getDatabase(getDatabaseRequest).getDatabase());
            ImmutableMap.Builder<String, Object> metadata = ImmutableMap.builder();
            if (database.getLocationUri() != null) {
                metadata.put(LOCATION_PROPERTY, database.getLocationUri());
            }
            if (database.getParameters() != null) {
                metadata.putAll(database.getParameters());
            }
            return metadata.buildOrThrow();
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
        checkArgument(owner.getType() == PrincipalType.USER, "Owner type must be USER");
        checkArgument(owner.getName().equals(session.getUser()), "Explicit schema owner is not supported");

        try {
            stats.getCreateDatabase().call(() ->
                    glueClient.createDatabase(new CreateDatabaseRequest()
                            .withDatabaseInput(createDatabaseInput(namespace, properties))));
        }
        catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(namespace);
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    private DatabaseInput createDatabaseInput(String namespace, Map<String, Object> properties)
    {
        DatabaseInput databaseInput = new DatabaseInput().withName(namespace);
        Object location = properties.get(LOCATION_PROPERTY);
        if (location != null) {
            databaseInput.setLocationUri((String) location);
        }

        return databaseInput;
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported for Iceberg Glue catalogs");
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported for Iceberg Glue catalogs");
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        try {
            List<String> namespaces = namespace.map(List::of).orElseGet(() -> listNamespaces(session));
            return namespaces.stream()
                    .flatMap(glueNamespace -> {
                        try {
                            return getPaginatedResults(
                                    glueClient::getTables,
                                    new GetTablesRequest().withDatabaseName(glueNamespace),
                                    GetTablesRequest::setNextToken,
                                    GetTablesResult::getNextToken,
                                    stats.getGetTables())
                                    .map(GetTablesResult::getTableList)
                                    .flatMap(List::stream)
                                    .map(table -> new SchemaTableName(glueNamespace, table.getName()));
                        }
                        catch (EntityNotFoundException e) {
                            // Namespace may have been deleted
                            return Stream.empty();
                        }
                    })
                    .collect(toImmutableList());
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

        return getIcebergTableWithMetadata(
                this,
                tableOperationsProvider,
                session,
                table,
                metadata);
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        BaseTable table = (BaseTable) loadTable(session, schemaTableName);
        validateTableCanBeDropped(table);
        try {
            deleteTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        }
        catch (AmazonServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        dropTableData(table.io(), table.operations().current());
        deleteTableDirectory(session, schemaTableName, hdfsEnvironment, new Path(table.location()));
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
                Optional.of(session.getUser()));
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        boolean newTableCreated = false;
        try {
            GetTableRequest getTableRequest = new GetTableRequest()
                    .withDatabaseName(from.getSchemaName())
                    .withName(from.getTableName());
            com.amazonaws.services.glue.model.Table table = stats.getGetTable().call(() -> glueClient.getTable(getTableRequest).getTable());
            TableInput tableInput = getTableInput(to.getTableName(), Optional.ofNullable(table.getOwner()), table.getParameters());
            CreateTableRequest createTableRequest = new CreateTableRequest()
                    .withDatabaseName(to.getSchemaName())
                    .withTableInput(tableInput);
            stats.getCreateTable().call(() -> glueClient.createTable(createTableRequest));
            newTableCreated = true;
            deleteTable(from.getSchemaName(), from.getTableName());
        }
        catch (RuntimeException e) {
            if (newTableCreated) {
                try {
                    deleteTable(to.getSchemaName(), to.getTableName());
                }
                catch (RuntimeException cleanupException) {
                    if (!cleanupException.equals(e)) {
                        e.addSuppressed(cleanupException);
                    }
                }
            }
            throw e;
        }
    }

    private void deleteTable(String schema, String table)
    {
        stats.getDeleteTable().call(() ->
                glueClient.deleteTable(new DeleteTableRequest()
                        .withDatabaseName(schema)
                        .withName(table)));
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest()
                .withName(schemaTableName.getSchemaName());
        String databaseLocation = stats.getGetDatabase().call(() ->
                glueClient.getDatabase(getDatabaseRequest)
                        .getDatabase()
                        .getLocationUri());

        String tableName = createNewTableName(schemaTableName.getTableName());

        Path location;
        if (databaseLocation == null) {
            if (defaultSchemaLocation.isEmpty()) {
                throw new TrinoException(
                        HIVE_DATABASE_LOCATION_ERROR,
                        format("Schema '%s' location cannot be determined. " +
                                        "Either set the 'location' property when creating the schema, or set the 'hive.metastore.glue.default-warehouse-dir' " +
                                        "catalog property.",
                                schemaTableName.getSchemaName()));
            }
            String schemaDirectoryName = schemaTableName.getSchemaName() + ".db";
            location = new Path(new Path(defaultSchemaLocation.get(), schemaDirectoryName), tableName);
        }
        else {
            location = new Path(databaseLocation, tableName);
        }

        return location.toString();
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTablePrincipal is not supported for Iceberg Glue catalogs");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new TrinoException(NOT_SUPPORTED, "createView is not supported for Iceberg Glue catalogs");
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameView is not supported for Iceberg Glue catalogs");
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported for Iceberg Glue catalogs");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropView is not supported for Iceberg Glue catalogs");
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
    public void createMaterializedView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "createMaterializedView is not supported for Iceberg Glue catalogs");
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropMaterializedView is not supported for Iceberg Glue catalogs");
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        return Optional.empty();
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameMaterializedView is not supported for Iceberg Glue catalogs");
    }
}
