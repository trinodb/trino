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
package io.trino.plugin.iceberg.catalog.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.jdbc.JdbcCatalog;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.transformValues;
import static io.trino.filesystem.Locations.appendPath;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableWithMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.plugin.iceberg.IcebergUtil.validateTableCanBeDropped;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogUtil.dropTableData;

public class TrinoJdbcCatalog
        extends AbstractTrinoCatalog
{
    private static final Logger LOG = Logger.get(TrinoJdbcCatalog.class);

    private final JdbcCatalog jdbcCatalog;
    private final IcebergJdbcClient jdbcClient;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final String defaultWarehouseDir;
    private final Map<SchemaTableName, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();

    public TrinoJdbcCatalog(
            CatalogName catalogName,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            JdbcCatalog jdbcCatalog,
            IcebergJdbcClient jdbcClient,
            TrinoFileSystemFactory fileSystemFactory,
            boolean useUniqueTableLocation,
            String defaultWarehouseDir)
    {
        super(catalogName, typeManager, tableOperationsProvider, useUniqueTableLocation);
        this.jdbcCatalog = requireNonNull(jdbcCatalog, "jdbcCatalog is null");
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.defaultWarehouseDir = requireNonNull(defaultWarehouseDir, "defaultWarehouseDir is null");
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        return jdbcCatalog.namespaceExists(Namespace.of(namespace));
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        return jdbcCatalog.listNamespaces().stream()
                .map(namespace -> namespace.level(0).toLowerCase(ENGLISH))
                .collect(toImmutableList());
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        return jdbcCatalog.loadNamespaceMetadata(Namespace.of(namespace)).entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
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
            jdbcCatalog.createNamespace(Namespace.of(namespace), transformValues(properties, String.class::cast));
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to create a namespace " + namespace, e);
        }
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        jdbcCatalog.dropNamespace(Namespace.of(namespace));
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        List<String> namespaces = listNamespaces(session, namespace);
        // Build as a set and convert to list for removing duplicate entries due to case difference
        ImmutableSet.Builder<SchemaTableName> tablesListBuilder = ImmutableSet.builder();
        for (String schemaName : namespaces) {
            try {
                jdbcCatalog.listTables(Namespace.of(schemaName)).forEach(table -> tablesListBuilder.add(new SchemaTableName(schemaName, table.name())));
            }
            catch (NoSuchNamespaceException e) {
                // Namespace may have been deleted
            }
        }
        return tablesListBuilder.build().asList();
    }

    private List<String> listNamespaces(ConnectorSession session, Optional<String> namespace)
    {
        if (namespace.isPresent() && namespaceExists(session, namespace.get())) {
            return ImmutableList.of(namespace.get());
        }
        return listNamespaces(session);
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
        if (!listNamespaces(session, Optional.of(schemaTableName.getSchemaName())).contains(schemaTableName.getSchemaName())) {
            throw new SchemaNotFoundException(schemaTableName.getSchemaName());
        }
        return newCreateTableTransaction(
                session,
                schemaTableName,
                schema,
                partitionSpec,
                sortOrder,
                location,
                properties,
                Optional.of(session.getUser()));
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName tableName, String tableLocation, String metadataLocation)
    {
        // Using IcebergJdbcClient because JdbcCatalog.registerTable causes the below error.
        // "Cannot invoke "org.apache.iceberg.util.SerializableSupplier.get()" because "this.hadoopConf" is null"
        jdbcClient.createTable(tableName.getSchemaName(), tableName.getTableName(), metadataLocation);
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        if (!jdbcCatalog.dropTable(toIdentifier(tableName), false)) {
            throw new TableNotFoundException(tableName);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        BaseTable table = (BaseTable) loadTable(session, schemaTableName);
        validateTableCanBeDropped(table);

        jdbcCatalog.dropTable(toIdentifier(schemaTableName), false);
        try {
            dropTableData(table.io(), table.operations().current());
        }
        catch (RuntimeException e) {
            // If the snapshot file is not found, an exception will be thrown by the dropTableData function.
            // So log the exception and continue with deleting the table location
            LOG.warn(e, "Failed to delete table data referenced by metadata");
        }
        deleteTableDirectory(fileSystemFactory.create(session), schemaTableName, table.location());
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Optional<String> metadataLocation = jdbcClient.getMetadataLocation(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        if (!jdbcCatalog.dropTable(toIdentifier(schemaTableName), false)) {
            throw new TableNotFoundException(schemaTableName);
        }
        if (metadataLocation.isEmpty()) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Could not find metadata_location for table %s", schemaTableName));
        }
        String tableLocation = metadataLocation.get().replaceFirst("/metadata/[^/]*$", "");
        deleteTableDirectory(fileSystemFactory.create(session), schemaTableName, tableLocation);
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        try {
            jdbcCatalog.renameTable(toIdentifier(from), toIdentifier(to));
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Failed to rename table from %s to %s".formatted(from, to), e);
        }
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
    public void updateViewComment(ConnectorSession session, SchemaTableName schemaViewName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateViewComment is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateViewColumnComment is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Namespace namespace = Namespace.of(schemaTableName.getSchemaName());
        String tableName = createNewTableName(schemaTableName.getTableName());

        Optional<String> databaseLocation = Optional.empty();
        if (jdbcCatalog.namespaceExists(namespace)) {
            databaseLocation = Optional.ofNullable(jdbcCatalog.loadNamespaceMetadata(namespace).get(LOCATION_PROPERTY));
        }

        String schemaLocation = databaseLocation.orElseGet(() ->
                appendPath(defaultWarehouseDir, schemaTableName.getSchemaName()));

        return appendPath(schemaLocation, tableName);
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTablePrincipal is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new TrinoException(NOT_SUPPORTED, "createView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropView is not supported for Iceberg JDBC catalogs");
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
    protected Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        return Optional.empty();
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "createMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        return Optional.empty();
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        return Optional.empty();
    }

    private static TableIdentifier toIdentifier(SchemaTableName table)
    {
        return TableIdentifier.of(table.getSchemaName(), table.getTableName());
    }
}
