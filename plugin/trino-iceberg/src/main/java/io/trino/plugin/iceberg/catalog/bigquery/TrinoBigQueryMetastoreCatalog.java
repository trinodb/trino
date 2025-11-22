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
package io.trino.plugin.iceberg.catalog.bigquery;

import com.google.api.client.util.Maps;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ExternalCatalogDatasetOptions;
import com.google.api.services.bigquery.model.TableReference;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.TableInfo;
import io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.gcp.bigquery.BigQueryMetastoreClientImpl;
import org.apache.iceberg.util.LocationUtil;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableWithMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class TrinoBigQueryMetastoreCatalog
        extends AbstractTrinoCatalog
{
    private static final int PER_QUERY_CACHE_SIZE = 1000;

    // User provided properties.
    public static final String PROJECT_ID = "gcp.bigquery.project-id";
    public static final String GCP_LOCATION = "gcp.bigquery.location";
    public static final String LIST_ALL_TABLES = "gcp.bigquery.list-all-tables";
    public static final String WAREHOUSE = "gcp.bigquery.warehouse";

    private final BigQueryMetastoreClientImpl client;
    private final String projectID;
    private final String gcpLocation;
    private final String warehouse;
    private final boolean listAllTables;

    private final Cache<SchemaTableName, TableMetadata> tableMetadataCache = EvictableCacheBuilder.newBuilder()
            .maximumSize(PER_QUERY_CACHE_SIZE)
            .build();

    public TrinoBigQueryMetastoreCatalog(CatalogName catalogName,
                                         TypeManager typeManager,
                                         TrinoFileSystemFactory fileSystemFactory,
                                         ForwardingFileIoFactory fileIoFactory,
                                         IcebergTableOperationsProvider tableOperationsProvider,
                                         BigQueryMetastoreClientImpl bigQueryMetastoreClient,
                                         String gcpLocation,
                                         String projectID,
                                         String listAllTables,
                                         String warehouse,
                                         boolean isUniqueTableLocation)
    {
        super(catalogName, isUniqueTableLocation, typeManager, tableOperationsProvider, fileSystemFactory, fileIoFactory);
        this.client = bigQueryMetastoreClient;
        this.projectID = projectID;
        this.gcpLocation = gcpLocation;
        this.warehouse = warehouse;
        this.listAllTables = Boolean.parseBoolean(listAllTables);
    }

    @Override
    protected Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        return Optional.empty();
    }

    @Override
    protected void invalidateTableCache(SchemaTableName schemaTableName)
    {
        tableMetadataCache.invalidate(schemaTableName);
    }

    private Map<String, Object> toMetadata(Dataset dataset)
    {
        ExternalCatalogDatasetOptions options = dataset.getExternalCatalogDatasetOptions();
        Map<String, Object> metadata = Maps.newHashMap();
        if (options != null) {
            if (options.getParameters() != null) {
                metadata.putAll(options.getParameters());
            }

            if (!Strings.isNullOrEmpty(options.getDefaultStorageLocationUri())) {
                metadata.put("location", options.getDefaultStorageLocationUri());
            }
        }

        return metadata;
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        try {
            client.load(toDatasetReference(namespace));
            return true;
        }
        catch (Throwable e) {
            return false;
        }
    }

    private String toNamespaceName(DatasetList.Datasets dataset)
    {
        return dataset.getDatasetReference().getDatasetId();
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        List<DatasetList.Datasets> allDatasets = client.list(this.projectID);
        return allDatasets.stream().map(this::toNamespaceName).collect(toImmutableList());
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        try {
            client.delete(toDatasetReference(namespace));
        }
        catch (NoSuchNamespaceException e) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, e);
        }
    }

    private void validateNamespace(String namespace)
    {
        checkArgument(
                namespace.split("\\.").length == 1,
                String.format(
                        Locale.ROOT,
                        "BigQuery Metastore only supports single level namespaces. Invalid namespace: \"%s\" has %s"
                                + " levels",
                        namespace,
                        namespace.split("\\.").length));
    }

    private DatasetReference toDatasetReference(String namespace)
    {
        validateNamespace(namespace);
        return new DatasetReference().setProjectId(projectID).setDatasetId(
                Arrays.stream(namespace.split("\\.")).toList().getFirst());
    }

    private TableReference toTableReference(TableIdentifier tableIdentifier)
    {
        DatasetReference datasetReference = toDatasetReference(tableIdentifier.namespace().level(0));
        return new TableReference()
                .setProjectId(datasetReference.getProjectId())
                .setDatasetId(datasetReference.getDatasetId())
                .setTableId(tableIdentifier.name());
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        try {
            return toMetadata(client.load(toDatasetReference(namespace)));
        }
        catch (Throwable e) {
            throw new NoSuchNamespaceException("%s", e.getMessage());
        }
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        return Optional.empty();
    }

    private String createDefaultStorageLocationUri(String dbId)
    {
        checkArgument(
                warehouse != null,
                String.format(
                        "Invalid data warehouse location: %s not set", WAREHOUSE));
        return String.format("%s/%s.db", LocationUtil.stripTrailingSlash(warehouse), dbId);
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        Dataset builder = new Dataset();
        DatasetReference datasetReference = toDatasetReference(namespace);
        builder.setLocation(this.gcpLocation);
        builder.setDatasetReference(datasetReference);
        builder.setExternalCatalogDatasetOptions(
                BigQueryMetastoreIcebergUtil.createExternalCatalogDatasetOptions(
                        createDefaultStorageLocationUri(datasetReference.getDatasetId()), properties));

        client.create(builder);
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported for BigQuery Metastore");
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported for BigQuery Metastore");
    }

    private List<TableInfo> list(Optional<String> namespace, boolean allTables)
    {
        return client.list(toDatasetReference(namespace.get()), allTables).stream()
                .map(
                        table -> new TableInfo(SchemaTableName.schemaTableName(namespace.get(), table.getTableReference().getTableId()),
                                TableInfo.ExtendedRelationType.TABLE))
                .collect(toImmutableList());
    }

    @Override
    public List<TableInfo> listTables(ConnectorSession session, Optional<String> namespace)
    {
        validateNamespace(namespace.get());
        return list(namespace, listAllTables);
    }

    @Override
    public List<SchemaTableName> listIcebergTables(ConnectorSession session, Optional<String> namespace)
    {
        return list(namespace, false).stream().map(TableInfo::tableName).collect(toImmutableList());
    }

    @Override
    public Optional<Iterator<RelationColumnsMetadata>> streamRelationColumns(ConnectorSession session, Optional<String> namespace, UnaryOperator<Set<SchemaTableName>> relationFilter, Predicate<SchemaTableName> isRedirected)
    {
        return Optional.empty();
    }

    @Override
    public Optional<Iterator<RelationCommentMetadata>> streamRelationComments(ConnectorSession session, Optional<String> namespace, UnaryOperator<Set<SchemaTableName>> relationFilter, Predicate<SchemaTableName> isRedirected)
    {
        return Optional.empty();
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, Optional<String> location, Map<String, String> properties)
    {
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
    public Transaction newCreateOrReplaceTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, String location, Map<String, String> properties)
    {
        return newCreateOrReplaceTableTransaction(
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
    public void registerTable(ConnectorSession session, SchemaTableName tableName, TableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "registerTable is not supported for BigQuery Metastore.");
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "unregisterTable is not supported for BigQuery Metastore.");
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableIdentifier identifier = TableIdentifier.of(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        client.delete(toTableReference(identifier));
        invalidateTableCache(schemaTableName);
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        dropTable(session, schemaTableName);
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        throw new TrinoException(NOT_SUPPORTED, "Table rename is not supported for BigQuery Metastore");
    }

    @Override
    public BaseTable loadTable(ConnectorSession session, SchemaTableName table)
    {
        TableMetadata metadata;
        try {
            metadata = uncheckedCacheGet(
                    tableMetadataCache,
                    table,
                    () -> {
                        TableOperations operations = tableOperationsProvider.createTableOperations(
                                this,
                                session,
                                table.getSchemaName(),
                                table.getTableName(),
                                Optional.empty(),
                                Optional.empty());
                        return new BaseTable(operations, quotedTableName(table)).operations().current();
                    });
        }
        catch (UncheckedExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw e;
        }

        return getIcebergTableWithMetadata(
                this,
                tableOperationsProvider,
                session,
                table,
                metadata);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables)
    {
        return ImmutableMap.of();
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName schemaViewName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateViewComment is not supported for BigQuery Metastore");
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateViewColumnComment is not supported for BigQuery Metastore");
    }

    @Nullable
    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return String.format("%s/%s", createDefaultStorageLocationUri(schemaTableName.getSchemaName()), schemaTableName.getTableName());
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTablePrincipal is not supported for BigQuery Metastore");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new TrinoException(NOT_SUPPORTED, "createView is not supported for BigQuery Metastore");
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameView is not supported for BigQuery Metastore");
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported for BigQuery Metastore");
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropView is not supported for BigQuery Metastore");
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, Map<String, Object> materializedViewProperties, boolean replace, boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "createMaterializedView is not supported for BigQuery Metastore");
    }

    @Override
    public void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateMaterializedViewColumnComment is not supported for BigQuery Metastore");
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropMaterializedView is not supported for BigQuery Metastore");
    }

    @Override
    public Optional<BaseTable> getMaterializedViewStorageTable(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameMaterializedView is not supported for BigQuery Metastore");
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName, String hiveCatalogName)
    {
        return Optional.empty();
    }
}
