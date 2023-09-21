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
package io.trino.plugin.iceberg.catalog.hadoop;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import io.trino.cache.SafeCaches;
import io.trino.hadoop.HadoopNative;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.util.PropertyUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;
import static org.apache.iceberg.CatalogUtil.loadCatalog;

public class TrinoHadoopCatalog
        extends AbstractTrinoCatalog
{
    static {
        HadoopNative.requireHadoopNative();
    }

    private static final Map<String, String> EMPTY_SESSION_MAP = ImmutableMap.of();
    private static final String CATALOG_IMPL = HadoopCatalog.class.getName();

    private final HdfsEnvironment hdfsEnvironment;
    private final Map<String, String> catalogProperties;
    private final Cache<String, Catalog> catalogCache;
    private final String warehouse;

    public TrinoHadoopCatalog(CatalogName catalogName, HdfsEnvironment hdfsEnvironment, TypeManager typeManager, IcebergTableOperationsProvider tableOperationsProvider, boolean useUniqueTableLocation, IcebergConfig config)
    {
        super(catalogName, typeManager, tableOperationsProvider, useUniqueTableLocation);
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.catalogProperties = convertToCatalogProperties(config);
        this.catalogCache = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(config.getCatalogCacheSize()));
        this.warehouse = requireNonNull(config.getCatalogWarehouse(), "warehouse is null");
    }

    /**
     * generate a unique string that represents the session information.
     * The string is used as the cache key, if the session key is the same, it means the same catalog can be reused.
     * The default behavior is to use {@link ConnectorSession#getQueryId()} as the cache key,
     * which means catalog is not shared across queries.
     * Implementations can override this method to use for example the authZ user information of the session instead.
     *
     * @param session session
     * @return session cache key
     */
    public String getSessionCacheKey(ConnectorSession session)
    {
        return session.getQueryId();
    }

    /**
     * Convert a session to a properties map that is used to initialize a new catalog
     * together with other catalog properties configured at connector level.
     * The default behavior is to return an empty map.
     * Implementations can override this method to pass session properties or session identity information to the catalog.
     *
     * @param session session
     * @return catalog properties derived from session
     */
    public Map<String, String> getSessionProperties(ConnectorSession session)
    {
        return EMPTY_SESSION_MAP;
    }

    private Catalog createNewCatalog(ConnectorSession session)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.putAll(catalogProperties);
        builder.putAll(getSessionProperties(session));
        Configuration hadoopConf = hdfsEnvironment.getConfiguration(new HdfsContext(session), new Path(warehouse));
        return loadCatalog(CATALOG_IMPL, catalogName.toString(), builder.buildOrThrow(), hadoopConf);
    }

    public Catalog getCatalog(ConnectorSession session)
    {
        try {
            return catalogCache.get(getSessionCacheKey(session), () -> createNewCatalog(session));
        }
        catch (ExecutionException e) {
            throw new IllegalStateException("Fail to create catalog for " + session, e);
        }
    }

    private SupportsNamespaces getNamespaceCatalog(ConnectorSession session)
    {
        Catalog catalog = getCatalog(session);
        if (catalog instanceof SupportsNamespaces) {
            return (SupportsNamespaces) catalog;
        }
        throw new TrinoException(NOT_SUPPORTED, "catalog " + CATALOG_IMPL + " does not support namespace operations");
    }

    private Map<String, String> convertToCatalogProperties(IcebergConfig config)
    {
        return ImmutableMap.of(WAREHOUSE_LOCATION, config.getCatalogWarehouse());
    }

    private TableIdentifier toTableId(SchemaTableName schemaTableName)
    {
        return TableIdentifier.of(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    private SchemaTableName schemaFromTableId(TableIdentifier tableIdentifier)
    {
        return new SchemaTableName(tableIdentifier.namespace().toString(), tableIdentifier.name());
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        if (!namespace.equals(namespace.toLowerCase(ENGLISH))) {
            // Currently, Trino schemas are always lowercase, so this one cannot exist (https://github.com/trinodb/trino/issues/17)
            return false;
        }
        if (listNamespaces(session).stream().filter(namespace::equals).findAny().orElse(null) != null) {
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        return getNamespaceCatalog(session).listNamespaces().stream().map(Object::toString).collect(Collectors.toList());
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        SupportsNamespaces catalog = getNamespaceCatalog(session);
        catalog.dropNamespace(catalog.listNamespaces().stream().filter(_namespace -> namespace.equals(_namespace.toString())).findAny().orElse(null));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        return (Map) getNamespaceCatalog(session).loadNamespaceMetadata(Namespace.of(namespace));
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        throw new TrinoException(NOT_SUPPORTED, "getNamespacePrincipal is not supported by " + getCatalog(session).name());
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        getNamespaceCatalog(session).createNamespace(Namespace.of(namespace), EMPTY_SESSION_MAP);
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported by " + getCatalog(session).name());
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported by " + getCatalog(session).name());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        return getCatalog(session).listTables(Namespace.of(namespace.orElseThrow())).stream().map(this::schemaFromTableId).collect(Collectors.toList());
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
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, String location, Map<String, String> properties)
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
    public void registerTable(ConnectorSession session, SchemaTableName tableName, TableMetadata tableMetadata)
    {
        getCatalog(session).registerTable(TableIdentifier.of(tableName.getSchemaName(), tableName.getTableName()), tableMetadata.metadataFileLocation());
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, String location, Map<String, String> properties, Optional<String> owner)
    {
        // Location cannot be specified for hadoop tables.
        return getCatalog(session).newCreateTableTransaction(toTableId(schemaTableName), schema, partitionSpec, null, properties);
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        dropTable(session, tableName);
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        getCatalog(session).dropTable(toTableId(schemaTableName), true);
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        getCatalog(session).dropTable(toTableId(schemaTableName), true);
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        getCatalog(session).renameTable(toTableId(from), toTableId(to));
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try {
            return getCatalog(session).loadTable(toTableId(schemaTableName));
        }
        catch (NoSuchTableException e) {
            // Have to change exception types due to code relying on specific exception to be thrown.
            throw new TableNotFoundException(schemaTableName, e);
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables)
    {
        return null;
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableIdentifier tableIdentifier = toTableId(schemaTableName);
        String dbLocationUri = PropertyUtil.propertyAsString(getNamespaceCatalog(session).loadNamespaceMetadata(tableIdentifier.namespace()), "locationUri", null);

        if (dbLocationUri != null) {
            return String.format("%s/%s", dbLocationUri, tableIdentifier.name());
        }
        else {
            return String.format("%s/%s/%s", warehouse, schemaFromTableId(tableIdentifier), tableIdentifier.name());
        }
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTablePrincipal is not supported by " + getCatalog(session).name());
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new TrinoException(NOT_SUPPORTED, "createView is not supported by " + getCatalog(session).name());
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameView is not supported by " + getCatalog(session).name());
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported by " + getCatalog(session).name());
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropView is not supported by " + getCatalog(session).name());
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return new ArrayList<>();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        return new ArrayList<>();
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateViewComment is not supported by " + getCatalog(session).name());
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateViewColumnComment is not supported by " + getCatalog(session).name());
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "createMaterializedView is not supported by " + getCatalog(session).name());
    }

    @Override
    public void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateMaterializedViewColumnComment is not supported by " + getCatalog(session).name());
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropMaterializedView is not supported by " + getCatalog(session).name());
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameMaterializedView is not supported by " + getCatalog(session).name());
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName, String catalogName)
    {
        return Optional.empty();
    }

    @Override
    protected Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        return Optional.empty();
    }
}
