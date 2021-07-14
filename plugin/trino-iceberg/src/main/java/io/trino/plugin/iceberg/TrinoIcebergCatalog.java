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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.util.PropertyUtil;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.iceberg.IcebergConfig.convertToCatalogProperties;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromTableId;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogUtil.loadCatalog;

public class TrinoIcebergCatalog
        implements TrinoCatalog
{
    private static final Map<String, String> EMPTY_SESSION_MAP = ImmutableMap.of();

    private final String catalogImpl;
    private final String catalogName;
    private final Map<String, String> catalogProperties;
    private final String warehouse;
    private final HdfsEnvironment hdfsEnvironment;
    private final Cache<String, Catalog> catalogCache;

    public TrinoIcebergCatalog(String catalogImpl, String catalogName, IcebergConfig config, HdfsEnvironment hdfsEnvironment)
    {
        this.catalogImpl = requireNonNull(catalogImpl, "catalogImpl is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");
        this.catalogProperties = convertToCatalogProperties(config);
        this.warehouse = requireNonNull(config.getCatalogWarehouse(), "warehouse is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.catalogCache = CacheBuilder.newBuilder().maximumSize(config.getCatalogCacheSize()).build();
    }

    /**
     * generate a unique string that represents the session.
     * The string is used as the cache key, if the session key is the same, it means the same catalog can be reused.
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
     * together with other catalog properties configured at connector level
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
        Configuration hadoopConf = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session), new Path(warehouse));
        return loadCatalog(catalogImpl, catalogName, builder.build(), hadoopConf);
    }

    private Catalog getCatalog(ConnectorSession session)
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
        throw new TrinoException(NOT_SUPPORTED, "catalog " + catalogImpl + " does not support namespace operations");
    }

    @Override
    public String getName(ConnectorSession session)
    {
        return getCatalog(session).name();
    }

    @Override
    public List<Namespace> listNamespaces(ConnectorSession session)
    {
        return getNamespaceCatalog(session).listNamespaces();
    }

    @Override
    public void createNamespaceWithPrincipal(Namespace namespace, Map<String, Object> map, TrinoPrincipal owner, ConnectorSession session)
    {
        getNamespaceCatalog(session).createNamespace(namespace, map.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
    }

    @Override
    public Map<String, Object> loadNamespaceMetadataObjects(Namespace namespace, ConnectorSession session)
            throws NoSuchNamespaceException
    {
        return getNamespaceCatalog(session).loadNamespaceMetadata(namespace).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public boolean dropNamespace(Namespace namespace, ConnectorSession session)
            throws NamespaceNotEmptyException
    {
        return getNamespaceCatalog(session).dropNamespace(namespace);
    }

    @Override
    public void setNamespacePrincipal(Namespace namespace, TrinoPrincipal principal, ConnectorSession session)
            throws NoSuchNamespaceException
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported by " + getName(session));
    }

    @Override
    public void renameNamespace(Namespace source, Namespace target, ConnectorSession session)
            throws NoSuchNamespaceException
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported by " + getName(session));
    }

    @Override
    public TrinoPrincipal getNamespacePrincipal(Namespace namespace, ConnectorSession session)
            throws NoSuchNamespaceException
    {
        throw new TrinoException(NOT_SUPPORTED, "getNamespacePrincipal is not supported by " + getName(session));
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace, ConnectorSession session)
    {
        return getCatalog(session).listTables(namespace);
    }

    @Override
    public Transaction newCreateTableTransaction(TableIdentifier tableIdentifier, Schema schema, PartitionSpec partitionSpec, String location,
            Map<String, String> properties, ConnectorSession session)
    {
        return getCatalog(session).newCreateTableTransaction(tableIdentifier, schema, partitionSpec, location, properties);
    }

    @Override
    public boolean tableExists(TableIdentifier identifier, ConnectorSession session)
    {
        return getCatalog(session).tableExists(identifier);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge, ConnectorSession session)
    {
        return getCatalog(session).dropTable(identifier, purge);
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to, ConnectorSession session)
    {
        getCatalog(session).renameTable(from, to);
    }

    @Override
    public Table loadTable(TableIdentifier identifier, ConnectorSession session)
    {
        return getCatalog(session).loadTable(identifier);
    }

    @Override
    public void updateTableComment(TableIdentifier tableIdentifier, Optional<String> comment, ConnectorSession session)
    {
        UpdateProperties update = loadTable(tableIdentifier, session).updateProperties();
        comment.ifPresentOrElse(c -> update.set(TABLE_COMMENT, c), () -> update.remove(TABLE_COMMENT));
    }

    @Override
    public String defaultTableLocation(TableIdentifier tableIdentifier, ConnectorSession session)
    {
        String dbLocationUri = PropertyUtil.propertyAsString(
                getNamespaceCatalog(session).loadNamespaceMetadata(tableIdentifier.namespace()), "locationUri", null);

        if (dbLocationUri != null) {
            return String.format("%s/%s", dbLocationUri, tableIdentifier.name());
        }
        else {
            return String.format("%s/%s/%s", warehouse, schemaFromTableId(tableIdentifier), tableIdentifier.name());
        }
    }

    @Override
    public void createMaterializedView(TableIdentifier viewIdentifier, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting, ConnectorSession session)
    {
        throw new TrinoException(NOT_SUPPORTED, "createMaterializedView is not supported by " + getName(session));
    }

    @Override
    public void dropMaterializedView(TableIdentifier viewIdentifier, ConnectorSession session)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropMaterializedView is not supported by " + getName(session));
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(TableIdentifier viewIdentifier, ConnectorSession session)
    {
        return Optional.empty();
    }

    @Override
    public List<TableIdentifier> listViews(Namespace namespace, ConnectorSession session)
    {
        throw new TrinoException(NOT_SUPPORTED, "listViews is not supported by " + getName(session));
    }

    @Override
    public List<TableIdentifier> listMaterializedViews(Namespace namespace, ConnectorSession session)
    {
        throw new TrinoException(NOT_SUPPORTED, "listMaterializedViews is not supported by " + getName(session));
    }
}
