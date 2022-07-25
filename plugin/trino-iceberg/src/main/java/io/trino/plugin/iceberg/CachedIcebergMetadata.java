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

import io.airlift.json.JsonCodec;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CachedIcebergMetadata
        extends IcebergMetadata
{
    private IcebergMetadataCache icebergMetadataCache;

    public CachedIcebergMetadata(IcebergMetadataFactory metadataFactory, CatalogType catalogType,
            TypeManager typeManager,
            TypeOperators typeOperators,
            JsonCodec<CommitTaskData> commitTaskCodec,
            TrinoCatalog catalog,
            HdfsEnvironment hdfsEnvironment, long globalMetadataCacheTtl, int maxCacheSize, long globalMetadataCacheTtlForListing)
    {
        super(typeManager, typeOperators, commitTaskCodec, catalog, hdfsEnvironment);
        icebergMetadataCache = IcebergMetadataCache.getMetadataCache(metadataFactory, typeManager, catalogType, globalMetadataCacheTtl, maxCacheSize, globalMetadataCacheTtlForListing);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return icebergMetadataCache.listSchemaNames(session);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, CatalogSchemaName schemaName)
    {
        return icebergMetadataCache.getSchemaProperties(session, schemaName);
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, CatalogSchemaName schemaName)
    {
        return icebergMetadataCache.getSchemaOwner(session, schemaName);
    }

    @Override
    public IcebergTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        return icebergMetadataCache.getTableHandle(session, tableName, startVersion, endVersion);
    }

    @Override
    Table getIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return icebergMetadataCache.getIcebergTable(session, schemaTableName);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return icebergMetadataCache.getSystemTable(session, tableName);
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return icebergMetadataCache.getTableProperties(session, tableHandle);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return icebergMetadataCache.getTableStatistics(session, tableHandle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return icebergMetadataCache.getTableMetadata(session, tableHandle);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return icebergMetadataCache.listTables(session, schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return icebergMetadataCache.getColumnHandles(session, tableHandle);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return icebergMetadataCache.getViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return icebergMetadataCache.getView(session, viewName);
    }
}
