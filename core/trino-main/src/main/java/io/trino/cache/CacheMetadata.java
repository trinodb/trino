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
package io.trino.cache;

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.TableHandle;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CacheMetadata
{
    private final CatalogServiceProvider<Optional<ConnectorCacheMetadata>> cacheMetadataProvider;

    @Inject
    public CacheMetadata(CatalogServiceProvider<Optional<ConnectorCacheMetadata>> cacheMetadataProvider)
    {
        this.cacheMetadataProvider = requireNonNull(cacheMetadataProvider, "cacheMetadataProvider is null");
    }

    public Optional<CacheTableId> getCacheTableId(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        Optional<ConnectorCacheMetadata> service = cacheMetadataProvider.getService(catalogHandle);

        return service.flatMap(cacheMetadata -> cacheMetadata.getCacheTableId(tableHandle.connectorHandle()));
    }

    public Optional<CacheColumnId> getCacheColumnId(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        Optional<ConnectorCacheMetadata> service = cacheMetadataProvider.getService(catalogHandle);

        return service.flatMap(cacheMetadata -> cacheMetadata.getCacheColumnId(tableHandle.connectorHandle(), columnHandle));
    }

    public TableHandle getCanonicalTableHandle(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        Optional<ConnectorCacheMetadata> service = cacheMetadataProvider.getService(catalogHandle);
        return service
                .map(connectorCacheMetadata -> new TableHandle(
                        tableHandle.catalogHandle(),
                        connectorCacheMetadata.getCanonicalTableHandle(tableHandle.connectorHandle()),
                        tableHandle.transaction()))
                .orElse(tableHandle);
    }
}
