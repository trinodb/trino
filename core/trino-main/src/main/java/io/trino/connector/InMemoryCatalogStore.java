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
package io.trino.connector;

import com.google.common.collect.ImmutableList;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.ConnectorName;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.trino.connector.FileCatalogStore.computeCatalogVersion;
import static java.util.Objects.requireNonNull;

public class InMemoryCatalogStore
        implements CatalogStore
{
    private final ConcurrentMap<CatalogName, StoredCatalog> catalogs = new ConcurrentHashMap<>();

    @Override
    public Collection<StoredCatalog> getCatalogs()
    {
        return ImmutableList.copyOf(catalogs.values());
    }

    @Override
    public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
    {
        return new CatalogProperties(
                catalogName,
                computeCatalogVersion(catalogName, connectorName, properties),
                connectorName,
                properties);
    }

    @Override
    public void addOrReplaceCatalog(CatalogProperties catalogProperties)
    {
        CatalogName catalogName = catalogProperties.name();
        catalogs.put(catalogName, new InMemoryStoredCatalog(catalogName, catalogProperties));
    }

    @Override
    public void removeCatalog(CatalogName catalogName)
    {
        catalogs.remove(catalogName);
    }

    private static class InMemoryStoredCatalog
            implements StoredCatalog
    {
        private final CatalogName catalogName;
        private final CatalogProperties catalogProperties;

        public InMemoryStoredCatalog(CatalogName catalogName, CatalogProperties catalogProperties)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.catalogProperties = requireNonNull(catalogProperties, "catalogProperties is null");
        }

        @Override
        public CatalogName name()
        {
            return catalogName;
        }

        @Override
        public CatalogProperties loadProperties()
        {
            return catalogProperties;
        }
    }
}
