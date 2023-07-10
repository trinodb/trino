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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.trino.connector.FileCatalogStore.computeCatalogVersion;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static java.util.Objects.requireNonNull;

public class InMemoryCatalogStore
        implements CatalogStore
{
    private final ConcurrentMap<String, StoredCatalog> catalogs = new ConcurrentHashMap<>();

    @Override
    public Collection<StoredCatalog> getCatalogs()
    {
        return ImmutableList.copyOf(catalogs.values());
    }

    @Override
    public CatalogProperties createCatalogProperties(String catalogName, ConnectorName connectorName, Map<String, String> properties)
    {
        return new CatalogProperties(
                createRootCatalogHandle(catalogName, computeCatalogVersion(catalogName, connectorName, properties)),
                connectorName,
                properties);
    }

    @Override
    public void addOrReplaceCatalog(CatalogProperties catalogProperties)
    {
        catalogs.put(catalogProperties.getCatalogHandle().getCatalogName(), new InMemoryStoredCatalog(catalogProperties));
    }

    @Override
    public void removeCatalog(String catalogName)
    {
        catalogs.remove(catalogName);
    }

    private static class InMemoryStoredCatalog
            implements StoredCatalog
    {
        private final CatalogProperties catalogProperties;

        public InMemoryStoredCatalog(CatalogProperties catalogProperties)
        {
            this.catalogProperties = requireNonNull(catalogProperties, "catalogProperties is null");
        }

        @Override
        public String getName()
        {
            return catalogProperties.getCatalogHandle().getCatalogName();
        }

        @Override
        public CatalogProperties loadProperties()
        {
            return catalogProperties;
        }
    }
}
