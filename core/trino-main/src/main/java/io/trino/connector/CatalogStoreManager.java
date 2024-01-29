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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.CatalogStoreFactory;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorName;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.util.Objects.requireNonNull;

public class CatalogStoreManager
        implements CatalogStore
{
    private static final Logger log = Logger.get(CatalogStoreManager.class);
    private static final File CATALOG_STORE_CONFIGURATION = new File("etc/catalog-store.properties");
    private static final String CATALOG_STORE_PROPERTY_NAME = "catalog-store.name";
    private final Map<String, CatalogStoreFactory> catalogStoreFactories = new ConcurrentHashMap<>();
    private final AtomicReference<Optional<CatalogStore>> configuredCatalogStore = new AtomicReference<>(Optional.empty());

    public void addCatalogStoreFactory(CatalogStoreFactory catalogStoreFactory)
    {
        requireNonNull(catalogStoreFactory, "catalogStoreFactory is null");

        if (catalogStoreFactories.putIfAbsent(catalogStoreFactory.getName(), catalogStoreFactory) != null) {
            throw new IllegalArgumentException("Catalog store factory '%s' is already registered".formatted(catalogStoreFactory.getName()));
        }
    }

    public void loadConfiguredCatalogStore()
            throws IOException
    {
        loadConfiguredCatalogStore(CATALOG_STORE_CONFIGURATION);
    }

    @VisibleForTesting
    void loadConfiguredCatalogStore(File catalogStoreFile)
            throws IOException
    {
        if (configuredCatalogStore.get().isPresent() || !catalogStoreFile.exists()) {
            return;
        }
        Map<String, String> properties = new HashMap<>(loadPropertiesFrom(catalogStoreFile.getPath()));

        String catalogStoreName = properties.remove(CATALOG_STORE_PROPERTY_NAME);
        checkArgument(!isNullOrEmpty(catalogStoreName),
                "Catalog store configuration %s does not contain %s", catalogStoreFile.getAbsoluteFile(), CATALOG_STORE_PROPERTY_NAME);

        setConfiguredCatalogStore(catalogStoreName, properties);
    }

    @VisibleForTesting
    protected void setConfiguredCatalogStore(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading catalog store %s --", name);

        CatalogStoreFactory factory = catalogStoreFactories.get(name);
        checkState(factory != null, "Catalog store %s is not registered", name);

        CatalogStore catalogStore;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            catalogStore = factory.create(ImmutableMap.copyOf(properties));
        }

        setConfiguredCatalogStore(catalogStore);

        log.info("-- Loaded catalog store %s --", name);
    }

    @VisibleForTesting
    protected void setConfiguredCatalogStore(CatalogStore catalogStore)
    {
        checkState(configuredCatalogStore.compareAndSet(Optional.empty(), Optional.of(catalogStore)), "catalogStore is already set");
    }

    @Override
    public Collection<StoredCatalog> getCatalogs()
    {
        return getCatalogStore().getCatalogs();
    }

    @Override
    public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
    {
        return getCatalogStore().createCatalogProperties(catalogName, connectorName, properties);
    }

    @Override
    public void addOrReplaceCatalog(CatalogProperties catalogProperties)
    {
        getCatalogStore().addOrReplaceCatalog(catalogProperties);
    }

    @Override
    public void removeCatalog(CatalogName catalogName)
    {
        getCatalogStore().removeCatalog(catalogName);
    }

    private CatalogStore getCatalogStore()
    {
        return configuredCatalogStore.get().orElseThrow(() -> new IllegalStateException("Catalog store is not configured"));
    }
}
