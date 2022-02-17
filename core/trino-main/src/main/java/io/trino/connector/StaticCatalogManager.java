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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.log.Logger;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.spi.connector.Connector;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class StaticCatalogManager
        implements CatalogManager, ConnectorServicesProvider
{
    private static final Logger log = Logger.get(StaticCatalogManager.class);

    private enum State { CREATED, INITIALIZED, STOPPED }

    private final CatalogFactory catalogFactory;
    private final List<CatalogProperties> catalogProperties;

    @GuardedBy("this")
    private final ConcurrentMap<String, CatalogConnector> catalogs = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private State state = State.CREATED;

    @Inject
    public StaticCatalogManager(CatalogFactory catalogFactory, StaticCatalogManagerConfig config)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        List<String> disabledCatalogs = firstNonNull(config.getDisabledCatalogs(), ImmutableList.of());

        ImmutableList.Builder<CatalogProperties> catalogProperties = ImmutableList.builder();
        for (File file : listCatalogFiles(config.getCatalogConfigurationDir())) {
            String catalogName = Files.getNameWithoutExtension(file.getName());
            if (disabledCatalogs.contains(catalogName)) {
                log.info("Skipping disabled catalog %s", catalogName);
                continue;
            }

            Map<String, String> properties;
            try {
                properties = new HashMap<>(loadPropertiesFrom(file.getPath()));
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error reading catalog property file " + file, e);
            }

            String connectorName = properties.remove("connector.name");
            checkState(connectorName != null, "Catalog configuration %s does not contain connector.name", file.getAbsoluteFile());

            catalogProperties.add(new CatalogProperties(catalogName, connectorName, ImmutableMap.copyOf(properties)));
        }
        this.catalogProperties = catalogProperties.build();
    }

    private static List<File> listCatalogFiles(File catalogsDirectory)
    {
        if (catalogsDirectory == null || !catalogsDirectory.isDirectory()) {
            return ImmutableList.of();
        }

        File[] files = catalogsDirectory.listFiles();
        if (files == null) {
            return ImmutableList.of();
        }
        return Arrays.stream(files)
                .filter(File::isFile)
                .filter(file -> file.getName().endsWith(".properties"))
                .collect(toImmutableList());
    }

    @PreDestroy
    public synchronized void stop()
    {
        if (state == State.STOPPED) {
            return;
        }
        state = State.STOPPED;

        for (CatalogConnector connector : catalogs.values()) {
            connector.shutdown();
        }
        catalogs.clear();
    }

    public synchronized void loadInitialCatalogs()
    {
        switch (state) {
            case CREATED:
                break;
            case INITIALIZED:
                return;
            case STOPPED:
                throw new IllegalStateException("Catalog manager is stopped");
        }
        state = State.INITIALIZED;

        for (CatalogProperties catalog : catalogProperties) {
            log.info("-- Loading catalog %s --", catalog.getCatalogName());
            CatalogConnector newCatalog = catalogFactory.createCatalog(catalog.getCatalogName(), catalog.getConnectorName(), catalog.getProperties());
            catalogs.put(catalog.getCatalogName(), newCatalog);
            log.info("-- Added catalog %s using connector %s --", catalog.getCatalogName(), catalog.getConnectorName());
        }
    }

    @Override
    public synchronized Set<String> getCatalogNames()
    {
        return ImmutableSet.copyOf(catalogs.keySet());
    }

    @Override
    public synchronized Optional<Catalog> getCatalog(String catalogName)
    {
        return Optional.ofNullable(catalogs.get(catalogName))
                .map(CatalogConnector::getCatalog);
    }

    @Override
    public synchronized ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
    {
        CatalogConnector catalogConnector = catalogs.get(catalogHandle.getCatalogName());
        checkArgument(catalogConnector != null, "No catalog '%s'", catalogHandle.getCatalogName());
        return catalogConnector.getMaterializedConnector(catalogHandle.getType());
    }

    public synchronized CatalogHandle createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(connectorName, "connectorName is null");
        requireNonNull(properties, "properties is null");

        checkState(state != State.STOPPED, "Catalog manager is stopped");

        checkArgument(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
        CatalogConnector catalog = catalogFactory.createCatalog(catalogName, connectorName, properties);
        catalogs.put(catalogName, catalog);
        return catalog.getCatalogHandle();
    }

    public synchronized void createCatalog(CatalogHandle catalogHandle, String connectorName, Connector connector)
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(connectorName, "connectorName is null");
        requireNonNull(connector, "connector is null");

        checkState(state != State.STOPPED, "Catalog manager is stopped");
        String catalogName = catalogHandle.getCatalogName();
        checkArgument(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);

        CatalogConnector catalog = catalogFactory.createCatalog(catalogHandle, connectorName, connector);
        catalogs.put(catalogName, catalog);
    }

    private static class CatalogProperties
    {
        private final String catalogName;
        private final String connectorName;
        private final Map<String, String> properties;

        public CatalogProperties(String catalogName, String connectorName, Map<String, String> properties)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.connectorName = requireNonNull(connectorName, "connectorName is null");
            this.properties = requireNonNull(properties, "properties is null");
        }

        public String getCatalogName()
        {
            return catalogName;
        }

        public String getConnectorName()
        {
            return connectorName;
        }

        public Map<String, String> getProperties()
        {
            return properties;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("catalogName", catalogName)
                    .add("connectorName", connectorName)
                    .toString();
        }
    }
}
