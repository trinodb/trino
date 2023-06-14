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
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.server.ForStartup;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import jakarta.annotation.PreDestroy;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_AVAILABLE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.util.Executors.executeUntilFailure;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class StaticCatalogManager
        implements CatalogManager, ConnectorServicesProvider
{
    private static final Logger log = Logger.get(StaticCatalogManager.class);

    private enum State { CREATED, INITIALIZED, STOPPED }

    private final CatalogFactory catalogFactory;
    private final List<CatalogProperties> catalogProperties;
    private final Executor executor;

    private final ConcurrentMap<String, CatalogConnector> catalogs = new ConcurrentHashMap<>();

    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);

    @Inject
    public StaticCatalogManager(CatalogFactory catalogFactory, StaticCatalogManagerConfig config, @ForStartup Executor executor)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        List<String> disabledCatalogs = firstNonNull(config.getDisabledCatalogs(), ImmutableList.of());

        ImmutableList.Builder<CatalogProperties> catalogProperties = ImmutableList.builder();
        for (File file : listCatalogFiles(config.getCatalogConfigurationDir())) {
            String catalogName = Files.getNameWithoutExtension(file.getName());
            checkArgument(!catalogName.equals(GlobalSystemConnector.NAME), "Catalog name SYSTEM is reserved for internal usage");
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
            if (connectorName.indexOf('-') >= 0) {
                String deprecatedConnectorName = connectorName;
                connectorName = connectorName.replace('-', '_');
                log.warn("Catalog '%s' is using the deprecated connector name '%s'. The correct connector name is '%s'", catalogName, deprecatedConnectorName, connectorName);
            }

            catalogProperties.add(new CatalogProperties(
                    createRootCatalogHandle(catalogName, new CatalogVersion("default")),
                    new ConnectorName(connectorName),
                    ImmutableMap.copyOf(properties)));
        }
        this.catalogProperties = catalogProperties.build();
        this.executor = requireNonNull(executor, "executor is null");
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
    public void stop()
    {
        if (state.getAndSet(State.STOPPED) == State.STOPPED) {
            return;
        }

        for (CatalogConnector connector : catalogs.values()) {
            connector.shutdown();
        }
        catalogs.clear();
    }

    @Override
    public void loadInitialCatalogs()
    {
        if (!state.compareAndSet(State.CREATED, State.INITIALIZED)) {
            return;
        }

        executeUntilFailure(
                executor,
                catalogProperties.stream()
                        .map(catalog -> (Callable<?>) () -> {
                            String catalogName = catalog.getCatalogHandle().getCatalogName();
                            log.info("-- Loading catalog %s --", catalogName);
                            CatalogConnector newCatalog = catalogFactory.createCatalog(catalog);
                            catalogs.put(catalogName, newCatalog);
                            log.info("-- Added catalog %s using connector %s --", catalogName, catalog.getConnectorName());
                            return null;
                        })
                        .collect(toImmutableList()));
    }

    @Override
    public Set<String> getCatalogNames()
    {
        return ImmutableSet.copyOf(catalogs.keySet());
    }

    @Override
    public Optional<Catalog> getCatalog(String catalogName)
    {
        return Optional.ofNullable(catalogs.get(catalogName))
                .map(CatalogConnector::getCatalog);
    }

    @Override
    public void ensureCatalogsLoaded(Session session, List<CatalogProperties> catalogs)
    {
        List<CatalogProperties> missingCatalogs = catalogs.stream()
                .filter(catalog -> !this.catalogs.containsKey(catalog.getCatalogHandle().getCatalogName()))
                .collect(toImmutableList());

        if (!missingCatalogs.isEmpty()) {
            throw new TrinoException(CATALOG_NOT_AVAILABLE, "Missing catalogs: " + missingCatalogs);
        }
    }

    @Override
    public void pruneCatalogs(Set<CatalogHandle> catalogsInUse)
    {
        // static catalogs do not need management
    }

    @Override
    public Optional<CatalogProperties> getCatalogProperties(CatalogHandle catalogHandle)
    {
        // static catalog manager does not propagate catalogs between machines
        return Optional.empty();
    }

    @Override
    public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
    {
        CatalogConnector catalogConnector = catalogs.get(catalogHandle.getCatalogName());
        checkArgument(catalogConnector != null, "No catalog '%s'", catalogHandle.getCatalogName());
        return catalogConnector.getMaterializedConnector(catalogHandle.getType());
    }

    public void registerGlobalSystemConnector(GlobalSystemConnector connector)
    {
        requireNonNull(connector, "connector is null");

        CatalogConnector catalog = catalogFactory.createCatalog(GlobalSystemConnector.CATALOG_HANDLE, new ConnectorName(GlobalSystemConnector.NAME), connector);
        if (catalogs.putIfAbsent(GlobalSystemConnector.NAME, catalog) != null) {
            throw new IllegalStateException("Global system catalog already registered");
        }
    }

    @Override
    public void createCatalog(String catalogName, ConnectorName connectorName, Map<String, String> properties, boolean notExists)
    {
        throw new TrinoException(NOT_SUPPORTED, "CREATE CATALOG is not supported by the static catalog store");
    }

    @Override
    public void dropCatalog(String catalogName, boolean exists)
    {
        throw new TrinoException(NOT_SUPPORTED, "DROP CATALOG is not supported by the static catalog store");
    }
}
