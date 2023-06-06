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
import com.google.common.collect.ImmutableSet;
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

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metadata.Catalog.failedCatalog;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_AVAILABLE;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.util.Executors.executeUntilFailure;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class CoordinatorDynamicCatalogManager
        implements CatalogManager, ConnectorServicesProvider
{
    private static final Logger log = Logger.get(CoordinatorDynamicCatalogManager.class);

    private enum State { CREATED, INITIALIZED, STOPPED }

    private final CatalogStore catalogStore;
    private final CatalogFactory catalogFactory;
    private final Executor executor;

    private final Lock catalogsUpdateLock = new ReentrantLock();

    /**
     * Active catalogs that have been created and not dropped.
     */
    private final ConcurrentMap<String, Catalog> activeCatalogs = new ConcurrentHashMap<>();

    /**
     * All catalogs including those that have been dropped.
     */
    private final ConcurrentMap<CatalogHandle, CatalogConnector> allCatalogs = new ConcurrentHashMap<>();

    @GuardedBy("catalogsUpdateLock")
    private State state = State.CREATED;

    @Inject
    public CoordinatorDynamicCatalogManager(CatalogStore catalogStore, CatalogFactory catalogFactory, @ForStartup Executor executor)
    {
        this.catalogStore = requireNonNull(catalogStore, "catalogStore is null");
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @PreDestroy
    public void stop()
    {
        List<CatalogConnector> catalogs;

        catalogsUpdateLock.lock();
        try {
            if (state == State.STOPPED) {
                return;
            }
            state = State.STOPPED;

            catalogs = ImmutableList.copyOf(allCatalogs.values());
            allCatalogs.clear();
            activeCatalogs.clear();
        }
        finally {
            catalogsUpdateLock.unlock();
        }

        for (CatalogConnector connector : catalogs) {
            connector.shutdown();
        }
    }

    @Override
    public void loadInitialCatalogs()
    {
        catalogsUpdateLock.lock();
        try {
            if (state == State.INITIALIZED) {
                return;
            }
            checkState(state != State.STOPPED, "ConnectorManager is stopped");
            state = State.INITIALIZED;

            executeUntilFailure(
                    executor,
                    catalogStore.getCatalogs().stream()
                            .map(storedCatalog -> (Callable<?>) () -> {
                                CatalogProperties catalog = null;
                                try {
                                    catalog = storedCatalog.loadProperties();
                                    CatalogConnector newCatalog = catalogFactory.createCatalog(catalog);
                                    activeCatalogs.put(catalog.getCatalogHandle().getCatalogName(), newCatalog.getCatalog());
                                    allCatalogs.put(catalog.getCatalogHandle(), newCatalog);
                                    log.info("-- Added catalog %s using connector %s --", storedCatalog.getName(), catalog.getConnectorName());
                                }
                                catch (Throwable e) {
                                    CatalogHandle catalogHandle = catalog != null ? catalog.getCatalogHandle() : createRootCatalogHandle(storedCatalog.getName(), new CatalogVersion("failed"));
                                    ConnectorName connectorName = catalog != null ? catalog.getConnectorName() : new ConnectorName("unknown");
                                    activeCatalogs.put(storedCatalog.getName(), failedCatalog(storedCatalog.getName(), catalogHandle, connectorName));
                                    log.error(e, "-- Failed to load catalog %s using connector %s --", storedCatalog.getName(), connectorName);
                                }
                                return null;
                            })
                            .collect(toImmutableList()));
        }
        finally {
            catalogsUpdateLock.unlock();
        }
    }

    @Override
    public Set<String> getCatalogNames()
    {
        return ImmutableSet.copyOf(activeCatalogs.keySet());
    }

    @Override
    public Optional<Catalog> getCatalog(String catalogName)
    {
        return Optional.ofNullable(activeCatalogs.get(catalogName));
    }

    public Set<CatalogHandle> getActiveCatalogs()
    {
        return activeCatalogs.values().stream()
                .map(Catalog::getCatalogHandle)
                .collect(toImmutableSet());
    }

    @Override
    public void ensureCatalogsLoaded(Session session, List<CatalogProperties> catalogs)
    {
        List<CatalogProperties> missingCatalogs = catalogs.stream()
                .filter(catalog -> !allCatalogs.containsKey(catalog.getCatalogHandle()))
                .collect(toImmutableList());

        if (!missingCatalogs.isEmpty()) {
            throw new TrinoException(CATALOG_NOT_AVAILABLE, "Missing catalogs: " + missingCatalogs);
        }
    }

    @Override
    public void pruneCatalogs(Set<CatalogHandle> catalogsInUse)
    {
        List<CatalogConnector> removedCatalogs = new ArrayList<>();
        catalogsUpdateLock.lock();
        try {
            if (state == State.STOPPED) {
                return;
            }
            Iterator<Entry<CatalogHandle, CatalogConnector>> iterator = allCatalogs.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<CatalogHandle, CatalogConnector> entry = iterator.next();

                Catalog activeCatalog = activeCatalogs.get(entry.getKey().getCatalogName());
                if (activeCatalog != null && activeCatalog.getCatalogHandle().equals(entry.getKey())) {
                    // catalog is registered with a name, and therefor is available for new queries, and should not be removed
                    continue;
                }

                if (!catalogsInUse.contains(entry.getKey())) {
                    iterator.remove();
                    removedCatalogs.add(entry.getValue());
                }
            }
        }
        finally {
            catalogsUpdateLock.unlock();
        }

        // todo do this in a background thread
        for (CatalogConnector removedCatalog : removedCatalogs) {
            try {
                removedCatalog.shutdown();
            }
            catch (Throwable e) {
                log.error(e, "Error shutting down catalog: %s".formatted(removedCatalog));
            }
        }

        if (!removedCatalogs.isEmpty()) {
            List<String> sortedHandles = removedCatalogs.stream().map(connector -> connector.getCatalogHandle().toString()).sorted().toList();
            log.info("Pruned catalogs: %s", sortedHandles);
        }
    }

    @Override
    public Optional<CatalogProperties> getCatalogProperties(CatalogHandle catalogHandle)
    {
        return Optional.ofNullable(allCatalogs.get(catalogHandle.getRootCatalogHandle()))
                .flatMap(CatalogConnector::getCatalogProperties);
    }

    @Override
    public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
    {
        CatalogConnector catalogConnector = allCatalogs.get(catalogHandle.getRootCatalogHandle());
        checkArgument(catalogConnector != null, "No catalog '%s'", catalogHandle.getCatalogName());
        return catalogConnector.getMaterializedConnector(catalogHandle.getType());
    }

    @Override
    public void createCatalog(String catalogName, ConnectorName connectorName, Map<String, String> properties, boolean notExists)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(connectorName, "connectorName is null");
        requireNonNull(properties, "properties is null");

        catalogsUpdateLock.lock();
        try {
            checkState(state != State.STOPPED, "ConnectorManager is stopped");

            if (activeCatalogs.containsKey(catalogName)) {
                if (!notExists) {
                    throw new TrinoException(ALREADY_EXISTS, format("Catalog '%s' already exists", catalogName));
                }
                return;
            }

            CatalogProperties catalogProperties = catalogStore.createCatalogProperties(catalogName, connectorName, properties);

            // get or create catalog for the handle
            CatalogConnector catalog = allCatalogs.computeIfAbsent(
                    catalogProperties.getCatalogHandle(),
                    handle -> catalogFactory.createCatalog(catalogProperties));
            activeCatalogs.put(catalogName, catalog.getCatalog());
            catalogStore.addOrReplaceCatalog(catalogProperties);

            log.info("Added catalog: %s", catalog.getCatalogHandle());
        }
        finally {
            catalogsUpdateLock.unlock();
        }
    }

    public void registerGlobalSystemConnector(GlobalSystemConnector connector)
    {
        requireNonNull(connector, "connector is null");

        catalogsUpdateLock.lock();
        try {
            if (state == State.STOPPED) {
                return;
            }

            CatalogConnector catalog = catalogFactory.createCatalog(GlobalSystemConnector.CATALOG_HANDLE, new ConnectorName(GlobalSystemConnector.NAME), connector);
            if (activeCatalogs.putIfAbsent(GlobalSystemConnector.NAME, catalog.getCatalog()) != null) {
                throw new IllegalStateException("Global system catalog already registered");
            }
            allCatalogs.put(GlobalSystemConnector.CATALOG_HANDLE, catalog);
        }
        finally {
            catalogsUpdateLock.unlock();
        }
    }

    @Override
    public void dropCatalog(String catalogName, boolean exists)
    {
        requireNonNull(catalogName, "catalogName is null");

        boolean removed;
        catalogsUpdateLock.lock();
        try {
            checkState(state != State.STOPPED, "ConnectorManager is stopped");

            catalogStore.removeCatalog(catalogName);
            removed = activeCatalogs.remove(catalogName) != null;
        }
        finally {
            catalogsUpdateLock.unlock();
        }

        if (!removed && !exists) {
            throw new TrinoException(NOT_FOUND, format("Catalog '%s' does not exist", catalogName));
        }
        // Do not shut down the catalog, because there may still be running queries using this catalog.
        // Catalog shutdown logic will be added later.
    }
}
