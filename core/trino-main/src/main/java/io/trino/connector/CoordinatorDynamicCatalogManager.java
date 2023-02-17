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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.server.ForStartup;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
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
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_AVAILABLE;
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
    private final ConcurrentMap<String, CatalogConnector> activeCatalogs = new ConcurrentHashMap<>();

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
                                log.info("-- Loading catalog %s --", storedCatalog.getName());
                                CatalogProperties catalog = storedCatalog.loadProperties();
                                CatalogConnector newCatalog = catalogFactory.createCatalog(catalog);
                                activeCatalogs.put(catalog.getCatalogHandle().getCatalogName(), newCatalog);
                                allCatalogs.put(catalog.getCatalogHandle(), newCatalog);
                                log.info("-- Added catalog %s using connector %s --", storedCatalog.getName(), catalog.getConnectorName());
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
        return Optional.ofNullable(activeCatalogs.get(catalogName))
                .map(CatalogConnector::getCatalog);
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
    public void createCatalog(String catalogName, ConnectorName connectorName, Map<String, String> properties)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(connectorName, "connectorName is null");
        requireNonNull(properties, "properties is null");

        catalogsUpdateLock.lock();
        try {
            checkState(state != State.STOPPED, "ConnectorManager is stopped");

            if (activeCatalogs.containsKey(catalogName)) {
                throw new TrinoException(ALREADY_EXISTS, format("Catalog '%s' already exists", catalogName));
            }

            // get or create catalog for the handle
            CatalogConnector catalog = allCatalogs.computeIfAbsent(
                    createRootCatalogHandle(catalogName, computeCatalogVersion(catalogName, connectorName, properties)),
                    handle -> catalogFactory.createCatalog(new CatalogProperties(handle, connectorName, properties)));
            activeCatalogs.put(catalogName, catalog);
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
            if (activeCatalogs.putIfAbsent(GlobalSystemConnector.NAME, catalog) != null) {
                throw new IllegalStateException("Global system catalog already registered");
            }
            allCatalogs.put(GlobalSystemConnector.CATALOG_HANDLE, catalog);
        }
        finally {
            catalogsUpdateLock.unlock();
        }
    }

    static CatalogVersion computeCatalogVersion(String catalogName, ConnectorName connectorName, Map<String, String> properties)
    {
        Hasher hasher = Hashing.sha256().newHasher();
        hasher.putUnencodedChars("catalog-hash");
        hashLengthPrefixedString(hasher, catalogName);
        hashLengthPrefixedString(hasher, connectorName.toString());
        hasher.putInt(properties.size());
        ImmutableSortedMap.copyOf(properties).forEach((key, value) -> {
            hashLengthPrefixedString(hasher, key);
            hashLengthPrefixedString(hasher, value);
        });
        return new CatalogVersion(hasher.hash().toString());
    }

    private static void hashLengthPrefixedString(Hasher hasher, String value)
    {
        hasher.putInt(value.length());
        hasher.putUnencodedChars(value);
    }
}
