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
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorName;
import jakarta.annotation.PreDestroy;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class WorkerDynamicCatalogManager
        implements ConnectorServicesProvider
{
    private static final Logger log = Logger.get(WorkerDynamicCatalogManager.class);

    private final CatalogFactory catalogFactory;

    private final ReadWriteLock catalogsLock = new ReentrantReadWriteLock();
    private final Lock catalogLoadingLock = catalogsLock.readLock();
    private final Lock catalogRemovingLock = catalogsLock.writeLock();
    private final ConcurrentMap<CatalogHandle, Future<CatalogConnector>> catalogs = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @GuardedBy("catalogsUpdateLock")
    private boolean stopped;

    @Inject
    public WorkerDynamicCatalogManager(CatalogFactory catalogFactory)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
    }

    @PreDestroy
    public void stop()
    {
        List<Future<CatalogConnector>> catalogs;

        catalogRemovingLock.lock();
        try {
            if (stopped) {
                return;
            }
            stopped = true;

            catalogs = ImmutableList.copyOf(this.catalogs.values());
            this.catalogs.clear();
        }
        finally {
            catalogRemovingLock.unlock();
        }

        for (Future<CatalogConnector> connector : catalogs) {
            switch (connector.state()) {
                case SUCCESS -> connector.resultNow().shutdown();
                case RUNNING -> connector.cancel(true);
                case CANCELLED, FAILED -> {}
            }
        }
    }

    @Override
    public void loadInitialCatalogs() {}

    @Override
    public void ensureCatalogsLoaded(Session session, List<CatalogProperties> expectedCatalogs)
    {
        if (getMissingCatalogs(expectedCatalogs).isEmpty()) {
            return;
        }

        catalogLoadingLock.lock();
        try {
            if (stopped) {
                return;
            }

            List<CatalogProperties> missingCatalogs = getMissingCatalogs(expectedCatalogs);
            missingCatalogs.forEach(catalog -> checkArgument(!catalog.catalogHandle().equals(GlobalSystemConnector.CATALOG_HANDLE), "Global system catalog not registered"));

            ImmutableList.Builder<Future<CatalogConnector>> futures = ImmutableList.builder();

            for (CatalogProperties catalogProperties : missingCatalogs) {
                futures.add(catalogs.computeIfAbsent(catalogProperties.catalogHandle(), ignore -> executor.submit(() -> {
                    CatalogConnector newCatalog = catalogFactory.createCatalog(catalogProperties);
                    log.debug("Added catalog: " + catalogProperties.catalogHandle());
                    return newCatalog;
                })));
            }

            ImmutableList.Builder<Throwable> exceptionsBuilder = ImmutableList.builder();
            for (Future<CatalogConnector> loadedCatalog : futures.build()) {
                try {
                    loadedCatalog.get();
                }
                catch (ExecutionException e) {
                    if (e.getCause() != null) {
                        exceptionsBuilder.add(e.getCause());
                    }
                    else {
                        exceptionsBuilder.add(e);
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    exceptionsBuilder.add(e);
                }
                finally {
                    loadedCatalog.cancel(true);
                }
            }

            List<Throwable> catalogLoadingExceptions = exceptionsBuilder.build();
            if (!catalogLoadingExceptions.isEmpty()) {
                Throwable firstError = catalogLoadingExceptions.removeFirst();
                while (!catalogLoadingExceptions.isEmpty()) {
                    firstError.addSuppressed(catalogLoadingExceptions.getFirst());
                }
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error loading catalogs on worker", firstError);
            }
        }
        finally {
            catalogLoadingLock.unlock();
        }
    }

    @Override
    public void pruneCatalogs(Set<CatalogHandle> catalogsInUse)
    {
        ImmutableList.Builder<Future<CatalogConnector>> futures = ImmutableList.builder();
        catalogRemovingLock.lock();
        try {
            if (stopped) {
                return;
            }

            Set<CatalogHandle> catalogsToPrune = Sets.difference(catalogs.keySet(), catalogsInUse);
            if (catalogsToPrune.isEmpty()) {
                return;
            }

            for (CatalogHandle catalogHandleToPrune : catalogsToPrune) {
                futures.add(catalogs.remove(catalogHandleToPrune));
            }
        }
        finally {
            catalogRemovingLock.unlock();
        }

        for (Future<CatalogConnector> future : futures.build()) {
            switch (future.state()) {
                case RUNNING, SUCCESS -> {
                    try {
                        CatalogConnector catalogConnector = future.get();
                        catalogConnector.shutdown();
                        log.debug("Pruned catalog: %s", catalogConnector.getCatalogHandle());
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error(e, "Error shutting down catalog");
                    }
                    catch (Throwable e) {
                        log.error(e, "Error shutting down catalog");
                    }
                }
                case CANCELLED, FAILED -> {}
            }
        }
    }

    private List<CatalogProperties> getMissingCatalogs(List<CatalogProperties> expectedCatalogs)
    {
        return expectedCatalogs.stream()
                .filter(catalog -> !catalogs.containsKey(catalog.catalogHandle()))
                .collect(toImmutableList());
    }

    @Override
    public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
    {
        Future<CatalogConnector> catalogConnector = catalogs.get(catalogHandle.getRootCatalogHandle());
        checkArgument(catalogConnector != null, "No catalog '%s'", catalogHandle.getCatalogName());

        try {
            // This will synchronize while catalog is loading
            return catalogConnector.get().getMaterializedConnector(catalogHandle.getType());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void registerGlobalSystemConnector(GlobalSystemConnector connector)
    {
        requireNonNull(connector, "connector is null");

        catalogLoadingLock.lock();
        try {
            if (stopped) {
                return;
            }

            CatalogConnector catalog = catalogFactory.createCatalog(GlobalSystemConnector.CATALOG_HANDLE, new ConnectorName(GlobalSystemConnector.NAME), connector);
            if (catalogs.putIfAbsent(GlobalSystemConnector.CATALOG_HANDLE, immediateFuture(catalog)) != null) {
                throw new IllegalStateException("Global system catalog already registered");
            }
        }
        finally {
            catalogLoadingLock.unlock();
        }
    }
}
