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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import jakarta.annotation.PreDestroy;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
    private final ConcurrentMap<CatalogHandle, CatalogConnector> catalogs = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

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
        List<CatalogConnector> catalogs;

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

        for (CatalogConnector connector : catalogs) {
            connector.shutdown();
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
            missingCatalogs.forEach(catalog -> checkArgument(!catalog.getCatalogHandle().equals(GlobalSystemConnector.CATALOG_HANDLE), "Global system catalog not registered"));
            List<ListenableFuture<Void>> loadedCatalogs = Futures.inCompletionOrder(
                    missingCatalogs.stream()
                            .map(catalog ->
                                    Futures.submit(() -> {
                                        catalogs.computeIfAbsent(catalog.getCatalogHandle(), ignore -> {
                                            CatalogConnector newCatalog = catalogFactory.createCatalog(catalog);
                                            log.debug("Added catalog: " + catalog.getCatalogHandle());
                                            return newCatalog;
                                        });
                                    }, executor))
                            .collect(toImmutableList()));

            Deque<Throwable> catalogLoadingExceptions = new LinkedList<>();
            for (ListenableFuture<Void> loadedCatalog : loadedCatalogs) {
                try {
                    loadedCatalog.get();
                }
                catch (ExecutionException e) {
                    if (e.getCause() != null) {
                        catalogLoadingExceptions.add(e.getCause());
                    }
                    else {
                        catalogLoadingExceptions.add(e);
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    catalogLoadingExceptions.add(e);
                }
                finally {
                    loadedCatalog.cancel(true);
                }
            }
            if (!catalogLoadingExceptions.isEmpty()) {
                Throwable firstError = catalogLoadingExceptions.poll();
                while (!catalogLoadingExceptions.isEmpty()) {
                    firstError.addSuppressed(catalogLoadingExceptions.poll());
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
        List<CatalogConnector> removedCatalogs = new ArrayList<>();
        catalogRemovingLock.lock();
        try {
            if (stopped) {
                return;
            }
            Iterator<Entry<CatalogHandle, CatalogConnector>> iterator = catalogs.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<CatalogHandle, CatalogConnector> entry = iterator.next();
                if (!catalogsInUse.contains(entry.getKey())) {
                    iterator.remove();
                    removedCatalogs.add(entry.getValue());
                }
            }
        }
        finally {
            catalogRemovingLock.unlock();
        }

        removedCatalogs.forEach(removedCatalog -> Futures.submit(
                () -> {
                    try {
                        removedCatalog.shutdown();
                        log.debug("Pruned catalog: %s", removedCatalog.getCatalogHandle());
                    }
                    catch (Throwable e) {
                        log.error(e, "Error shutting down catalog: %s".formatted(removedCatalog));
                    }
                },
                executor).state());
    }

    private List<CatalogProperties> getMissingCatalogs(List<CatalogProperties> expectedCatalogs)
    {
        return expectedCatalogs.stream()
                .filter(catalog -> !catalogs.containsKey(catalog.getCatalogHandle()))
                .collect(toImmutableList());
    }

    @Override
    public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
    {
        CatalogConnector catalogConnector = catalogs.get(catalogHandle.getRootCatalogHandle());
        checkArgument(catalogConnector != null, "No catalog '%s'", catalogHandle.getCatalogName());
        return catalogConnector.getMaterializedConnector(catalogHandle.getType());
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
            if (catalogs.putIfAbsent(GlobalSystemConnector.CATALOG_HANDLE, catalog) != null) {
                throw new IllegalStateException("Global system catalog already registered");
            }
        }
        finally {
            catalogLoadingLock.unlock();
        }
    }
}
