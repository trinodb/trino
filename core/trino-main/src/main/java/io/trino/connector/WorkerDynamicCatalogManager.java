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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
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
    private final ConcurrentMap<CatalogHandle, ListenableFuture<CatalogConnector>> catalogs = new ConcurrentHashMap<>();
    private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newVirtualThreadPerTaskExecutor());

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
        Map<CatalogHandle, ListenableFuture<CatalogConnector>> catalogToPrune;

        catalogRemovingLock.lock();
        try {
            if (stopped) {
                return;
            }
            stopped = true;

            catalogToPrune = ImmutableMap.copyOf(this.catalogs);
            this.catalogs.clear();
        }
        finally {
            catalogRemovingLock.unlock();
        }

        pruneCatalogs(catalogToPrune);
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

            List<ListenableFuture<CatalogConnector>> loadingFutures = new ArrayList<>();
            for (CatalogProperties catalogProperties : missingCatalogs) {
                loadingFutures.add(catalogs.computeIfAbsent(catalogProperties.catalogHandle(), ignored -> loadCatalog(catalogProperties)));
            }

            CountDownLatch catalogsLoading = new CountDownLatch(loadingFutures.size());

            List<Throwable> catalogLoadingExceptions = new ArrayList<>();
            for (ListenableFuture<CatalogConnector> catalogFuture : loadingFutures) {
                addCallback(catalogFuture, new FutureCallback<>()
                {
                    @Override
                    public void onSuccess(CatalogConnector result)
                    {
                        catalogsLoading.countDown();
                    }

                    @Override
                    public void onFailure(Throwable throwable)
                    {
                        if (throwable.getCause() != null) {
                            catalogLoadingExceptions.add(throwable.getCause());
                        }
                        else {
                            catalogLoadingExceptions.add(throwable);
                        }

                        catalogsLoading.countDown();
                    }
                }, executor);
            }

            try {
                catalogsLoading.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for catalogs to load", e);
            }

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

    private ListenableFuture<CatalogConnector> loadCatalog(CatalogProperties catalogProperties)
    {
        return executor.submit(() -> {
            CatalogConnector newCatalog = catalogFactory.createCatalog(catalogProperties);
            log.debug("Loaded catalog: " + catalogProperties.catalogHandle());
            return newCatalog;
        });
    }

    @Override
    public void pruneCatalogs(Set<CatalogHandle> catalogsInUse)
    {
        Map<CatalogHandle, ListenableFuture<CatalogConnector>> catalogToPrune = new HashMap<>();
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
                ListenableFuture<CatalogConnector> catalogFuture = catalogs.remove(catalogHandleToPrune);

                if (catalogFuture != null) {
                    catalogToPrune.put(catalogHandleToPrune, catalogFuture);
                }
            }
        }
        finally {
            catalogRemovingLock.unlock();
        }

        pruneCatalogs(catalogToPrune);
    }

    private void pruneCatalogs(Map<CatalogHandle, ListenableFuture<CatalogConnector>> catalogsToPrune)
    {
        CountDownLatch catalogsUnloading = new CountDownLatch(catalogsToPrune.size());
        for (Map.Entry<CatalogHandle, ListenableFuture<CatalogConnector>> entry : catalogsToPrune.entrySet()) {
            addCallback(pruneCatalog(entry.getValue()), new FutureCallback<>()
            {
                @Override
                public void onSuccess(CatalogHandle result)
                {
                    catalogsUnloading.countDown();
                }

                @Override
                public void onFailure(Throwable throwable)
                {
                    log.error(throwable, "Could not prune catalog: %s", entry.getKey());
                }
            }, executor);
        }

        try {
            catalogsUnloading.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while pruning catalogs", e);
        }
    }

    private ListenableFuture<CatalogHandle> pruneCatalog(ListenableFuture<CatalogConnector> catalogFuture)
    {
        return transform(catalogFuture, catalogConnector -> {
            CatalogHandle catalogHandle = catalogConnector.getCatalogHandle();
            catalogConnector.shutdown();
            log.debug("Pruned catalog: %s", catalogHandle);
            return catalogHandle;
        }, executor);
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
        return getCatalogConnector(catalogHandle).getMaterializedConnector(catalogHandle.getType());
    }

    private CatalogConnector getCatalogConnector(CatalogHandle catalogHandle)
    {
        Future<CatalogConnector> catalogConnector = catalogs.get(catalogHandle.getRootCatalogHandle());
        checkArgument(catalogConnector != null, "No catalog '%s'", catalogHandle.getCatalogName());

        try {
            // This will synchronize while catalog is loading
            return catalogConnector.get();
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
