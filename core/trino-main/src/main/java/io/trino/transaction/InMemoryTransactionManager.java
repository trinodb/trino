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
package io.trino.transaction;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.NotInTransactionException;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.CatalogMetadata;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.trino.metadata.CatalogManager.NO_CATALOGS;
import static io.trino.spi.StandardErrorCode.ADMINISTRATIVELY_KILLED;
import static io.trino.spi.StandardErrorCode.AUTOCOMMIT_WRITE_CONFLICT;
import static io.trino.spi.StandardErrorCode.MULTI_CATALOG_WRITE_CONFLICT;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.READ_ONLY_VIOLATION;
import static io.trino.spi.StandardErrorCode.TRANSACTION_ALREADY_ABORTED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;

@ThreadSafe
public class InMemoryTransactionManager
        implements TransactionManager
{
    private static final Logger log = Logger.get(InMemoryTransactionManager.class);

    private final Duration idleTimeout;
    private final int maxFinishingConcurrency;

    private final ConcurrentMap<TransactionId, TransactionMetadata> transactions = new ConcurrentHashMap<>();
    private final CatalogManager catalogManager;
    private final Executor finishingExecutor;

    private InMemoryTransactionManager(Duration idleTimeout, int maxFinishingConcurrency, CatalogManager catalogManager, Executor finishingExecutor)
    {
        this.catalogManager = catalogManager;
        requireNonNull(idleTimeout, "idleTimeout is null");
        checkArgument(maxFinishingConcurrency > 0, "maxFinishingConcurrency must be at least 1");
        requireNonNull(finishingExecutor, "finishingExecutor is null");

        this.idleTimeout = idleTimeout;
        this.maxFinishingConcurrency = maxFinishingConcurrency;
        this.finishingExecutor = finishingExecutor;
    }

    public static TransactionManager create(
            TransactionManagerConfig config,
            ScheduledExecutorService idleCheckExecutor,
            CatalogManager catalogManager,
            Executor finishingExecutor)
    {
        InMemoryTransactionManager transactionManager = new InMemoryTransactionManager(config.getIdleTimeout(), config.getMaxFinishingConcurrency(), catalogManager, finishingExecutor);
        transactionManager.scheduleIdleChecks(config.getIdleCheckInterval(), idleCheckExecutor);
        return transactionManager;
    }

    public static TransactionManager createTestTransactionManager()
    {
        // No idle checks needed
        return new InMemoryTransactionManager(new Duration(1, TimeUnit.DAYS), 1, NO_CATALOGS, directExecutor());
    }

    private void scheduleIdleChecks(Duration idleCheckInterval, ScheduledExecutorService idleCheckExecutor)
    {
        idleCheckExecutor.scheduleWithFixedDelay(() -> {
            try {
                cleanUpExpiredTransactions();
            }
            catch (Throwable t) {
                log.error(t, "Unexpected exception while cleaning up expired transactions");
            }
        }, idleCheckInterval.toMillis(), idleCheckInterval.toMillis(), MILLISECONDS);
    }

    private synchronized void cleanUpExpiredTransactions()
    {
        Iterator<Entry<TransactionId, TransactionMetadata>> iterator = transactions.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<TransactionId, TransactionMetadata> entry = iterator.next();
            if (entry.getValue().isExpired(idleTimeout)) {
                iterator.remove();
                log.info("Removing expired transaction: %s", entry.getKey());
                entry.getValue().asyncAbort();
            }
        }
    }

    @Override
    public boolean transactionExists(TransactionId transactionId)
    {
        return tryGetTransactionMetadata(transactionId).isPresent();
    }

    @Override
    public TransactionInfo getTransactionInfo(TransactionId transactionId)
    {
        return getTransactionMetadata(transactionId).getTransactionInfo();
    }

    @Override
    public Optional<TransactionInfo> getTransactionInfoIfExist(TransactionId transactionId)
    {
        return tryGetTransactionMetadata(transactionId).map(TransactionMetadata::getTransactionInfo);
    }

    @Override
    public List<TransactionInfo> getAllTransactionInfos()
    {
        return transactions.values().stream()
                .map(TransactionMetadata::getTransactionInfo)
                .collect(toImmutableList());
    }

    @Override
    public Set<TransactionId> getTransactionsUsingCatalog(CatalogHandle catalogHandle)
    {
        return transactions.values().stream()
                .filter(transactionMetadata -> transactionMetadata.isUsingCatalog(catalogHandle))
                .map(TransactionMetadata::getTransactionId)
                .collect(toImmutableSet());
    }

    @Override
    public TransactionId beginTransaction(boolean autoCommitContext)
    {
        return beginTransaction(DEFAULT_ISOLATION, DEFAULT_READ_ONLY, autoCommitContext);
    }

    @Override
    public TransactionId beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommitContext)
    {
        TransactionId transactionId = TransactionId.create();
        BoundedExecutor executor = new BoundedExecutor(finishingExecutor, maxFinishingConcurrency);
        TransactionMetadata transactionMetadata = new TransactionMetadata(transactionId, isolationLevel, readOnly, autoCommitContext, catalogManager, executor);
        checkState(transactions.put(transactionId, transactionMetadata) == null, "Duplicate transaction ID: %s", transactionId);
        return transactionId;
    }

    @Override
    public List<CatalogInfo> getCatalogs(TransactionId transactionId)
    {
        return getTransactionMetadata(transactionId).listCatalogs();
    }

    @Override
    public List<CatalogInfo> getActiveCatalogs(TransactionId transactionId)
    {
        return getTransactionMetadata(transactionId).getActiveCatalogs();
    }

    @Override
    public Optional<CatalogHandle> getCatalogHandle(TransactionId transactionId, String catalogName)
    {
        return getTransactionMetadata(transactionId).tryRegisterCatalog(catalogName);
    }

    @Override
    public Optional<CatalogMetadata> getOptionalCatalogMetadata(TransactionId transactionId, String catalogName)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);
        return transactionMetadata.tryRegisterCatalog(catalogName)
                .map(transactionMetadata::getTransactionCatalogMetadata);
    }

    @Override
    public CatalogMetadata getCatalogMetadata(TransactionId transactionId, CatalogHandle catalogHandle)
    {
        return getTransactionMetadata(transactionId).getTransactionCatalogMetadata(catalogHandle);
    }

    @Override
    public CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, CatalogHandle catalogHandle)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadata(transactionId, catalogHandle);
        checkConnectorWrite(transactionId, catalogHandle);
        return catalogMetadata;
    }

    @Override
    public CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, String catalogName)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);

        CatalogHandle catalogHandle = transactionMetadata.tryRegisterCatalog(catalogName)
                .orElseThrow(() -> new TrinoException(NOT_FOUND, "Catalog does not exist: " + catalogName));

        return getCatalogMetadataForWrite(transactionId, catalogHandle);
    }

    @Override
    public ConnectorTransactionHandle getConnectorTransaction(TransactionId transactionId, String catalogName)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);

        CatalogHandle catalogHandle = transactionMetadata.tryRegisterCatalog(catalogName)
                .orElseThrow(() -> new TrinoException(NOT_FOUND, "Catalog does not exist: " + catalogName));

        return transactionMetadata.getTransactionCatalogMetadata(catalogHandle).getTransactionHandleFor(catalogHandle);
    }

    @Override
    public ConnectorTransactionHandle getConnectorTransaction(TransactionId transactionId, CatalogHandle catalogHandle)
    {
        return getCatalogMetadata(transactionId, catalogHandle).getTransactionHandleFor(catalogHandle);
    }

    private void checkConnectorWrite(TransactionId transactionId, CatalogHandle catalogHandle)
    {
        getTransactionMetadata(transactionId).checkConnectorWrite(catalogHandle);
    }

    @Override
    public void checkAndSetActive(TransactionId transactionId)
    {
        TransactionMetadata metadata = getTransactionMetadata(transactionId);
        metadata.checkOpenTransaction();
        metadata.setActive();
    }

    @Override
    public void trySetActive(TransactionId transactionId)
    {
        tryGetTransactionMetadata(transactionId).ifPresent(TransactionMetadata::setActive);
    }

    @Override
    public void trySetInactive(TransactionId transactionId)
    {
        tryGetTransactionMetadata(transactionId).ifPresent(TransactionMetadata::setInactive);
    }

    private TransactionMetadata getTransactionMetadata(TransactionId transactionId)
    {
        TransactionMetadata transactionMetadata = transactions.get(transactionId);
        if (transactionMetadata == null) {
            throw new NotInTransactionException(transactionId);
        }
        return transactionMetadata;
    }

    private Optional<TransactionMetadata> tryGetTransactionMetadata(TransactionId transactionId)
    {
        return Optional.ofNullable(transactions.get(transactionId));
    }

    private ListenableFuture<TransactionMetadata> removeTransactionMetadataAsFuture(TransactionId transactionId)
    {
        TransactionMetadata transactionMetadata = transactions.remove(transactionId);
        if (transactionMetadata == null) {
            return immediateFailedFuture(new NotInTransactionException(transactionId));
        }
        return immediateFuture(transactionMetadata);
    }

    @Override
    public ListenableFuture<Void> asyncCommit(TransactionId transactionId)
    {
        return nonCancellationPropagating(Futures.transformAsync(removeTransactionMetadataAsFuture(transactionId), TransactionMetadata::asyncCommit, directExecutor()));
    }

    @Override
    public ListenableFuture<Void> asyncAbort(TransactionId transactionId)
    {
        return nonCancellationPropagating(Futures.transformAsync(removeTransactionMetadataAsFuture(transactionId), TransactionMetadata::asyncAbort, directExecutor()));
    }

    @Override
    public void blockCommit(TransactionId transactionId, String reason)
    {
        getTransactionMetadata(transactionId).blockCommit(reason);
    }

    @Override
    public void fail(TransactionId transactionId)
    {
        // Mark transaction as failed, but don't remove it.
        tryGetTransactionMetadata(transactionId).ifPresent(TransactionMetadata::asyncAbort);
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    @ThreadSafe
    private static class TransactionMetadata
    {
        private final DateTime createTime = DateTime.now();
        private final CatalogManager catalogManager;
        private final TransactionId transactionId;
        private final IsolationLevel isolationLevel;
        private final boolean readOnly;
        private final boolean autoCommitContext;
        private final Executor finishingExecutor;

        private final AtomicReference<String> commitBlocked = new AtomicReference<>();
        private final AtomicReference<Boolean> completedSuccessfully = new AtomicReference<>();
        private final AtomicReference<Long> idleStartTime = new AtomicReference<>();

        @GuardedBy("this")
        private final Map<String, Optional<Catalog>> registeredCatalogs = new ConcurrentHashMap<>();
        @GuardedBy("this")
        private final Map<CatalogHandle, CatalogMetadata> activeCatalogs = new ConcurrentHashMap<>();
        @GuardedBy("this")
        private final AtomicReference<CatalogHandle> writtenCatalog = new AtomicReference<>();

        public TransactionMetadata(
                TransactionId transactionId,
                IsolationLevel isolationLevel,
                boolean readOnly,
                boolean autoCommitContext,
                CatalogManager catalogManager,
                Executor finishingExecutor)
        {
            this.transactionId = requireNonNull(transactionId, "transactionId is null");
            this.isolationLevel = requireNonNull(isolationLevel, "isolationLevel is null");
            this.readOnly = readOnly;
            this.autoCommitContext = autoCommitContext;
            this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
            this.finishingExecutor = requireNonNull(finishingExecutor, "finishingExecutor is null");
        }

        public TransactionId getTransactionId()
        {
            return transactionId;
        }

        public void setActive()
        {
            idleStartTime.set(null);
        }

        public void setInactive()
        {
            idleStartTime.set(System.nanoTime());
        }

        public boolean isExpired(Duration idleTimeout)
        {
            Long idleStartTime = this.idleStartTime.get();
            return idleStartTime != null && Duration.nanosSince(idleStartTime).compareTo(idleTimeout) > 0;
        }

        public void blockCommit(String reason)
        {
            commitBlocked.set(requireNonNull(reason, "reason is null"));
        }

        public void checkOpenTransaction()
        {
            Boolean completedStatus = this.completedSuccessfully.get();
            if (completedStatus != null) {
                if (completedStatus) {
                    // Should not happen normally
                    throw new IllegalStateException("Current transaction already committed");
                }
                throw new TrinoException(TRANSACTION_ALREADY_ABORTED, "Current transaction is aborted, commands ignored until end of transaction block");
            }
        }

        public synchronized boolean isUsingCatalog(CatalogHandle catalogHandle)
        {
            return registeredCatalogs.values().stream()
                    .flatMap(Optional::stream)
                    .map(Catalog::getCatalogHandle)
                    .anyMatch(catalogHandle::equals);
        }

        private synchronized List<CatalogInfo> getActiveCatalogs()
        {
            return activeCatalogs.keySet().stream()
                    .map(CatalogHandle::getCatalogName)
                    .distinct()
                    .map(key -> registeredCatalogs.getOrDefault(key, Optional.empty()))
                    .flatMap(Optional::stream)
                    .map(catalog -> new CatalogInfo(catalog.getCatalogName(), catalog.getCatalogHandle(), catalog.getConnectorName()))
                    .collect(toImmutableList());
        }

        private synchronized List<CatalogInfo> listCatalogs()
        {
            // register all known catalogs - but don't verify so failed catalogs can be listed
            catalogManager.getCatalogNames()
                    .forEach(catalogName -> registeredCatalogs.computeIfAbsent(catalogName, catalogManager::getCatalog));

            return registeredCatalogs.values().stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(catalog -> new CatalogInfo(catalog.getCatalogName(), catalog.getCatalogHandle(), catalog.getConnectorName()))
                    .collect(toImmutableList());
        }

        private synchronized Optional<CatalogHandle> tryRegisterCatalog(String catalogName)
        {
            Optional<Catalog> catalog = registeredCatalogs.computeIfAbsent(catalogName, catalogManager::getCatalog);
            catalog.ifPresent(Catalog::verify);
            return catalog.map(Catalog::getCatalogHandle);
        }

        private synchronized CatalogMetadata getTransactionCatalogMetadata(CatalogHandle catalogHandle)
        {
            checkOpenTransaction();

            CatalogMetadata catalogMetadata = activeCatalogs.get(catalogHandle.getRootCatalogHandle());
            if (catalogMetadata == null) {
                // catalog name will not be an internal catalog (e.g., information schema) because internal
                // catalog references can only be generated from the main catalog
                checkArgument(!catalogHandle.getType().isInternal(), "Internal catalog handle not allowed: %s", catalogHandle);
                Catalog catalog = registeredCatalogs.getOrDefault(catalogHandle.getCatalogName(), Optional.empty())
                        .orElseThrow(() -> new IllegalArgumentException("No catalog registered for handle: " + catalogHandle));

                catalogMetadata = catalog.beginTransaction(transactionId, isolationLevel, readOnly, autoCommitContext);

                activeCatalogs.put(catalogHandle, catalogMetadata);
            }
            return catalogMetadata;
        }

        public synchronized void checkConnectorWrite(CatalogHandle catalogHandle)
        {
            checkOpenTransaction();
            CatalogMetadata catalogMetadata = activeCatalogs.get(catalogHandle);
            checkArgument(catalogMetadata != null, "Cannot record write for catalog not part of transaction");
            if (readOnly) {
                throw new TrinoException(READ_ONLY_VIOLATION, "Cannot execute write in a read-only transaction");
            }
            if (!writtenCatalog.compareAndSet(null, catalogHandle) && !writtenCatalog.get().equals(catalogHandle)) {
                String writtenCatalogName = activeCatalogs.get(writtenCatalog.get()).getCatalogName();
                throw new TrinoException(MULTI_CATALOG_WRITE_CONFLICT, "Multi-catalog writes not supported in a single transaction. Already wrote to catalog " + writtenCatalogName);
            }
            if (catalogMetadata.isSingleStatementWritesOnly() && !autoCommitContext) {
                throw new TrinoException(AUTOCOMMIT_WRITE_CONFLICT, "Catalog only supports writes using autocommit: " + catalogMetadata.getCatalogName());
            }
        }

        public synchronized ListenableFuture<Void> asyncCommit()
        {
            if (!completedSuccessfully.compareAndSet(null, true)) {
                if (completedSuccessfully.get()) {
                    // Already done
                    return immediateVoidFuture();
                }
                // Transaction already aborted
                return immediateFailedFuture(new TrinoException(TRANSACTION_ALREADY_ABORTED, "Current transaction has already been aborted"));
            }

            String commitBlockedReason = commitBlocked.get();
            if (commitBlockedReason != null) {
                return Futures.transform(
                        abortInternal(),
                        ignored -> {
                            throw new TrinoException(ADMINISTRATIVELY_KILLED, commitBlockedReason);
                        },
                        directExecutor());
            }

            CatalogHandle writeCatalogHandle = this.writtenCatalog.get();
            if (writeCatalogHandle == null) {
                ListenableFuture<Void> future = asVoid(Futures.allAsList(activeCatalogs.values().stream()
                        .map(catalog -> Futures.submit(catalog::commit, finishingExecutor))
                        .collect(toList())));
                addExceptionCallback(future, throwable -> {
                    abortInternal();
                    log.error(throwable, "Read-only connector should not throw exception on commit");
                });
                return nonCancellationPropagating(future);
            }

            Supplier<ListenableFuture<Void>> commitReadOnlyConnectors = () -> {
                List<ListenableFuture<Void>> futures = activeCatalogs.entrySet().stream()
                        .filter(entry -> !entry.getKey().equals(writeCatalogHandle))
                        .map(Entry::getValue)
                        .map(transactionMetadata -> Futures.submit(transactionMetadata::commit, finishingExecutor))
                        .collect(toList());
                ListenableFuture<Void> future = asVoid(Futures.allAsList(futures));
                addExceptionCallback(future, throwable -> log.error(throwable, "Read-only connector should not throw exception on commit"));
                return future;
            };

            CatalogMetadata writeCatalog = activeCatalogs.get(writeCatalogHandle);
            ListenableFuture<Void> commitFuture = Futures.submit(writeCatalog::commit, finishingExecutor);
            ListenableFuture<Void> readOnlyCommitFuture = Futures.transformAsync(commitFuture, ignored -> commitReadOnlyConnectors.get(), directExecutor());
            addExceptionCallback(readOnlyCommitFuture, this::abortInternal);
            return nonCancellationPropagating(readOnlyCommitFuture);
        }

        public synchronized ListenableFuture<Void> asyncAbort()
        {
            if (!completedSuccessfully.compareAndSet(null, false)) {
                if (completedSuccessfully.get()) {
                    // Should not happen normally
                    return immediateFailedFuture(new IllegalStateException("Current transaction already committed"));
                }
                // Already done
                return immediateVoidFuture();
            }
            return abortInternal();
        }

        private synchronized ListenableFuture<Void> abortInternal()
        {
            // the callbacks in statement performed on another thread so are safe
            List<ListenableFuture<Void>> futures = activeCatalogs.values().stream()
                    .map(catalog -> Futures.submit(catalog::abort, finishingExecutor))
                    .collect(toList());
            ListenableFuture<Void> future = asVoid(Futures.allAsList(futures));
            return nonCancellationPropagating(future);
        }

        public TransactionInfo getTransactionInfo()
        {
            Duration idleTime = Optional.ofNullable(idleStartTime.get())
                    .map(Duration::nanosSince)
                    .orElse(new Duration(0, MILLISECONDS));

            // dereferencing this field is safe because the field is atomic, and activeCatalogs is a concurrent map
            @SuppressWarnings("FieldAccessNotGuarded") Optional<String> writtenCatalogName = Optional.ofNullable(this.writtenCatalog.get())
                    .map(activeCatalogs::get)
                    .map(CatalogMetadata::getCatalogName);

            // access here is safe here because the map is concurrent
            @SuppressWarnings("FieldAccessNotGuarded") List<String> catalogNames = activeCatalogs.values().stream()
                    .map(CatalogMetadata::getCatalogName)
                    .sorted()
                    .collect(toUnmodifiableList());

            return new TransactionInfo(
                    transactionId,
                    isolationLevel,
                    readOnly,
                    autoCommitContext,
                    createTime,
                    idleTime,
                    catalogNames,
                    writtenCatalogName,
                    ImmutableSet.copyOf(activeCatalogs.keySet()));
        }
    }
}
