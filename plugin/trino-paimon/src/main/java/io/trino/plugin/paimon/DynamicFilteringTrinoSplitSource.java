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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.predicate.TupleDomain;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DynamicFilteringTrinoSplitSource
        implements ConnectorSplitSource
{
    private static final Logger LOG = Logger.get(DynamicFilteringTrinoSplitSource.class);
    private static final List<ConnectorSplit> EMPTY_BATCH = ImmutableList.of();
    private static final List<ConnectorSplit> FINISHED_BATCH = ImmutableList.of();

    private final PaimonTableHandle tableHandle;
    private final ConnectorSession session;
    private final PaimonCatalog paimonCatalog;
    private final DynamicFilter dynamicFilter;
    private final Duration dynamicFilteringWaitTimeout;
    private final long dynamicFilteringWaitStartMillis;

    @GuardedBy("this")
    private boolean splitsPlanningStarted;

    @GuardedBy("this")
    private CompletableFuture<PaimonSplitSource> delegateSplitSourceFuture;

    @GuardedBy("this")
    private PaimonSplitSource delegateSplitSource;

    @GuardedBy("this")
    private boolean closed;

    public DynamicFilteringTrinoSplitSource(
            PaimonTableHandle tableHandle,
            ConnectorSession session,
            PaimonCatalog paimonCatalog,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringWaitTimeout)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.session = requireNonNull(session, "session is null");
        this.paimonCatalog = requireNonNull(paimonCatalog, "paimonCatalog is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.dynamicFilteringWaitTimeout = requireNonNull(dynamicFilteringWaitTimeout, "dynamicFilteringWaitTimeout is null");
        this.dynamicFilteringWaitStartMillis = System.currentTimeMillis();
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        checkArgument(maxSize > 0, "Cannot fetch a batch of zero size");
        long timeLeft = computeTimeLeft();

        boolean planSplits = false;
        CompletableFuture<PaimonSplitSource> splitSourceFuture;
        synchronized (this) {
            if (closed) {
                return CompletableFuture.completedFuture(FINISHED_BATCH);
            }

            // Wait for dynamic filters if not yet started planning
            if (!splitsPlanningStarted && PaimonSplitManager.canApplyDynamicFilter(tableHandle) && dynamicFilter.isAwaitable() && timeLeft > 0) {
                CompletableFuture<?> blocked = dynamicFilter.isBlocked();
                if (!blocked.isDone()) {
                    LOG.debug("Waiting for dynamic filters, time left: %sms", timeLeft);
                    return closeAware(blocked.thenApply(_ -> EMPTY_BATCH)
                            .completeOnTimeout(EMPTY_BATCH, timeLeft, MILLISECONDS));
                }
            }

            // Start split planning if not yet started
            if (!splitsPlanningStarted) {
                splitsPlanningStarted = true;
                delegateSplitSourceFuture = new CompletableFuture<>();
                planSplits = true;
            }
            splitSourceFuture = requireNonNull(delegateSplitSourceFuture, "delegateSplitSourceFuture is null");
        }

        if (!planSplits) {
            return splitSourceFuture.thenCompose(splitSource -> closeAware(splitSource.getNextBatch(maxSize, dynamicFilterSnapshot)));
        }

        PaimonSplitSource plannedSplitSource;
        try {
            plannedSplitSource = planSplits(dynamicFilterSnapshot);
        }
        catch (RuntimeException | Error e) {
            synchronized (this) {
                splitsPlanningStarted = false;
                delegateSplitSourceFuture = null;
            }
            splitSourceFuture.completeExceptionally(e);
            throw e;
        }

        boolean closedAfterPlanning;
        synchronized (this) {
            closedAfterPlanning = closed;
            if (!closedAfterPlanning) {
                delegateSplitSource = plannedSplitSource;
            }
        }
        if (closedAfterPlanning) {
            plannedSplitSource.close();
        }
        splitSourceFuture.complete(plannedSplitSource);
        if (closedAfterPlanning) {
            return CompletableFuture.completedFuture(FINISHED_BATCH);
        }

        return closeAware(plannedSplitSource.getNextBatch(maxSize, dynamicFilterSnapshot));
    }

    @Override
    public void close()
    {
        PaimonSplitSource splitSource;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            splitSource = delegateSplitSource;
        }
        if (splitSource != null) {
            splitSource.close();
        }
    }

    @Override
    public boolean isFinished()
    {
        synchronized (this) {
            if (closed) {
                return true;
            }
            if (!splitsPlanningStarted) {
                return false;
            }
            if (delegateSplitSource == null) {
                return false;
            }
            return delegateSplitSource.isFinished();
        }
    }

    @Override
    public long getRequestedDynamicFilterWaitTimeoutMillis()
    {
        return dynamicFilteringWaitTimeout.toMillis();
    }

    private long computeTimeLeft()
    {
        if (dynamicFilteringWaitTimeout.toMillis() == 0) {
            return 0;
        }
        long elapsedMillis = System.currentTimeMillis() - dynamicFilteringWaitStartMillis;
        return Math.max(0, dynamicFilteringWaitTimeout.toMillis() - elapsedMillis);
    }

    private PaimonSplitSource planSplits(DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        requireNonNull(dynamicFilterSnapshot, "dynamicFilterSnapshot is null");
        TupleDomain<PaimonColumnHandle> effectivePredicate = PaimonSplitManager.effectivePredicate(tableHandle, dynamicFilter);

        // Apply runtime dynamic filter snapshot for empty-build-side pruning
        if (dynamicFilterSnapshot.currentPredicate().isNone()) {
            effectivePredicate = TupleDomain.none();
        }
        else {
            // Only apply snapshot domains for Paimon column handles; ignore others
            TupleDomain<PaimonColumnHandle> snapshotPredicate = dynamicFilterSnapshot.currentPredicate()
                    .filter((columnHandle, _) -> columnHandle instanceof PaimonColumnHandle)
                    .transformKeys(PaimonColumnHandle.class::cast);
            effectivePredicate = TupleDomain.intersect(List.of(effectivePredicate, snapshotPredicate));
        }

        if (PaimonSplitManager.isEmptySplit(effectivePredicate, tableHandle)) {
            return PaimonSplitManager.emptySplitSource(tableHandle);
        }

        try {
            Catalog catalog = paimonCatalog.forSession(session);

            Table table = PaimonTableHandle.schemaAwareReadTable(
                    tableHandle.tableWithDynamicOptions(catalog, session),
                    !tableHandle.usesHistoricalReadSchema(session));
            ReadBuilder readBuilder = table.newReadBuilder();
            PaimonSplitManager.pushPredicate(readBuilder, table, effectivePredicate);
            PaimonSplitManager.pushLimit(readBuilder, tableHandle);
            List<Split> splits = PaimonSplitManager.planScan(readBuilder);

            LOG.debug("Planned %s splits after applying effective filters", splits.size());

            double minimumSplitWeight = PaimonSessionProperties.getMinimumSplitWeight(session);

            return new PaimonSplitSource(PaimonSplitManager.toPaimonSplits(splits, minimumSplitWeight), tableHandle.getLimit());
        }
        catch (UnsupportedOperationException e) {
            throw PaimonSplitManager.unsupportedReadOperation(tableHandle, e);
        }
        catch (RuntimeException e) {
            throw PaimonSplitManager.splitPlanningException(tableHandle, e);
        }
    }

    private CompletableFuture<List<ConnectorSplit>> closeAware(CompletableFuture<List<ConnectorSplit>> future)
    {
        requireNonNull(future, "future is null");
        return future.thenApply(batch -> {
            synchronized (this) {
                if (closed) {
                    return FINISHED_BATCH;
                }
            }
            return batch;
        });
    }
}
