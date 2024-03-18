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
package io.trino.plugin.varada.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.classifier.ClassificationType;
import io.trino.plugin.varada.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.juffer.PredicateCacheData;
import io.trino.plugin.varada.juffer.PredicatesCacheService;
import io.trino.plugin.varada.juffer.StorageEngineTxService;
import io.trino.plugin.varada.metrics.CustomStatsContext;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.gen.stats.VaradaStatsDispatcherPageSource;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorAlternativeChooser;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.varada.tools.CatalogNameProvider;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

@Singleton
public class DispatcherAlternativeChooser
        implements ConnectorAlternativeChooser
{
    private static final Logger logger = Logger.get(DispatcherAlternativeChooser.class);

    private final ConnectorPageSourceProvider connectorPageSourceProvider;
    private final DispatcherPageSourceFactory pageSourceFactory;
    private final StorageEngineTxService txService;
    private final MetricsManager metricsManager;
    private final CatalogNameProvider catalogNameProvider;
    private final QueryClassifier queryClassifier;
    private final RowGroupDataService rowGroupDataService;
    private final PredicatesCacheService predicatesCacheService;
    private final NativeStorageStateHandler nativeStorageStateHandler;

    @Inject
    public DispatcherAlternativeChooser(
            @ForWarp ConnectorPageSourceProvider connectorPageSourceProvider,
            DispatcherPageSourceFactory pageSourceFactory,
            StorageEngineTxService txService,
            MetricsManager metricsManager,
            CatalogNameProvider catalogNameProvider,
            QueryClassifier queryClassifier,
            RowGroupDataService rowGroupDataService,
            PredicatesCacheService predicatesCacheService,
            NativeStorageStateHandler nativeStorageStateHandler)
    {
        this.connectorPageSourceProvider = requireNonNull(connectorPageSourceProvider);
        this.pageSourceFactory = requireNonNull(pageSourceFactory);
        this.txService = requireNonNull(txService);
        this.metricsManager = requireNonNull(metricsManager);
        this.catalogNameProvider = requireNonNull(catalogNameProvider);
        this.queryClassifier = requireNonNull(queryClassifier);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.predicatesCacheService = requireNonNull(predicatesCacheService);
        this.nativeStorageStateHandler = requireNonNull(nativeStorageStateHandler);
    }

    @Override
    public Choice chooseAlternative(ConnectorSession session, ConnectorSplit split, List<ConnectorTableHandle> alternatives)
    {
        DispatcherSplit dispatcherSplit = (DispatcherSplit) split;
        RowGroupKey rowGroupKey = rowGroupDataService.createRowGroupKey(
                dispatcherSplit.getSchemaName(),
                dispatcherSplit.getTableName(),
                dispatcherSplit.getPath(),
                dispatcherSplit.getStart(),
                dispatcherSplit.getLength(),
                dispatcherSplit.getFileModifiedTime(),
                dispatcherSplit.getDeletedFilesHash());

        int trivialAlternativeIndex = findAlternativeIndex(alternatives, false);
        if (trivialAlternativeIndex == -1) {
            throw new IllegalStateException("Trivial alternative doesn't exists");
        }
        DispatcherTableHandle trivialAlternative = (DispatcherTableHandle) alternatives.get(trivialAlternativeIndex);

        CustomStatsContext customStatsContext = new CustomStatsContext(metricsManager, trivialAlternative.getCustomStats());
        customStatsContext.getOrRegister(new VaradaStatsDispatcherPageSource(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY));

        Optional<RowGroupCloseHandler> closeHandler = Optional.empty();
        QueryContext queryContext = null;
        int chosenIndex = -1;
        try {
            if (alternatives.size() > 1 && nativeStorageStateHandler.isStorageAvailable()) {
                RowGroupData rowGroupData = rowGroupDataService.getIfPresent(rowGroupKey);
                if (!Objects.isNull(rowGroupData) && !rowGroupData.getValidWarmUpElements().isEmpty()) { // warmed
                    QueryContext basicQueryContext = queryClassifier.getBasicQueryContext(
                            List.of(),  // collect is not needed, we are only interested in predicate pushdown
                            trivialAlternative,
                            DynamicFilter.EMPTY, // not known yet and has no effect on which alternative should be chosen
                            session,
                            ClassificationType.CHOOSING_ALTERNATIVE);

                    if (rowGroupData.getLock().readLock()) {
                        // only need to lock when using the non-trivial alternative
                        closeHandler = Optional.of(new RowGroupCloseHandler());
                        final RowGroupData afterLockRowGroupData = rowGroupDataService.getIfPresent(rowGroupKey); // double-checked locking
                        if (!Objects.isNull(afterLockRowGroupData) && !afterLockRowGroupData.getValidWarmUpElements().isEmpty()) { // still warmed
                            queryContext = queryClassifier.classify(basicQueryContext, afterLockRowGroupData, trivialAlternative, Optional.of(session), Optional.empty(), ClassificationType.CHOOSING_ALTERNATIVE);
                            if ((!queryContext.getMatchLeavesDFS().isEmpty() || queryContext.isNoneOnly()) && // prefer the trivial alternative when Trino won't filter anyway
                                    queryContext.getPredicateContextData().getRemainingColumns().isEmpty() &&
                                    queryContext.isCanBeTight()) {
                                chosenIndex = findAlternativeIndex(alternatives, true);
                            }
                        }
                        if (chosenIndex == -1) {
                            // no need to lock resources when using the trivial alternative (we'll lock later if necessary)
                            closeResources(closeHandler, rowGroupKey, Optional.ofNullable(queryContext));
                            closeHandler = Optional.empty();
                            queryContext = null;
                        }
                        else {
                            VaradaStatsDispatcherPageSource dispatcherPageSourceStats = (VaradaStatsDispatcherPageSource) customStatsContext.getStat(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY);
                            dispatcherPageSourceStats.incnon_trivial_alternative_chosen();
                        }
                    }
                }
            }

            if (chosenIndex == -1) {
                chosenIndex = findAlternativeIndex(alternatives, false);
                if (chosenIndex == -1) {
                    throw new IllegalStateException("Could not find an alternative to use");
                }
            }

            ConnectorTableHandle alternative = alternatives.get(chosenIndex);
            logger.debug("The chosen alternative index is %d. There are %d alternatives in total. alternative=%s", chosenIndex, alternatives.size(), alternative);

            return new Choice(
                    chosenIndex,
                    new DispatcherAlternativePageSourceProvider(
                            connectorPageSourceProvider,
                            pageSourceFactory,
                            txService,
                            customStatsContext,
                            catalogNameProvider,
                            split,
                            alternative,
                            new ResourceCloser(closeHandler, rowGroupKey, Optional.ofNullable(queryContext))));
        }
        catch (Exception e) {
            closeResources(closeHandler, rowGroupKey, Optional.ofNullable(queryContext));
            throw e;
        }
    }

    /**
     * @return alternative's index or -1 if not exists
     */
    private static int findAlternativeIndex(List<ConnectorTableHandle> alternatives, boolean subsumedPredicates)
    {
        for (int i = 0; i < alternatives.size(); i++) {
            DispatcherTableHandle handle = (DispatcherTableHandle) alternatives.get(i);
            if (handle.isSubsumedPredicates() == subsumedPredicates) {
                return i;
            }
        }
        return -1;
    }

    private void closeResources(Optional<RowGroupCloseHandler> closeHandler, RowGroupKey rowGroupKey, Optional<QueryContext> queryContext)
    {
        try {
            closeHandler.ifPresent(value -> {
                RowGroupData rowGroupDataToClose = rowGroupDataService.getIfPresent(rowGroupKey);
                if (rowGroupDataToClose != null) {
                    value.accept(rowGroupDataToClose);
                }
            });
        }
        finally {
            queryContext.ifPresent(value -> {
                try {
                    List<PredicateCacheData> predicateCacheData = value.getMatchLeavesDFS().stream()
                            .map(QueryMatchData::getPredicateCacheData)
                            .toList();
                    predicatesCacheService.markFinished(predicateCacheData);
                }
                finally {
                    queryClassifier.close(value);
                }
            });
        }
    }

    @Override
    // TODO - what is the alternative for the deprecation
    public TupleDomain<ColumnHandle> getUnenforcedPredicate(
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            TupleDomain<ColumnHandle> dynamicFilter)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) table;
        ConnectorSplit connectorSplit = ((DispatcherSplit) split).getProxyConnectorSplit();
        ConnectorTableHandle connectorTableHandle = dispatcherTableHandle.getProxyConnectorTableHandle();
        TupleDomain<ColumnHandle> unenforcedPredicate = connectorPageSourceProvider.getUnenforcedPredicate(session, connectorSplit, connectorTableHandle, dynamicFilter);
        if (unenforcedPredicate.isNone()) {
            // split is fully filtered out
            return TupleDomain.none();
        }

        // Warp speed can apply very large predicates. At this point we also don't know who will
        // serve the split, warp or proxy. Therefore, for correctness we can assume both fullPredicate
        // and dynamic filter are not simplified. However, we can still prune columns which are ineffective
        // in filtering split data.
        return unenforcedPredicate.intersect(connectorPageSourceProvider.prunePredicate(
                session,
                connectorSplit,
                connectorTableHandle,
                dispatcherTableHandle.getFullPredicate().intersect(dynamicFilter)));
    }

    @Override
    public TupleDomain<ColumnHandle> prunePredicate(
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            TupleDomain<ColumnHandle> predicate)
    {
        return connectorPageSourceProvider.prunePredicate(session,
                ((DispatcherSplit) split).getProxyConnectorSplit(),
                ((DispatcherTableHandle) table).getProxyConnectorTableHandle(),
                predicate);
    }

    @Override
    public boolean shouldPerformDynamicRowFiltering()
    {
        return true;
    }

    public class ResourceCloser
    {
        private final Optional<RowGroupCloseHandler> closeHandler;
        private final RowGroupKey rowGroupKey;
        private final Optional<QueryContext> queryContext;
        private final AtomicBoolean handled = new AtomicBoolean();

        public ResourceCloser(Optional<RowGroupCloseHandler> closeHandler, RowGroupKey rowGroupKey, Optional<QueryContext> queryContext)
        {
            this.closeHandler = closeHandler;
            this.rowGroupKey = rowGroupKey;
            this.queryContext = queryContext;
        }

        public void close()
        {
            if (!handled.getAndSet(true)) {
                closeResources(closeHandler, rowGroupKey, queryContext);
            }
        }
    }
}
