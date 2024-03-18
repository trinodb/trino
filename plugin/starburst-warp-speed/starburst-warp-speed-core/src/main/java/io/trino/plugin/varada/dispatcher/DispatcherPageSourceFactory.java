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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dictionary.DictionaryCacheService;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.MatchCollectUtils.MatchCollectType;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.varada.dispatcher.query.classifier.WarpCacheColumnHandle;
import io.trino.plugin.varada.dispatcher.query.data.QueryColumn;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.juffer.PredicatesCacheService;
import io.trino.plugin.varada.metrics.CustomStatsContext;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.varada.storage.read.ChunksQueueService;
import io.trino.plugin.varada.storage.read.CollectTxService;
import io.trino.plugin.varada.storage.read.PrefilledPageSource;
import io.trino.plugin.varada.storage.read.QueryParams;
import io.trino.plugin.varada.storage.read.RangeFillerService;
import io.trino.plugin.varada.storage.read.StorageCollectorService;
import io.trino.plugin.varada.storage.read.VaradaPageSource;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsDictionary;
import io.trino.plugin.warp.gen.stats.VaradaStatsDispatcherPageSource;
import io.trino.spi.TrinoException;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.varada.log.ShapingLogger;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.varada.storage.read.QueryParamsConverter.createQueryParams;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

@Singleton
public class DispatcherPageSourceFactory
{
    public static final String VARADA_COLLECT = "varada-collect";
    public static final String VARADA_MATCH = "varada-match";
    public static final String EXTERNAL_COLLECT = "external-collect";
    public static final String EXTERNAL_MATCH = "external-match";
    public static final String PREFILLED = "prefilled";
    public static final String STATS_DISPATCHER_KEY = "dispatcherPageSource";
    private static final Logger logger = Logger.get(DispatcherPageSourceFactory.class);
    private final ShapingLogger shapingLogger;
    private final ReadErrorHandler readErrorHandler;
    private final CollectTxService collectTxService;
    private final ChunksQueueService chunksQueueService;
    private final StorageCollectorService storageCollectorService;
    private final RangeFillerService rangeFillerService;
    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final BufferAllocator bufferAllocator;
    private final RowGroupDataService rowGroupDataService;
    private final WorkerWarmingService workerWarmingService;
    private final MetricsManager metricsManager;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final PredicatesCacheService predicatesCacheService;
    private final QueryClassifier queryClassifier;
    private final DictionaryCacheService dictionaryCacheService;

    private final GlobalConfiguration globalConfiguration;
    private final NativeStorageStateHandler nativeStorageStateHandler;
    private final VaradaStatsDispatcherPageSource statsDispatcherPageSource;

    @Inject
    public DispatcherPageSourceFactory(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            RowGroupDataService rowGroupDataService,
            WorkerWarmingService workerWarmingService,
            MetricsManager metricsManager,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            PredicatesCacheService predicatesCacheService,
            QueryClassifier queryClassifier,
            DictionaryCacheService dictionaryCacheService,
            GlobalConfiguration globalConfiguration,
            NativeStorageStateHandler nativeStorageStateHandler,
            ReadErrorHandler readErrorHandler,
            CollectTxService collectTxService,
            ChunksQueueService chunksQueueService,
            StorageCollectorService storageCollectorService,
            RangeFillerService rangeFillerService)
    {
        this.storageEngine = requireNonNull(storageEngine);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.workerWarmingService = requireNonNull(workerWarmingService);
        this.metricsManager = requireNonNull(metricsManager);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.predicatesCacheService = requireNonNull(predicatesCacheService);
        this.queryClassifier = requireNonNull(queryClassifier);
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.nativeStorageStateHandler = requireNonNull(nativeStorageStateHandler);
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfiguration.getShapingLoggerThreshold(),
                globalConfiguration.getShapingLoggerDuration(),
                globalConfiguration.getShapingLoggerNumberOfSamples());
        this.readErrorHandler = requireNonNull(readErrorHandler);
        this.collectTxService = collectTxService;
        this.chunksQueueService = chunksQueueService;
        this.storageCollectorService = storageCollectorService;
        this.rangeFillerService = rangeFillerService;
        metricsManager.registerMetric(VaradaStatsDispatcherPageSource.create(STATS_DISPATCHER_KEY));
        this.statsDispatcherPageSource = metricsManager.registerMetric(VaradaStatsDispatcherPageSource.create(STATS_DISPATCHER_KEY));
    }

    public static String createFixedStatKey(Object... parts)
    {
        StringJoiner sj = new StringJoiner(":");
        for (Object part : parts) {
            sj.add(part.toString());
        }
        return sj.toString();
    }

    public ConnectorPageSource createConnectorPageSource(
            ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherSplit dispatcherSplit,
            DispatcherTableHandle dispatcherTableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            CustomStatsContext customStatsContext)
    {
        if ((!nativeStorageStateHandler.isStorageAvailable() && dispatcherTableHandle.isSubsumedPredicates())) {
            throw new TrinoException(VaradaErrorCode.VARADA_NATIVE_ERROR,
                    "storage is not available");
        }

        if (!nativeStorageStateHandler.isStorageAvailable() ||
                !dispatcherProxiedConnectorTransformer.isValidForAcceleration(dispatcherTableHandle)) {
            logger.debug("Query is not valid for acceleration, reading from proxy connector without warmup. dispatcherTableHandle=%s", dispatcherTableHandle);
            return connectorPageSourceProvider.createPageSource(
                    transactionHandle,
                    session,
                    dispatcherSplit.getProxyConnectorSplit(),
                    dispatcherTableHandle.getProxyConnectorTableHandle(),
                    columns,
                    dynamicFilter);
        }

        initializeCustomStats(customStatsContext);
        ConnectorPageSource connectorPageSource = getConnectorPageSource(
                connectorPageSourceProvider,
                transactionHandle,
                session,
                dispatcherSplit,
                dispatcherTableHandle,
                columns,
                dynamicFilter,
                customStatsContext);

        workerWarmingService.warm(
                connectorPageSourceProvider,
                transactionHandle,
                session,
                dispatcherSplit,
                dispatcherTableHandle,
                columns,
                dynamicFilter,
                1);

        return connectorPageSource;
    }

    ConnectorPageSource getConnectorPageSource(ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherSplit dispatcherSplit,
            DispatcherTableHandle dispatcherTableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            CustomStatsContext customStatsContext)
    {
        // HACK HACK HACK  - to make load a bit faster in POCs and tests
        if (VaradaSessionProperties.isEmptyQuery(session)) {
            return new EmptyPageSource();
        }

        VaradaStatsDispatcherPageSource dispatcherPageSourceStats = (VaradaStatsDispatcherPageSource) customStatsContext.getStat(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY);

        RowGroupKey rowGroupKey = rowGroupDataService.createRowGroupKey(dispatcherSplit.getSchemaName(),
                dispatcherSplit.getTableName(),
                dispatcherSplit.getPath(),
                dispatcherSplit.getStart(),
                dispatcherSplit.getLength(),
                dispatcherSplit.getFileModifiedTime(),
                dispatcherSplit.getDeletedFilesHash());

        RowGroupData rowGroupData = rowGroupDataService.getIfPresent(rowGroupKey);
        PageSourceDecision pageSourceDecision = getBasicPageSourceDecision(
                rowGroupData,
                dispatcherTableHandle,
                columns,
                dynamicFilter,
                dispatcherPageSourceStats);

        if (PageSourceDecision.EMPTY.equals(pageSourceDecision)) {
            dispatcherPageSourceStats.incempty_page_source();
            return new EmptyPageSource();
        }
        if (PageSourceDecision.PREFILL.equals(pageSourceDecision) && columns.isEmpty()) {
            int totalRecords = getTotalRecords(rowGroupData);
            dispatcherPageSourceStats.incempty_collect_columns();
            return new PrefilledPageSource(emptyMap(), dispatcherPageSourceStats, rowGroupData, totalRecords, Optional.empty());
        }

        RowGroupCloseHandler closeHandler = new RowGroupCloseHandler();
        try {
            QueryContext basicQueryContext = queryClassifier.getBasicQueryContext(columns,
                    dispatcherTableHandle,
                    dynamicFilter,
                    session);

            if (PageSourceDecision.PROXY.equals(pageSourceDecision)) {
                addProxiedColumnStats(dispatcherPageSourceStats,
                        customStatsContext,
                        columns,
                        basicQueryContext);

                return createProxiedConnectorPageSource(
                        Optional.empty(),
                        connectorPageSourceProvider,
                        transactionHandle,
                        session,
                        dispatcherSplit,
                        dispatcherTableHandle,
                        columns,
                        dynamicFilter,
                        Optional.empty(),
                        closeHandler,
                        pageSourceDecision);
            }

            final RowGroupData afterLockRowGroupData = rowGroupDataService.getIfPresent(rowGroupKey); // re-fetch the row group since it might have been changed while this flow was in read-lock

            if (logger.isDebugEnabled()) {
                logger.debug("Intersected fullPredicate: %s, dynamicFilter: %s -> into tupleDomain: %s",
                        dispatcherTableHandle.getFullPredicate().toString(session),
                        dynamicFilter.getCurrentPredicate().toString(session),
                        basicQueryContext.getPredicateContextData());
            }

            QueryContext queryContext = queryClassifier.classify(
                    basicQueryContext,
                    afterLockRowGroupData,
                    dispatcherTableHandle,
                    Optional.of(session));

            pageSourceDecision = getPageSourceDecision(queryContext);
            if (PageSourceDecision.EMPTY.equals(pageSourceDecision)) {
                closeHandler.accept(afterLockRowGroupData);
                queryClassifier.close(queryContext);
                addStatsOnFilteredByPredicate(columns, customStatsContext, dispatcherPageSourceStats, basicQueryContext);
                return new EmptyPageSource();
            }

            addColumnStats(customStatsContext, queryContext);

            if (PageSourceDecision.PROXY.equals(pageSourceDecision)) {
                addProxiedColumnStats(dispatcherPageSourceStats,
                        customStatsContext,
                        columns,
                        queryContext);

                return createProxiedConnectorPageSource(
                        Optional.of(afterLockRowGroupData),
                        connectorPageSourceProvider,
                        transactionHandle,
                        session,
                        dispatcherSplit,
                        dispatcherTableHandle,
                        columns,
                        dynamicFilter,
                        Optional.of(queryContext),
                        closeHandler,
                        PageSourceDecision.PROXY);
            }

            checkArgument(queryContext.getTotalRecords() != QueryClassifier.INVALID_TOTAL_RECORDS, "invalid totalRecords, %s", queryContext);
            if (PageSourceDecision.PREFILL.equals(pageSourceDecision)) {
                dispatcherPageSourceStats.addprefilled_collect_columns(columns.size());
                queryClassifier.close(queryContext);
                //in prefill queryContext doesn't hold any WE
                int totalRecords = getTotalRecords(rowGroupData);
                return new PrefilledPageSource(queryContext.getPrefilledQueryCollectDataByBlockIndex(),
                        dispatcherPageSourceStats,
                        afterLockRowGroupData,
                        totalRecords,
                        Optional.of(closeHandler));
            }

            if (PageSourceDecision.MIXED.equals(pageSourceDecision) ||
                    PageSourceDecision.VARADA.equals(pageSourceDecision)) {
                try {
                    increaseMixedCounters(dispatcherPageSourceStats, queryContext);

                    return createMixedPageSource(connectorPageSourceProvider,
                            queryClassifier,
                            queryContext,
                            transactionHandle,
                            dispatcherTableHandle,
                            session,
                            dispatcherSplit,
                            afterLockRowGroupData,
                            pageSourceDecision,
                            customStatsContext,
                            closeHandler);
                }
                catch (Exception e) {
                    if (Thread.interrupted()) {
                        closeHandler.accept(afterLockRowGroupData);
                        Thread.currentThread().interrupt();
                        throw new TrinoException(VaradaErrorCode.VARADA_TX_ALLOCATION_INTERRUPTED,
                                "interrupted while trying to create page source");
                    }
                    shapingLogger.warn(e, "failed createMixedPageSource, returning proxied connector page source");
                    dispatcherPageSourceStats.incexternal_collect_columns();
                }
            }

            return createProxiedConnectorPageSource(
                    Optional.of(afterLockRowGroupData),
                    connectorPageSourceProvider,
                    transactionHandle,
                    session,
                    dispatcherSplit,
                    dispatcherTableHandle,
                    columns,
                    dynamicFilter,
                    Optional.of(queryContext),
                    closeHandler,
                    PageSourceDecision.PROXY);
        }
        catch (Exception e) {
            RowGroupData afterLockRowGroupData = rowGroupDataService.getIfPresent(rowGroupKey);
            if (afterLockRowGroupData != null) {
                closeHandler.accept(afterLockRowGroupData);
            }
            throw e;
        }
    }

    private void updateUsageForEmptyRowGroupData(RowGroupData rowGroupData)
    {
        long lastUsedTimestamp = Instant.now().toEpochMilli();
        rowGroupData.getWarmUpElements().forEach(warmUpElement -> warmUpElement.setLastUsedTimestamp(lastUsedTimestamp));
    }

    private ConnectorPageSource createProxiedConnectorPageSource(
            Optional<RowGroupData> rowGroupData,
            ConnectorPageSourceProvider proxiedConnectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherSplit dispatcherSplit,
            DispatcherTableHandle dispatcherTableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            Optional<QueryContext> queryContext,
            RowGroupCloseHandler closeHandler,
            PageSourceDecision pageSourceDecision)
    {
        rowGroupData.ifPresent(closeHandler);
        queryContext.ifPresent(queryClassifier::close);

        ConnectorTableHandle connectorTableHandle;
        ConnectorSplit proxiedSplit;
        if (PageSourceDecision.PROXY.equals(pageSourceDecision)) {
            connectorTableHandle = dispatcherTableHandle.getProxyConnectorTableHandle();
            proxiedSplit = dispatcherSplit.getProxyConnectorSplit();
        }
        else {
            connectorTableHandle = dispatcherProxiedConnectorTransformer.createProxiedConnectorTableHandleForMixedQuery(dispatcherTableHandle);
            proxiedSplit = dispatcherProxiedConnectorTransformer.createProxiedConnectorNonFilteredSplit(dispatcherSplit.getProxyConnectorSplit());
        }
        return proxiedConnectorPageSourceProvider.createPageSource(transactionHandle,
                session,
                proxiedSplit,
                connectorTableHandle,
                columns,
                dynamicFilter);
    }

    private ConnectorPageSource createMixedPageSource(
            ConnectorPageSourceProvider connectorPageSourceProvider,
            QueryClassifier queryClassifier,
            QueryContext queryContext,
            ConnectorTransactionHandle transactionHandle,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorSession session,
            DispatcherSplit dispatcherSplit,
            RowGroupData rowGroupData,
            PageSourceDecision pageSourceDecision,
            CustomStatsContext customStatsContext,
            RowGroupCloseHandler closeHandler)
    {
        Provider<ConnectorPageSource> proxiedConnectorPageSourceProvider = null;

        if (PageSourceDecision.VARADA.equals(pageSourceDecision)) {
            proxiedConnectorPageSourceProvider = EmptyPageSource::new;
        }
        else if (PageSourceDecision.MIXED.equals(pageSourceDecision)) {
            proxiedConnectorPageSourceProvider = () -> createProxiedConnectorPageSource(
                    Optional.empty(),
                    connectorPageSourceProvider,
                    transactionHandle,
                    session,
                    dispatcherSplit,
                    dispatcherTableHandle,
                    ImmutableList.copyOf(queryContext.getRemainingCollectColumns()),
                    DynamicFilter.EMPTY,
                    Optional.empty(),
                    closeHandler,
                    PageSourceDecision.MIXED);
        }

        boolean isMixedQuery = PageSourceDecision.MIXED.equals(pageSourceDecision);
        String filePath = rowGroupData.getRowGroupKey().stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
        QueryParams queryParams = createQueryParams(queryContext, filePath);
        VaradaPageSource varadaPageSource = new VaradaPageSource(storageEngine,
                storageEngineConstants,
                dispatcherTableHandle.getLimit().orElse(Long.MAX_VALUE),
                bufferAllocator,
                queryParams,
                isMixedQuery,
                predicatesCacheService,
                dictionaryCacheService,
                customStatsContext,
                globalConfiguration,
                collectTxService,
                chunksQueueService,
                storageCollectorService,
                rangeFillerService);

        VaradaStatsDispatcherPageSource pageSourceStats = (VaradaStatsDispatcherPageSource) customStatsContext.getStat(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY);

        return new DispatcherPageSource(proxiedConnectorPageSourceProvider,
                dispatcherProxiedConnectorTransformer,
                queryClassifier,
                varadaPageSource,
                queryContext,
                rowGroupData,
                pageSourceDecision,
                pageSourceStats,
                closeHandler,
                readErrorHandler,
                globalConfiguration);
    }

    // it is assumed that in case all match are proxied,
    // the varada collect is empty. this is done by the parse API
    private PageSourceDecision getBasicPageSourceDecision(
            RowGroupData rowGroupData,
            DispatcherTableHandle dispatcherTableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            VaradaStatsDispatcherPageSource dispatcherPageSourceStats)
    {
        if (Objects.isNull(rowGroupData) ||
                rowGroupData.getValidWarmUpElements().isEmpty()) {
            logger.debug("file isn't warmed, reading from connector");
            dispatcherPageSourceStats.inccached_proxied_files();
            return PageSourceDecision.PROXY;
        }

        TupleDomain<ColumnHandle> fullPredicate = dispatcherTableHandle.getFullPredicate();
        if (fullPredicate.isNone()) {
            updateUsageForEmptyRowGroupData(rowGroupData);
            // Optimization: if the predicate is false, we don't read the split at all.
            // It can help dynamic filtering of inner-joins, if there are no build-side values.
            return PageSourceDecision.EMPTY;
        }

        if (rowGroupData.isEmpty()) {
            dispatcherPageSourceStats.incempty_row_group();
            updateUsageForEmptyRowGroupData(rowGroupData);
            return PageSourceDecision.EMPTY;
        }

        // now we mean business, check if related to varada
        dispatcherPageSourceStats.inccached_files();
        if (!dynamicFilter.getCurrentPredicate().isAll()) {
            dispatcherPageSourceStats.incdf_splits();
        }

        if (columns.isEmpty() &&
                dispatcherTableHandle.getFullPredicate().isAll() &&
                dispatcherTableHandle.getWarpExpression().map(expression -> expression.varadaExpressionDataLeaves().isEmpty()).orElse(true) &&
                dynamicFilter.getCurrentPredicate().isAll()) {
            // covers the case of 'SELECT count(*) FROM t'
            return PageSourceDecision.PREFILL;
        }

        // important to keep this lock here after basic decisions are made
        // if we can't get a read lock, fallback to proxy
        if (!rowGroupData.getLock().readLock()) {
            dispatcherPageSourceStats.inclocked_row_group();
            return PageSourceDecision.PROXY;
        }

        return PageSourceDecision.UNKNOWN;
    }

    private PageSourceDecision getPageSourceDecision(QueryContext queryContext)
    {
        PageSourceDecision pageSourceDecision = PageSourceDecision.MIXED;
        if (queryContext.getMatchLeavesDFS().stream().anyMatch(x -> x.getWarmUpElement().getWarmUpType() == WarmUpType.WARM_UP_TYPE_DATA)) {
            shapingLogger.error("match column contains WARM_UP_TYPE_DATA, invalid state. use proxy connector. %s", queryContext);
            pageSourceDecision = PageSourceDecision.PROXY;
        }
        else if (queryContext.isPrefilledOnly()) {
            logger.debug("all columns are prefilled -> only prefilled. %s", queryContext);
            pageSourceDecision = PageSourceDecision.PREFILL;
        }
        else if (queryContext.isProxyOnly()) {
            logger.debug("non of the required columns are warm -> only proxy. %s", queryContext);
            pageSourceDecision = PageSourceDecision.PROXY;
        }
        else if (queryContext.isNoneOnly()) {
            pageSourceDecision = PageSourceDecision.EMPTY;
        }
        else if (queryContext.isVaradaOnly()) {
            logger.debug("all columns are warmed -> only varada. %s", queryContext);
            pageSourceDecision = PageSourceDecision.VARADA;
        }

        logger.debug("pageSourceDecision=%s for queryContext=%s", pageSourceDecision, queryContext);
        return pageSourceDecision;
    }

    private void increaseMixedCounters(VaradaStatsDispatcherPageSource stats,
            QueryContext queryContext)
    {
        stats.addexternal_collect_columns(queryContext.getRemainingCollectColumnByBlockIndex().size());
        Set<String> varadaMatchColumns = queryContext
                .getMatchLeavesDFS()
                .stream()
                .flatMap(x -> Stream.of(x.getVaradaColumn().getName()))
                .collect(Collectors.toSet());
        Set<String> externalMatchColumns = queryContext
                .getPredicateContextData()
                .getRemainingColumns()
                .stream()
                .flatMap(x -> Stream.of(x.getName()))
                .filter(x -> !varadaMatchColumns.contains(x))
                .collect(Collectors.toSet());
        long externalMatchColumnsCount = externalMatchColumns.size();
        stats.addexternal_match_columns(externalMatchColumnsCount);
        long transformedColumns = queryContext.getMatchLeavesDFS().stream().filter(x -> x.getWarmUpElement().getVaradaColumn().isTransformedColumn()).map(QueryColumn::getVaradaColumn).distinct().count();
        stats.addtransformed_column(transformedColumns);
        stats.addvarada_collect_columns(queryContext.getNativeQueryCollectDataList().size());
        stats.addvarada_match_collect_columns(queryContext.getNativeQueryCollectDataList().stream()
                .filter(nativeQueryCollectData -> !MatchCollectType.DISABLED.equals(nativeQueryCollectData.getMatchCollectType()))
                .count());
        long mappedMatchCollect = queryContext.getNativeQueryCollectDataList().stream()
                .filter(nativeQueryCollectData -> MatchCollectType.MAPPED.equals(nativeQueryCollectData.getMatchCollectType()))
                .count();
        stats.addvarada_mapped_match_collect_columns(mappedMatchCollect);
        stats.addprefilled_collect_columns(queryContext.getPrefilledQueryCollectDataByBlockIndex().size());
        stats.addvarada_match_columns(varadaMatchColumns.size());
        stats.addvarada_match_on_simplified_domain(sumColumns(queryContext.getMatchLeavesDFS().stream().filter(QueryMatchData::isSimplifiedDomain)));

        stats.addcached_total_rows(queryContext.getTotalRecords());
    }

    private int sumColumns(Stream<? extends QueryMatchData> matchLeavesStream)
    {
        return (int) matchLeavesStream.count();
    }

    private void addColumnStats(CustomStatsContext customStatsContext, QueryContext queryContext)
    {
        queryContext.getNativeQueryCollectDataList().forEach((collectData) -> customStatsContext.addFixedStat(createFixedStatKey(VARADA_COLLECT, collectData.getVaradaColumn().getName(), collectData.getWarmUpElement().getWarmUpType()), 1));
        queryContext.getPrefilledQueryCollectDataByBlockIndex().values().forEach((prefilledData) -> customStatsContext.addFixedStat(createFixedStatKey(PREFILLED, prefilledData.getVaradaColumn().getName()), 1));
        queryContext.getMatchLeavesDFS().forEach((matchData) -> customStatsContext.addFixedStat(createFixedStatKey(VARADA_MATCH, matchData.getVaradaColumn().getName(), matchData.getWarmUpElement().getWarmUpType()), 1));
        queryContext.getRemainingCollectColumnByBlockIndex().values().forEach((collectColumnHandle) -> customStatsContext.addFixedStat(createFixedStatKey(EXTERNAL_COLLECT, dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(collectColumnHandle).getName()), 1));
        queryContext.getPredicateContextData()
                .getRemainingColumns()
                .stream()
                .flatMap(x -> Stream.of(x.getName()))
                .distinct()
                .forEach(columnName -> customStatsContext.addFixedStat(createFixedStatKey(EXTERNAL_MATCH, columnName), 1));
    }

    private void addProxiedColumnStats(VaradaStatsDispatcherPageSource globalPageSourceStats,
            CustomStatsContext customStatsContext,
            List<ColumnHandle> columns,
            QueryContext queryContext)
    {
        long externalMatchSize = queryContext.getPredicateContextData()
                .getRemainingColumns()
                .stream()
                .flatMap(x -> Stream.of(x.getName()))
                .distinct()
                .count();
        if (columns.isEmpty()) { //couldn't find any representative column to get from varada
            globalPageSourceStats.incexternal_collect_columns();
        }
        else {
            globalPageSourceStats.addexternal_collect_columns(columns.size());
            globalPageSourceStats.addexternal_match_columns(externalMatchSize);
        }
        customStatsContext.addFixedStat(EXTERNAL_MATCH, externalMatchSize);
        queryContext.getPredicateContextData()
                .getRemainingColumns()
                .forEach(varadaColumn -> customStatsContext.addFixedStat(createFixedStatKey(EXTERNAL_MATCH, varadaColumn.getName()), 1));
        ImmutableList<ColumnHandle> remainingCollectColumns = queryContext.getRemainingCollectColumns();
        if (remainingCollectColumns != null) {
            remainingCollectColumns.forEach(columnHandle ->
                    customStatsContext.addFixedStat(createFixedStatKey(EXTERNAL_COLLECT, dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(columnHandle).getName()), 1));
        }
    }

    private void initializeCustomStats(CustomStatsContext customStatsContext)
    {
        customStatsContext.getOrRegister(new VaradaStatsDispatcherPageSource(DispatcherPageSourceFactory.STATS_DISPATCHER_KEY));
        customStatsContext.getOrRegister(new VaradaStatsDictionary(DictionaryCacheService.DICTIONARY_STAT_GROUP));
    }

    /**
     * case we return emptyPageSource due to predicate filter we mark all collect and match columns as varada
     */
    private void addStatsOnFilteredByPredicate(List<ColumnHandle> columns, CustomStatsContext customStatsContext, VaradaStatsDispatcherPageSource dispatcherPageSourceStats, QueryContext basicQueryContext)
    {
        dispatcherPageSourceStats.incfiltered_by_predicate();
        columns.forEach((columnHandle) -> customStatsContext.addFixedStat(createFixedStatKey(VARADA_COLLECT, dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(columnHandle).getName(), WarmUpType.WARM_UP_TYPE_DATA), 1));
        Set<RegularColumn> varadaMatchColumns = basicQueryContext.getPredicateContextData().getRemainingColumns();
        varadaMatchColumns.forEach(regularColumn -> customStatsContext.addFixedStat(createFixedStatKey(VARADA_MATCH, regularColumn.getName(), WarmUpType.WARM_UP_TYPE_BASIC), 1));
        dispatcherPageSourceStats.addvarada_collect_columns(columns.size());
        dispatcherPageSourceStats.addvarada_match_columns(varadaMatchColumns.size());
    }

    public Optional<ConnectorPageSource> createConnectorPageSource(RowGroupKey rowGroupKey, PlanSignature planSignature)
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        if (rowGroupData == null) {
            return Optional.empty();
        }
        if (rowGroupData.isEmpty()) {
            statsDispatcherPageSource.incwarp_cache_manager();
            statsDispatcherPageSource.incempty_page_source();
            return Optional.of(new EmptyPageSource());
        }

        Optional<UUID> queryStoreId = queryClassifier.getQueryStoreId(rowGroupData, planSignature.getColumns());
        if (queryStoreId.isEmpty()) {
            statsDispatcherPageSource.incskip_warp_cache_manager();
            return Optional.empty();
        }
        ImmutableList.Builder<ColumnHandle> columns = ImmutableList.builder();
        for (int i = 0; i < planSignature.getColumns().size(); i++) {
            columns.add(new WarpCacheColumnHandle(
                    planSignature.getColumns().get(i).toString(),
                    planSignature.getColumnsTypes().get(i)));
        }
        QueryContext queryContext = queryClassifier.classifyCache(columns.build(), queryStoreId, rowGroupData);
        CustomStatsContext customStatsContext = new CustomStatsContext(metricsManager, List.of());
        initializeCustomStats(customStatsContext);
        String filePath = rowGroupData.getRowGroupKey().stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
        QueryParams queryParams = createQueryParams(queryContext, filePath);
        VaradaPageSource varadaPageSource = new VaradaPageSource(storageEngine,
                storageEngineConstants,
                Long.MAX_VALUE,
                bufferAllocator,
                queryParams,
                false,
                predicatesCacheService,
                dictionaryCacheService,
                customStatsContext,
                globalConfiguration,
                collectTxService,
                chunksQueueService,
                storageCollectorService,
                rangeFillerService);
        RowGroupCloseHandler closeHandler = new RowGroupCloseHandler();
        try {
            PageSourceDecision pageSourceDecision = PageSourceDecision.VARADA;
            DispatcherPageSource dispatcherPageSource = new DispatcherPageSource(EmptyPageSource::new,
                    dispatcherProxiedConnectorTransformer,
                    queryClassifier,
                    varadaPageSource,
                    queryContext,
                    rowGroupData,
                    pageSourceDecision,
                    statsDispatcherPageSource,
                    closeHandler,
                    readErrorHandler,
                    globalConfiguration);
            statsDispatcherPageSource.addvarada_collect_columns(planSignature.getColumns().size());
            statsDispatcherPageSource.incwarp_cache_manager();
            return Optional.of(dispatcherPageSource);
        }
        catch (Exception e) {
            RowGroupData afterLockRowGroupData = rowGroupDataService.get(rowGroupKey);
            if (afterLockRowGroupData != null) {
                closeHandler.accept(afterLockRowGroupData);
            }
            throw e;
        }
    }

    /**
     * in prefill queryContext doesn't hold any WE. we take totalRecords from RG and validate its all the same
     */
    private int getTotalRecords(RowGroupData rowGroupData)
    {
        long count = rowGroupData.getValidWarmUpElements().stream().map(WarmUpElement::getTotalRecords).distinct().count();
        checkArgument(count == 1, "total records must be distinct");
        return rowGroupData.getValidWarmUpElements().get(0).getTotalRecords();
    }
}
