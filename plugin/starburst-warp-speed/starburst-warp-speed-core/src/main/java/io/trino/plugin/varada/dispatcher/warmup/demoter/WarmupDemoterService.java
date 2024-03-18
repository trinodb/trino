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
package io.trino.plugin.varada.dispatcher.warmup.demoter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.configuration.WarmupDemoterConfiguration;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmState;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WildcardColumn;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService;
import io.trino.plugin.varada.dispatcher.warmup.WorkerWarmupRuleService;
import io.trino.plugin.varada.dispatcher.warmup.demoter.events.WarmupDemoterFinishEvent;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.flows.FlowIdGenerator;
import io.trino.plugin.varada.storage.flows.FlowType;
import io.trino.plugin.varada.storage.flows.FlowsSequencer;
import io.trino.plugin.varada.warmup.model.WarmupRule;
import io.trino.plugin.warp.gen.constants.DemoteStatus;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupDemoter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.varada.tools.CatalogNameProvider;
import io.varada.tools.util.StopWatch;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.collections4.CollectionUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.varada.dispatcher.warmup.WarmupProperties.NA_TTL;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;

@SuppressWarnings("ALL")
@Singleton
public class WarmupDemoterService
{
    public static final String WARMUP_DEMOTER_STAT_GROUP = "warmupDemoter";
    public static final int MAX_SUPPORTED_BATCH_SIZE = 100;
    public static final int FAILED_DEMOTE_SQUENCE = -1;
    private static final Logger logger = Logger.get(WarmupDemoterService.class);
    private final WorkerCapacityManager workerCapacityManager;
    private final RowGroupDataService rowGroupDataService;
    private final ConnectorSync connectorSync;
    private final WarmupDemoterConfiguration warmupDemoterConfiguration;
    private final VaradaStatsWarmupDemoter globalStatsDemoter;
    private final ExecutorService rowGroupExecutorService;
    private final FlowsSequencer flowsSequencer;
    private final WorkerWarmupRuleService workerWarmupRuleService;
    private WarmupProperties defaultWarmupProperties;
    private AtomicDouble highestPriority = new AtomicDouble(0);
    private AtomicBoolean isExecuting = new AtomicBoolean(false);
    private long lastExecutionTime = -1;
    private CatalogNameProvider catalogNameProvider;
    private List<TupleFilter> tupleFilters;
    private boolean forceDeleteDeadObjects;
    private boolean forceDeleteFailedObjects;
    private boolean resetHigestPriority;
    private boolean deleteEmptyRowGroups;
    private boolean enableDemote;
    private EventBus eventBus;
    private DemoteArguments demoteArguments;

    @Inject
    public WarmupDemoterService(WorkerCapacityManager workerCapacityManager,
            RowGroupDataService rowGroupDataService,
            WorkerWarmupRuleService workerWarmupRuleService,
            WarmupDemoterConfiguration warmupDemoterConfiguration,
            NativeConfiguration nativeConfiguration,
            MetricsManager metricsManager,
            FlowsSequencer flowsSequencer,
            ConnectorSync connectorSync,
            CatalogNameProvider catalogNameProvider,
            EventBus eventBus)
    {
        this.workerCapacityManager = requireNonNull(workerCapacityManager);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.workerWarmupRuleService = requireNonNull(workerWarmupRuleService);
        this.warmupDemoterConfiguration = requireNonNull(warmupDemoterConfiguration);
        this.globalStatsDemoter = (VaradaStatsWarmupDemoter) metricsManager.registerMetric(VaradaStatsWarmupDemoter.create(WARMUP_DEMOTER_STAT_GROUP));
        this.flowsSequencer = requireNonNull(flowsSequencer);
        this.connectorSync = requireNonNull(connectorSync);
        this.catalogNameProvider = requireNonNull(catalogNameProvider);
        this.eventBus = requireNonNull(eventBus);
        this.defaultWarmupProperties = new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, warmupDemoterConfiguration.getDefaultRulePriority(), NA_TTL, TransformFunction.NONE);
        this.enableDemote = warmupDemoterConfiguration.isEnableDemote();
        int rowGroupPoolSize = nativeConfiguration.getTaskMaxWorkerThreads();
        int rowGroupQueueSize = warmupDemoterConfiguration.getTasksExecutorQueueSize();
        long rowGroupKeepAliveTTL = Math.min(warmupDemoterConfiguration.getTasksExecutorKeepAliveTtl(), 1000);
        this.rowGroupExecutorService = new ThreadPoolExecutor(rowGroupPoolSize, rowGroupPoolSize,
                rowGroupKeepAliveTTL, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(rowGroupQueueSize));
        init();
    }

    private void init()
    {
        logger.debug("WarmupDemoterService init = %d", System.identityHashCode(connectorSync));
        connectorSync.setWarmupDemoterService(this);
    }

    //called from warmup - async
    public int tryDemoteStart()
    {
        return tryDemoteStart(null);
    }

    public int tryDemoteStart(List<TupleFilter> tupleFilters)
    {
        if (!enableDemote) {
            return FAILED_DEMOTE_SQUENCE;
        }

        if (this.tupleFilters == null) {
            this.tupleFilters = tupleFilters;
        }
        else if (tupleFilters != null) {
            this.tupleFilters = Streams.concat(this.tupleFilters.stream(), tupleFilters.stream())
                    .collect(Collectors.toList());
        }

        int demoteSequence;
        try {
            validateInput();
            demoteSequence = initiateDemoteProcess();
        }
        catch (Exception e) {
            logger.error(e);
            globalStatsDemoter.incnumber_of_runs_fail();
            demoteSequence = FAILED_DEMOTE_SQUENCE;
        }

        return demoteSequence;
    }

    int initiateDemoteProcess()
    {
        globalStatsDemoter.incnumber_of_calls();
        if (isExecuting.get() || demoteArguments != null) {
            logger.debug("%s: is already executing (isExecuting = %b, demoteArguments = %s)", catalogNameProvider.get(), isExecuting.get(), demoteArguments);
            globalStatsDemoter.incnot_executed_due_is_already_executing();
            return FAILED_DEMOTE_SQUENCE;
        }

        if (CollectionUtils.isEmpty(tupleFilters)
                && !forceDeleteDeadObjects
                && !forceDeleteFailedObjects
                && !reachedThreshold(warmupDemoterConfiguration.getMaxUsageThresholdPercentage())) {
            logger.debug("%s: not executing due thresholds", catalogNameProvider.get());
            globalStatsDemoter.incnot_executed_due_threshold();
            return FAILED_DEMOTE_SQUENCE;
        }

        logger.debug("%s: call start demote", catalogNameProvider.get());
        int demoterSequence = connectorSync.syncDemotePrepare(warmupDemoterConfiguration.getEpsilon());
        if (demoterSequence == FAILED_DEMOTE_SQUENCE) {
            logger.warn("%s: active demoter was initiated by another connector", catalogNameProvider.get());
            globalStatsDemoter.incnot_executed_due_sync_demote_start_rejected();
            isExecuting.set(false);
            return FAILED_DEMOTE_SQUENCE;
        }
        logger.debug("%s: call syncDemoteStart startDemote with seqId = %d", catalogNameProvider.get(), demoterSequence);
        connectorSync.startDemote(demoterSequence);
        return demoterSequence;
    }

    public void connectorSyncStartDemote(int demoterSequence)
    {
        logger.debug("%s: startDemote(demoterSequence = %d)", catalogNameProvider.get(), demoterSequence);
        try {
            if (demoteArguments != null && demoteArguments.demoterSequence != demoterSequence) {
                logger.debug("%s: abortActiveDemote(demoteArguments.demoterSequence = %d)", catalogNameProvider.get(), demoteArguments.demoterSequence);
                abortActiveDemote();
            }
            workerCapacityManager.setCurrentUsage();

            initDemoteArguments(demoterSequence);

            executeDemote(demoterSequence);
            highestPriority.set(0);
            logger.debug("%s: call demoteCycleEnd, demoterSequence = %d", catalogNameProvider.get(), demoteArguments.demoterSequence);

            demoteCycleEnd();
        }
        catch (Exception e) {
            if (e instanceof TrinoException) {
                logger.error("%s: startDemoteCycle failed with error: %s", catalogNameProvider.get(), ((TrinoException) e).getErrorCode().getName());
            }
            else {
                logger.error(e);
            }
            cancelDemoteExecution();
        }
    }

    void cancelDemoteExecution()
    {
        logger.error("failed to execute demote, call native to cancel demote with sequenceId = %d", demoteArguments.demoterSequence);
        connectorSync.syncDemoteCycleEnd(demoteArguments.demoterSequence,
                demoteArguments.getLowestPriority(),
                highestPriority.get(),
                DemoteStatus.DEMOTE_STATUS_REACHED_THRESHOLD);
        globalStatsDemoter.incnumber_of_runs_fail();
    }

    public void connectorSyncDemoteEnd(int demoteSequence, double highestPriority)
    {
        logger.debug("%s: demoteEnd(demoteSequence = %d, highestPriority = %f)", catalogNameProvider.get(), demoteSequence, highestPriority);
        this.highestPriority.set(resetHigestPriority ? 0 : highestPriority);
        if (isExecuting()) {
            globalStatsDemoter.incnumber_of_runs();
            demoteArguments.stopWatch.stop();
            deleteEmptyRowGroups = false;
            isExecuting.set(false);
            flowsSequencer.flowFinished(FlowType.WARMUP_DEMOTER, demoteArguments.flowId, true);
            fireEventDemoteEnd(true);
            demoteArguments = null;
        }
        else {
            logger.error("demote end was called by another procces (demoteSequence = %d), or demote was canceled", demoteSequence);
        }
    }

    private void fireEventDemoteEnd(boolean success)
    {
        logger.debug("fire event demote end");

        if (demoteArguments != null) {
            globalStatsDemoter.mergeStats(demoteArguments.statsWarmupDemoter);
            globalStatsDemoter.addnumber_of_cycles(demoteArguments.numberOfCycles);
            WarmupDemoterFinishEvent event = new WarmupDemoterFinishEvent(demoteArguments.demoterSequence, success, demoteArguments.statsWarmupDemoter.statsCounterMapper());
            eventBus.post(event);
        }
    }

    private void executeDemote(int demoterSequence)
            throws ExecutionException, InterruptedException
    {
        logger.debug("execute demote - demoteSequence = %d", demoterSequence);
        demoteArguments.flowId = FlowIdGenerator.generateFlowId();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        CompletableFuture<Boolean> future = flowsSequencer.tryRunningFlow(FlowType.WARMUP_DEMOTER, demoteArguments.flowId, Optional.empty());
        future.get();
        stopWatch.stop();
        globalStatsDemoter.addwaiting_for_lock_nano(stopWatch.getNanoTime());
        logger.debug("got key, start demote nano sec waited = %d", stopWatch.getNanoTime());
        List<TupleRank> immediateObjects = new ArrayList<>();
        List<TupleRank> failedObjects = new ArrayList<>();
        List<RowGroupData> rowGroupData = rowGroupDataService.getAll();
        List<WarmupRule> warmupRules = workerWarmupRuleService.fetchRulesFromCoordinator();
        logger.debug("%s: build tupleRank", catalogNameProvider.get());
        buildTupleRank(warmupRules,
                rowGroupData,
                immediateObjects,
                failedObjects,
                demoteArguments.tupleRankList,
                tupleFilters);
        if (forceDeleteFailedObjects) {
            // In case of forceDeleteFailedObjects = false, failed objects will be deleted regulary (as dead objects \ low priority)
            logger.debug("%s: deleteFailedObjects = %d", catalogNameProvider.get(), failedObjects.size());
            deleteFailedObjects(failedObjects, demoteArguments.statsWarmupDemoter);
        }
        logger.debug("%s: deleteImmediateObjects = %d", catalogNameProvider.get(), immediateObjects.size());
        deleteImmediateObjects(immediateObjects, demoteArguments.statsWarmupDemoter);
        sortTupleRankCollection(demoteArguments.tupleRankList);
        tupleFilters = null;
    }

    void initDemoteArguments(int demoterSequence)
    {
        isExecuting.set(true);
        lastExecutionTime = System.currentTimeMillis();
        int batchSize = Math.min(MAX_SUPPORTED_BATCH_SIZE, warmupDemoterConfiguration.getBatchSize());
        demoteArguments = new DemoteArguments(demoterSequence,
                warmupDemoterConfiguration.getMaxUsageThresholdPercentage(),
                warmupDemoterConfiguration.getCleanupUsageThresholdPercentage(),
                batchSize,
                warmupDemoterConfiguration.getMaxElementsToDemoteInIteration(),
                warmupDemoterConfiguration.getEpsilon(),
                deleteEmptyRowGroups);
        logger.debug("%s: initDemoteArguments: %s", catalogNameProvider.get(), demoteArguments);
    }

    private void demoteCycleEnd()
    {
        DemoteStatus demoteStatus;
        if (demoteArguments.tupleRankList.isEmpty()) {
            this.highestPriority.set(0);
            demoteStatus = DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE;
            if (CollectionUtils.isEmpty(tupleFilters)) {
                resetHighestPriority();
            }
        }
        else if (reachedThreshold(demoteArguments.cleanupUsageThresholdPercentage)) {
            demoteStatus = DemoteStatus.DEMOTE_STATUS_NOT_COMPLETED;
        }
        else {
            demoteStatus = DemoteStatus.DEMOTE_STATUS_REACHED_THRESHOLD;
        }
        logger.debug("%s: demoteCycleEnd: call connectorSync.syncDemoteCycleEnd (demoteSequence = %d, lowestPriority = %f, highestPriority = %f, demoteStatus = %s)",
                catalogNameProvider.get(), demoteArguments.demoterSequence, demoteArguments.getLowestPriority(), highestPriority.get(), demoteStatus.name());
        connectorSync.syncDemoteCycleEnd(demoteArguments.demoterSequence, demoteArguments.getLowestPriority(), highestPriority.get(), demoteStatus);
    }

    void abortActiveDemote()
    {
        logger.error("demote was aborted, demoteArguments = %s", demoteArguments);
        deleteEmptyRowGroups = false;
        isExecuting.set(false);
        demoteArguments = null;
        globalStatsDemoter.incnumber_of_runs_fail();
        fireEventDemoteEnd(false);
    }

    public void connectorSyncStartDemoteCycle(double maxPriorityToDemote, boolean isSingleConnector)
    {
        logger.debug("%s: startDemoteCycle (maxPriorityToDemote = %f, isSingleConnector = %b) tupleRankList.size %d",
                catalogNameProvider.get(), maxPriorityToDemote, isSingleConnector, demoteArguments.tupleRankList.size());
        try {
            maxPriorityToDemote = isSingleConnector ? Integer.MAX_VALUE : maxPriorityToDemote;
            demoteArguments.increaseNumberOfCycles();
            if (!hasElementsToDemote(maxPriorityToDemote)) {
                logger.error("should not call demote to this connector");
            }
            boolean reachedThreshold = demoteCycle(maxPriorityToDemote, isSingleConnector);
            DemoteStatus demoteStatus;
            if (reachedThreshold) {
                demoteStatus = DemoteStatus.DEMOTE_STATUS_REACHED_THRESHOLD;
            }
            else if (demoteArguments.tupleRankList.isEmpty()) {
                demoteStatus = DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE;
                if (CollectionUtils.isEmpty(tupleFilters)) {
                    resetHighestPriority();
                }
            }
            else {
                demoteStatus = DemoteStatus.DEMOTE_STATUS_NOT_COMPLETED;
            }
            double lowestPriority = demoteArguments.getLowestPriority();
            connectorSync.syncDemoteCycleEnd(demoteArguments.demoterSequence, lowestPriority, highestPriority.get(), demoteStatus);
        }
        catch (Exception e) {
            if (e instanceof TrinoException) {
                logger.error("%s: startDemoteCycle failed with error: %s", catalogNameProvider.get(), ((TrinoException) e).getErrorCode().getName());
            }
            else {
                logger.error(e);
            }
            cancelDemoteExecution();
        }
    }

    private boolean demoteCycle(double maxPriorityToDemote, boolean isSingleConnector)
            throws ExecutionException, InterruptedException
    {
        long elementsDeleted = 0;
        boolean reachedThreshold = false;
        logger.debug("%s: demoteCycle: start demoteCycle", catalogNameProvider.get());
        while (shouldContinueDemote(reachedThreshold, isSingleConnector, elementsDeleted, maxPriorityToDemote)) {
            if (reachedThreshold(demoteArguments.cleanupUsageThresholdPercentage)) {
                long maxElemntsToDemote = isSingleConnector ? Integer.MAX_VALUE : demoteArguments.maxElementsToDemote;
                long numberOfElementsToDemote = Math.min(maxElemntsToDemote - elementsDeleted, demoteArguments.batchSize);
                long newElementsDeleted = deleteByWarmupElement(numberOfElementsToDemote, maxPriorityToDemote);
                elementsDeleted += newElementsDeleted;
            }
            else {
                reachedThreshold = true;
                logger.warn("%s: demoteCycle: while loop: reached threshold", catalogNameProvider.get());
            }
        }
        logger.debug("%s: demoteCycle: tupleRankListSize = %d, elementsDeleted = %d, maxElementsToDemote = %d, maxPriorityToDemote = %f, lowestPriorityLeft = %f",
                catalogNameProvider.get(), demoteArguments.tupleRankList.size(), elementsDeleted, demoteArguments.maxElementsToDemote, maxPriorityToDemote, demoteArguments.getLowestPriority());
        return reachedThreshold;
    }

    private boolean shouldContinueDemote(boolean reachedThreshold, boolean isSingleConnector, long elementsDeleted, double maxPriorityToDemote)
    {
        if (reachedThreshold || demoteArguments.tupleRankList.isEmpty()) {
            return false;
        }
        if (isSingleConnector) {
            return true;
        }
        if (elementsDeleted < demoteArguments.maxElementsToDemote &&
                demoteArguments.tupleRankList.get(0).warmupProperties().priority() < maxPriorityToDemote) {
            return true;
        }
        return false;
    }

    private void validateInput()
    {
        if (warmupDemoterConfiguration.getBatchSize() < 1) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        if (warmupDemoterConfiguration.getEpsilon() <= 0) {
            throw new IllegalArgumentException("epsilon must be greater than 0");
        }
        if (warmupDemoterConfiguration.getMaxElementsToDemoteInIteration() < 1) {
            throw new IllegalArgumentException("maxElementsToDemote must be greater than 0");
        }
    }

    private boolean hasElementsToDemote(double priority)
    {
        return !demoteArguments.tupleRankList.isEmpty() && demoteArguments.getLowestPriority() < priority;
    }

    private boolean reachedThreshold(double usageThresholdPercenatge)
    {
        return workerCapacityManager.getFractionCurrentUsageFromTotal() > parseFromPercentageToFraction(usageThresholdPercenatge);
    }

    private double parseFromPercentageToFraction(double percentage)
    {
        return percentage / 100;
    }

    public boolean canAllowDefaultWarmup()
    {
        return canAllowWarmup(defaultWarmupProperties.priority());
    }

    public boolean canAllowWarmup(double priority)
    {
        return priority >= (highestPriority.get() - warmupDemoterConfiguration.getWarmingPriorityAllowThreshold());
    }

    public boolean canAllowWarmup()
    {
        return !reachedThreshold(warmupDemoterConfiguration.getMaxUsageThresholdPercentage());
    }

    public synchronized AcquireWarmupStatus tryAllocateNativeResourceForWarmup()
    {
        AcquireResult res = connectorSync.tryAcquireAllocation();
        if (!res.isSuccess()) {
            return AcquireWarmupStatus.EXCEEDED_LOADERS;
        }
        workerCapacityManager.setCurrentUsage();
        workerCapacityManager.setExecutingTx((int) res.numberOfActiveThreads());
        if (!canAllowWarmup()) {
            releaseTx();
            if (workerCapacityManager.getExecutingTxCount() <= 0) {
                tryDemoteStart();
            }
            return AcquireWarmupStatus.REACHED_THRESHOLD;
        }
        return AcquireWarmupStatus.SUCCESS;
    }

    public synchronized void tryAllocateTx()
    {
        AcquireResult res = connectorSync.tryAcquireAllocation();
        if (!res.isSuccess()) {
            throw new TrinoException(VaradaErrorCode.VARADA_EXCEEDED_LOADERS, "failed to Acquire loader");
        }
        workerCapacityManager.setCurrentUsage();
        workerCapacityManager.setExecutingTx((int) res.numberOfActiveThreads());
    }

    public void releaseTx()
    {
        workerCapacityManager.decreaseExecutingTx();
        globalStatsDemoter.addreserved_tx(-1);
    }

    @VisibleForTesting
    void buildTupleRank(List<WarmupRule> allRules,
            List<RowGroupData> rowGroupDataList,
            List<TupleRank> immediateObjects,
            List<TupleRank> failedObjects,
            List<TupleRank> tupleRankList,
            List<TupleFilter> tupleFilters)
    {
        Map<SchemaTableColumn, List<WarmupRule>> schemaTableColumnToRulesMap = allRules.stream()
                .collect(groupingBy(warmupRule -> new SchemaTableColumn(
                        new SchemaTableName(warmupRule.getSchema(),
                                warmupRule.getTable()),
                        warmupRule.getVaradaColumn())));
        Instant now = Instant.now();
        // all tupleRanks of the same shared rowGroup should be gathered together to reduce the nunmber of saved
        // the key of this map make sure that all rg+WarmupType will be hanndled in a single batch
        Map<String, TupleRank> tupleRanksByKey = new TreeMap<>();

        for (RowGroupData rowGroupData : rowGroupDataList) {
            RowGroupKey rowGroupKey = rowGroupData.getRowGroupKey();

            for (WarmUpElement warmUpElement : rowGroupData.getWarmUpElements()) {
                if (WarmState.WARM.equals(warmUpElement.getWarmState())) {
                    continue; // already demoted
                }
                if (CollectionUtils.isNotEmpty(tupleFilters) &&
                        tupleFilters.stream().anyMatch(filter -> !filter.shouldHandle(warmUpElement, rowGroupKey))) {
                    continue;
                }

                List<WarmupRule> warmupRuleList = findExistingWarmupElementRules(rowGroupKey, schemaTableColumnToRulesMap, warmUpElement);
                Stream<WarmupRule> wildcardWarmupRules = schemaTableColumnToRulesMap.getOrDefault(
                                new SchemaTableColumn(
                                        new SchemaTableName(rowGroupKey.schema(),
                                                rowGroupKey.table()),
                                        new WildcardColumn()),
                                List.of())
                        .stream()
                        .map(warmupRule -> WarmupRule.builder(warmupRule).varadaColumn(warmUpElement.getVaradaColumn()).build());

                WarmupProperties warmupProperties = findMostRelevantRulePropertiesForWarmupElement(rowGroupData,
                        warmUpElement,
                        Stream.concat(warmupRuleList.stream(), wildcardWarmupRules).toList());

                String warmupElementKey = rowGroupKey + "_" + warmUpElement.getVaradaColumn().getColumnId() + "_" + warmUpElement.getWarmUpType();
                tupleRanksByKey.put(warmupElementKey, new TupleRank(warmupProperties, warmUpElement, rowGroupKey));
            }
        }

        for (TupleRank tupleRank : tupleRanksByKey.values()) {
            WarmUpElement warmUpElement = tupleRank.warmUpElement();
            WarmupProperties warmupProperties = tupleRank.warmupProperties();

            if (forceDeleteFailedObjects && !warmUpElement.isValid()) {
                logger.debug("add failed warmupElement to failedObjects: varadaColumn = %s, warmupType = %s", warmUpElement.getVaradaColumn(), warmupProperties.warmUpType().name());
                failedObjects.add(tupleRank);
            }
            else if (isDeleteImmediatelyObject(tupleRank, now, tupleFilters)) {
                logger.debug("add warmupElement to ImmediateObject: varadaColumn = %s, warmupType = %s, ttl = %s", warmUpElement.getVaradaColumn(), warmupProperties.warmUpType().name(), warmupProperties.ttl());
                immediateObjects.add(tupleRank);
            }
            else {
                tupleRankList.add(tupleRank);
                logger.debug("add warmupElement to tupleRank: varadaColumn = %s, warmupType = %s, priority = %s", warmUpElement.getVaradaColumn(), warmupProperties.warmUpType().name(), warmupProperties.priority());
            }
        }
        logger.debug("buildTupleRank allRules.size %d rowGroupDataList.size %d tupleFilters.size %d failedObjects.size %d, immediateObjects.size %d, tupleRankList.size %d",
                allRules.size(), rowGroupDataList.size(), (tupleFilters != null) ? tupleFilters.size() : -1, failedObjects.size(), immediateObjects.size(), tupleRankList.size());
    }

    public Optional<WarmupRule> findMostRelevantRuleForWarmupElement(RowGroupData rowGroupData,
            WarmUpElement warmUpElement,
            List<WarmupRule> rulesForWarmupElement)
    {
        Map<RegularColumn, String> partitionKeys = rowGroupData
                .getPartitionKeys()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> (RegularColumn) entry.getKey(),
                        entry -> entry.getValue()));
        return Objects.nonNull(rulesForWarmupElement) ?
                rulesForWarmupElement.stream()
                        .filter(warmupRule -> warmUpElement.getWarmUpType() == warmupRule.getWarmUpType())
                        .filter(warmupRule -> (CollectionUtils.isEmpty(warmupRule.getPredicates()) ||
                                warmupRule.getPredicates().stream().allMatch(warmupPredicateRule -> warmupPredicateRule.test(partitionKeys))))
                        .sorted(WorkerWarmingService.warmupRuleComparator.reversed())
                        .findFirst()
//                        .max(Comparator.comparing(WarmupRule::getPriority))
                : Optional.empty();
    }

    private WarmupProperties findMostRelevantRulePropertiesForWarmupElement(RowGroupData rowGroupData,
            WarmUpElement warmUpElement,
            List<WarmupRule> rulesForWarmupElement)
    {
        Optional<WarmupRule> optionalWarmupRule = findMostRelevantRuleForWarmupElement(rowGroupData, warmUpElement, rulesForWarmupElement);
        return optionalWarmupRule.map(warmupRule -> new WarmupProperties(warmupRule.getWarmUpType(), warmupRule.getPriority(), warmupRule.getTtl(), TransformFunction.NONE))
                .orElse(defaultWarmupProperties);
    }

    private boolean isDeleteImmediatelyObject(TupleRank tupleRank, Instant currentTime, List<TupleFilter> tupleFilters)
    {
        return CollectionUtils.isNotEmpty(tupleFilters) || // since tuppleRanks were already filtered by tupleFilters
                ((tupleRank.warmupProperties().ttl() > NA_TTL) &&
                        (tupleRank.warmupProperties().ttl() == 0 ||
                                currentTime.isAfter(Instant.ofEpochMilli(tupleRank.warmUpElement().getLastUsedTimestamp())
                                        .plus(tupleRank.warmupProperties().ttl(), ChronoUnit.SECONDS))));
    }

    private void deleteImmediateObjects(List<TupleRank> tupleRanks, VaradaStatsWarmupDemoter statsWarmupDemoter)
            throws ExecutionException, InterruptedException
    {
        logger.debug("start deleting %d immediate objects", tupleRanks.size());
        long deletedObjectsCount = delete(tupleRanks);
        statsWarmupDemoter.adddead_objects_deleted(deletedObjectsCount);
    }

    private void deleteFailedObjects(List<TupleRank> failedObjects, VaradaStatsWarmupDemoter statsWarmupDemoter)
            throws ExecutionException, InterruptedException
    {
        logger.debug("start deleting %d failed objects", failedObjects.size());
        long deletedObjectsCount = delete(failedObjects);
        statsWarmupDemoter.addfailed_objects_deleted(deletedObjectsCount);
    }

    private long deleteByWarmupElement(long maxElementsToDemote, double maxPriorityToDemote)
            throws ExecutionException, InterruptedException
    {
        if (demoteArguments.tupleRankList.isEmpty()) {
            return 0;
        }
        int toIndex = 0;
        List<TupleRank> elementsToDemote = new ArrayList<>();
        while (demoteArguments.tupleRankList.size() > toIndex && toIndex < maxElementsToDemote &&
                demoteArguments.tupleRankList.get(toIndex).warmupProperties().priority() < maxPriorityToDemote) {
            elementsToDemote.add(demoteArguments.tupleRankList.get(toIndex));
            toIndex++;
        }
        long deletedObjectsCount = delete(elementsToDemote);
        demoteArguments.tupleRankList.removeAll(elementsToDemote);
        demoteArguments.statsWarmupDemoter.adddeleted_by_low_priority(deletedObjectsCount);
        double highestPriorityDeleted = elementsToDemote.get(elementsToDemote.size() - 1).warmupProperties().priority();
        highestPriority.set(highestPriorityDeleted);
        return deletedObjectsCount;
    }

    @VisibleForTesting
    long delete(List<TupleRank> tuppleRankList)
            throws ExecutionException, InterruptedException
    {
        Map<RowGroupKey, List<TupleRank>> rowGroupDataWarmUpElementMap = tuppleRankList.stream()
                .filter(tr -> !demoteArguments.failedRowGropDataSet.contains(tr.rowGroupKey()))
                .collect(groupingBy(TupleRank::rowGroupKey, mapping(Function.identity(), Collectors.toList())));
        List<RowGroupKey> rowGroupDataList = List.copyOf(rowGroupDataWarmUpElementMap.keySet());
        logger.debug("going to demote %d rowGroupData", rowGroupDataWarmUpElementMap.size());
        List<ListenableFuture<Long>> rowGroupDeleteFutures = new ArrayList<>();
        long deletedObject = 0;
        try {
            for (int i = 0; i < rowGroupDataList.size(); i++) {
                RowGroupKey rowGroupKey = rowGroupDataList.get(i);
                RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
                try {
                    rowGroupDeleteFutures.add(Futures.submit(() -> deleteRowGroupData(rowGroupData, rowGroupDataWarmUpElementMap.get(rowGroupKey)), rowGroupExecutorService));
                }
                catch (RejectedExecutionException ree) {
                    logger.warn("retry to submit row group data %s", rowGroupKey);
                    try {
                        deletedObject += Futures.allAsList(rowGroupDeleteFutures).get().stream().collect(Collectors.summingLong(Long::longValue));
                        rowGroupDeleteFutures = new ArrayList<>();
                        rowGroupDeleteFutures.add(Futures.submit(() -> deleteRowGroupData(rowGroupData, rowGroupDataWarmUpElementMap.get(rowGroupKey)), rowGroupExecutorService));
                    }
                    catch (Exception e) {
                        logger.error("failed to submit row group data %s", rowGroupKey);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        catch (Exception e) {
            Futures.allAsList(rowGroupDeleteFutures).get();
            throw e;
        }
        deletedObject += Futures.allAsList(rowGroupDeleteFutures).get().stream().collect(Collectors.summingLong(Long::longValue));
        return deletedObject;
    }

    private long deleteRowGroupData(RowGroupData rowGroupData, List<TupleRank> tupleRanksToDelete)
    {
        List<WarmUpElement> elementsToDelete = tupleRanksToDelete.stream().map(TupleRank::warmUpElement).collect(Collectors.toList());
        AtomicBoolean delete = new AtomicBoolean(true);
        if (rowGroupData.isEmpty()) {
            logger.debug("empty rowGropData %s", rowGroupData);
            if (rowGroupData.getWarmUpElements().size() == elementsToDelete.size()) {
                logger.debug("empty rowGropData, delete all row group");
                rowGroupDataService.deleteData(rowGroupData, true);
            }
            else {
                if (deleteEmptyRowGroups) {
                    logger.debug("empty rowGropData %s, delete partial elementsToDelete = %d, left = %d",
                            rowGroupData.getRowGroupKey(), elementsToDelete.size(), rowGroupData.getWarmUpElements().size());
                    rowGroupDataService.updateEmptyRowGroup(rowGroupData, Collections.emptyList(), elementsToDelete);
                }
                else {
                    delete.set(false);
                }
            }
        }
        else {
            AtomicLong retryFailure = new AtomicLong();
            try {
                RetryPolicy<Boolean> retryPolicy = new RetryPolicy<Boolean>().withDelay(warmupDemoterConfiguration.getDelayAcquireThread())
                        .withMaxDuration(warmupDemoterConfiguration.getMaxDurationAcquireThread())
                        .onFailedAttempt(a -> retryFailure.incrementAndGet())
                        .handle(TrinoException.class)
                        .withMaxRetries(warmupDemoterConfiguration.getMaxRetriesAcquireThread()).handle(RuntimeException.class);
                Failsafe.with(retryPolicy).run(() -> {
                    tryAllocateTx();
                    if (!coolRowGroupData(rowGroupData, elementsToDelete)) {
                        delete.set(false);
                    }
                });
            }
            catch (Exception e) {
                logger.error("failed to acquire threads for demotion, number of retries = %d", retryFailure.longValue());
                throw e;
            }
            finally {
                demoteArguments.statsWarmupDemoter.addnumber_fail_acquire(retryFailure.get());
            }
        }
        return delete.get() ? elementsToDelete.size() : 0;
    }

    private List<WarmupRule> findExistingWarmupElementRules(RowGroupKey rowGroupKey,
            Map<SchemaTableColumn, List<WarmupRule>> schemaTableColumnToRulesMap,
            WarmUpElement warmUpElement)
    {
        VaradaColumn varadaColumn = warmUpElement.getVaradaColumn();
        VaradaColumn newVaradaColumn;
        if (varadaColumn instanceof RegularColumn regularColumn) {
            newVaradaColumn = new RegularColumn(regularColumn.getName());
        }
        else {
            newVaradaColumn = varadaColumn;
        }

        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(
                new SchemaTableName(rowGroupKey.schema(),
                        rowGroupKey.table()),
                newVaradaColumn);
        return schemaTableColumnToRulesMap.getOrDefault(schemaTableColumn, List.of());
    }

    private boolean coolRowGroupData(RowGroupData rowGroupData, List<WarmUpElement> elementsToDelete)
    {
        logger.debug("coolRowGroupData rowGroup key %s - going to delete warmupElements size = %d",
                rowGroupData.getRowGroupKey(), elementsToDelete.size());
        boolean success = true;
        try {
            demote(rowGroupData, elementsToDelete);
        }
        catch (Exception e) {
            logger.error(e, String.format("failed to demote rowGroupData: %s, row grop will be deleted",
                    rowGroupData.getRowGroupKey()));
            handleFailDeleteRowGroup(rowGroupData);
            success = false;
        }
        finally {
            releaseTx();
        }
        return success;
    }

    private void demote(RowGroupData rowGroupData, List<WarmUpElement> elementsToDelete)
            throws InterruptedException
    {
        boolean locked = false;
        try {
            rowGroupData.getLock().writeLock();
            locked = true;
            try {
                rowGroupDataService.removeElements(rowGroupData, elementsToDelete);
            }
            catch (TrinoException te) {
                logger.error(te, "failed during attach rowGroup %s", rowGroupData.getRowGroupKey());
                throw te;
            }
        }
        catch (InterruptedException e) {
            logger.warn(e, "failed to acquire write lock for row group %s", rowGroupData.getRowGroupKey());
            throw e;
        }
        finally {
            if (rowGroupData != null && locked) {
                rowGroupData.getLock().writeUnlock();
            }
        }
    }

    private void handleFailDeleteRowGroup(RowGroupData rowGroupData)
    {
        rowGroupDataService.removeElements(rowGroupData);
        demoteArguments.addFailedRowGropData(rowGroupData.getRowGroupKey());
        demoteArguments.statsWarmupDemoter.incfailed_row_group_data();
    }

    void sortTupleRankCollection(List<TupleRank> tupleRankList)
    {
        Collections.sort(tupleRankList);
    }

    public AtomicDouble getDemoterHighestPriority()
    {
        return highestPriority;
    }

    @VisibleForTesting
    public void resetHighestPriority()
    {
        this.highestPriority.set(0);
    }

    @VisibleForTesting
    boolean trySetIsExecutingToTrue()
    {
        return this.isExecuting.compareAndSet(false, true);
    }

    public boolean isExecuting()
    {
        return isExecuting.get();
    }

    @VisibleForTesting
    public int getCurrentRunSequence()
    {
        return demoteArguments == null ? FAILED_DEMOTE_SQUENCE : demoteArguments.demoterSequence;
    }

    public long getLastExecutionTime()
    {
        return lastExecutionTime;
    }

    public VaradaStatsWarmupDemoter getCurrentRunStats()
    {
        return demoteArguments == null ? null : demoteArguments.statsWarmupDemoter;
    }

    public void setTupleFilters(List<TupleFilter> tupleFilters)
    {
        this.tupleFilters = tupleFilters;
    }

    public void setForceDeleteDeadObjects(boolean forceDeleteDeadObjects)
    {
        this.forceDeleteDeadObjects = forceDeleteDeadObjects;
    }

    public void setForceDeleteFailedObjects(boolean forceDeleteFailedObjects)
    {
        this.forceDeleteFailedObjects = forceDeleteFailedObjects;
    }

    public void setResetHigestPriority(boolean resetHigestPriority)
    {
        this.resetHigestPriority = resetHigestPriority;
    }

    public void setDeleteEmptyRowGroups(boolean deleteEmptyRowGroups)
    {
        this.deleteEmptyRowGroups = deleteEmptyRowGroups;
    }

    public void setEnableDemote(boolean enableDemote)
    {
        this.enableDemote = enableDemote;
    }

    private class DemoteArguments
    {
        StopWatch stopWatch;
        int demoterSequence = FAILED_DEMOTE_SQUENCE;
        List<TupleRank> tupleRankList = new ArrayList<>();
        double maxUsageThresholdPercentage = -1;
        double cleanupUsageThresholdPercentage = -1;
        int batchSize = 1;
        VaradaStatsWarmupDemoter statsWarmupDemoter;
        long flowId = -1;
        long maxElementsToDemote = 1000;
        double epsilon = -1;
        int numberOfCycles;
        boolean deleteEmptyRowGroups;

        Set<RowGroupKey> failedRowGropDataSet = new HashSet<>();

        public DemoteArguments(int demoterSequence,
                double maxUsageThresholdPercentage,
                double cleanupUsageThresholdPercentage,
                int batchSize,
                long maxElementsToDemote,
                double epsilon,
                boolean deleteEmptyRowGroups)
        {
            this.demoterSequence = demoterSequence;
            this.maxUsageThresholdPercentage = maxUsageThresholdPercentage;
            this.cleanupUsageThresholdPercentage = cleanupUsageThresholdPercentage;
            this.batchSize = batchSize;
            this.maxElementsToDemote = maxElementsToDemote;
            this.epsilon = epsilon;
            this.deleteEmptyRowGroups = deleteEmptyRowGroups;
            this.statsWarmupDemoter = VaradaStatsWarmupDemoter.create(WARMUP_DEMOTER_STAT_GROUP);
            stopWatch = new StopWatch();
            stopWatch.start();
        }

        public double getLowestPriority()
        {
            return tupleRankList.isEmpty() ? 0 : tupleRankList.get(0).warmupProperties().priority();
        }

        public void increaseNumberOfCycles()
        {
            numberOfCycles++;
        }

        public void addFailedRowGropData(RowGroupKey failedRowGroup)
        {
            this.failedRowGropDataSet.add(failedRowGroup);
        }

        @Override
        public String toString()
        {
            String statsWarmupDemoterJson = null;
            try {
                statsWarmupDemoterJson = new ObjectMapper().writeValueAsString(statsWarmupDemoter);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            return "DemoteArguments{" +
                    "stopWatch=" + stopWatch +
                    ", demoterSequence=" + demoterSequence +
                    ", tupleRankList=" + tupleRankList.size() +
                    ", tupleFilters=" + tupleFilters +
                    ", maxUsageThresholdPercentage=" + maxUsageThresholdPercentage +
                    ", cleanupUsageThresholdPercentage=" + cleanupUsageThresholdPercentage +
                    ", batchSize=" + batchSize +
                    ", statsWarmupDemoter=" + statsWarmupDemoterJson +
                    ", flowId=" + flowId +
                    ", maxElementsToDemote=" + maxElementsToDemote +
                    ", epsilon=" + epsilon +
                    ", deleteEmptyRowGroups=" + deleteEmptyRowGroups +
                    '}';
        }
    }
}
