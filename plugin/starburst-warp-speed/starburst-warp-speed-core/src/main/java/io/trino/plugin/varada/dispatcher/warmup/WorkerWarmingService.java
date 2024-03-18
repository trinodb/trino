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
package io.trino.plugin.varada.dispatcher.warmup;

import com.amazonaws.util.CollectionUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.WarmupDemoterConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.PartitionKey;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.TransformedColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.dispatcher.model.WildcardColumn;
import io.trino.plugin.varada.dispatcher.query.PredicateContext;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.classifier.PredicateContextData;
import io.trino.plugin.varada.dispatcher.query.classifier.WarmedWarmupTypes;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.varada.warmup.model.WarmupRule;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.trino.plugin.varada.dispatcher.warmup.WarmupProperties.NA_TTL;
import static io.trino.plugin.varada.type.TypeUtils.isWarmBasicSupported;
import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerWarmingService
{
    public static final String WARMING_SERVICE_STAT_GROUP = "warming-service";
    private static final int MAX_BATCH_SIZE = 1024;

    public static final Comparator<WarmupRule> warmupRuleComparator =
            Comparator.comparingInt((WarmupRule o) -> o.getVaradaColumn().getOrder())
                    .thenComparingInt(o -> o.getPredicates().size());
    private static final Logger logger = Logger.get(WorkerWarmingService.class);

    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final VaradaStatsWarmingService statsWarmingService;
    private final WarmExecutionTaskFactory warmExecutionTaskFactory;
    private final WorkerTaskExecutorService workerTaskExecutorService;
    private final WarmupDemoterService warmupDemoterService;
    private final WorkerWarmupRuleService workerWarmupRuleService;
    private final RowGroupDataService rowGroupDataService;
    private final WarmupDemoterConfiguration warmupDemoterConfiguration;
    private final GlobalConfiguration globalConfiguration;
    private ImmutableMap<WarmUpType, WarmupProperties> defaultRules;
    private Map<WarmUpType, Predicate<Type>> warmupTypeValidators;
    private final StorageWarmerService storageWarmerService;
    private final int batchSize;

    @Inject
    public WorkerWarmingService(MetricsManager metricsManager,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            WorkerTaskExecutorService workerTaskExecutorService,
            WarmExecutionTaskFactory warmExecutionTaskFactory,
            WarmupDemoterService warmupDemoterService,
            WorkerWarmupRuleService workerWarmupRuleService,
            RowGroupDataService rowGroupDataService,
            WarmupDemoterConfiguration warmupDemoterConfiguration,
            GlobalConfiguration globalConfiguration,
            StorageWarmerService storageWarmerService)
    {
        this(metricsManager,
                dispatcherProxiedConnectorTransformer,
                workerTaskExecutorService,
                warmExecutionTaskFactory,
                warmupDemoterService,
                workerWarmupRuleService,
                rowGroupDataService,
                warmupDemoterConfiguration,
                globalConfiguration,
                storageWarmerService,
                MAX_BATCH_SIZE);
    }

    @VisibleForTesting
    public WorkerWarmingService(MetricsManager metricsManager,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            WorkerTaskExecutorService workerTaskExecutorService,
            WarmExecutionTaskFactory warmExecutionTaskFactory,
            WarmupDemoterService warmupDemoterService,
            WorkerWarmupRuleService workerWarmupRuleService,
            RowGroupDataService rowGroupDataService,
            WarmupDemoterConfiguration warmupDemoterConfiguration,
            GlobalConfiguration globalConfiguration,
            StorageWarmerService storageWarmerService,
            int batchSize)
    {
        this.warmExecutionTaskFactory = warmExecutionTaskFactory;
        this.dispatcherProxiedConnectorTransformer = dispatcherProxiedConnectorTransformer;
        this.statsWarmingService = metricsManager.registerMetric(VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP));
        this.workerTaskExecutorService = requireNonNull(workerTaskExecutorService);
        this.warmupDemoterService = requireNonNull(warmupDemoterService);
        this.workerWarmupRuleService = requireNonNull(workerWarmupRuleService);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.warmupDemoterConfiguration = requireNonNull(warmupDemoterConfiguration);
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.storageWarmerService = storageWarmerService;
        this.batchSize = batchSize;
        initDefaultRules();
        initWarmUpTypeValidators();
    }

    public void warm(ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherSplit dispatcherSplit,
            DispatcherTableHandle dispatcherTableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            int iterationCount)
    {
        try {
            RowGroupKey rowGroupKey = rowGroupDataService.createRowGroupKey(dispatcherSplit.getSchemaName(),
                    dispatcherSplit.getTableName(),
                    dispatcherSplit.getPath(),
                    dispatcherSplit.getStart(),
                    dispatcherSplit.getLength(),
                    dispatcherSplit.getFileModifiedTime(),
                    dispatcherSplit.getDeletedFilesHash());
            WorkerSubmittableTask prioritizeTask = warmExecutionTaskFactory.createExecutionTask(connectorPageSourceProvider,
                    transactionHandle,
                    session,
                    dispatcherSplit,
                    dispatcherTableHandle,
                    columns,
                    dynamicFilter,
                    rowGroupKey,
                    this,
                    iterationCount,
                    0,
                    WorkerTaskExecutorService.TaskExecutionType.CLASSIFY);
            WorkerTaskExecutorService.SubmissionResult submissionResult = workerTaskExecutorService.submitTask(prioritizeTask, true);

            if (submissionResult == WorkerTaskExecutorService.SubmissionResult.CONFLICT) {
                statsWarmingService.incwarm_skipped_due_key_conflict();
            }
        }
        catch (Exception e) {
            logger.debug(e, "warm failed");
            statsWarmingService.incwarm_failed();
        }
    }

    protected void removeRowGroupFromSubmittedRowGroup(RowGroupKey rowGroupKey)
    {
        workerTaskExecutorService.taskFinished(rowGroupKey);
    }

    WarmData getWarmData(List<ColumnHandle> columns,
            RowGroupKey rowGroupKey,
            DispatcherSplit dispatcherSplit,
            ConnectorSession session,
            QueryContext queryContext,
            boolean isDryRun)
    {
        if (!shouldWarm(columns)) {
            return new WarmData(List.of(), ImmutableSetMultimap.of(), WarmExecutionState.NOTHING_TO_WARM, false, queryContext, null);
        }
        Map<RegularColumn, ColumnHandle> columnNameHandleMap = columns.stream()
                .collect(Collectors.toMap(dispatcherProxiedConnectorTransformer::getVaradaRegularColumn, Function.identity()));

        Map<VaradaColumn, Map<WarmUpType, WarmupProperties>> requiredWarmupMap = getMatchingRules(dispatcherSplit, columnNameHandleMap);

        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        WarmDataState warmDataState = getWarmDataState(rowGroupData, requiredWarmupMap);

        if (warmDataState.newRequiredWarmUpTypeMap().isEmpty() && canAddDefaultRules(session, queryContext)) {
            Map<VaradaColumn, Set<WarmupProperties>> colNameToDefaultRules = getDefaultPropertiesRules(columns, queryContext, session);

            addDefaultRulesToRequiredColumns(requiredWarmupMap, colNameToDefaultRules);
            warmDataState = getWarmDataState(rowGroupData, requiredWarmupMap);
        }

        if (warmDataState.newRequiredWarmUpTypeMap().isEmpty()) {
            statsWarmingService.incall_elements_warmed_or_skipped();
            logger.debug("nothing to warm, do nothing");
            return new WarmData(List.of(), ImmutableSetMultimap.of(), WarmExecutionState.NOTHING_TO_WARM, false, queryContext, null);
        }
        if (!isDryRun) {
            if (!storageWarmerService.tryAllocateNativeResourceForWarmup()) {  // validate usage only if this is a 'real' run
                return new WarmData(List.of(), ImmutableSetMultimap.of(), WarmExecutionState.NOTHING_TO_WARM, false, queryContext, null);
            }
        }
        WarmExecutionState warmExecutionState = rowGroupData != null && rowGroupData.isEmpty() ? WarmExecutionState.EMPTY_ROW_GROUP : WarmExecutionState.WARM;
        SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap = warmDataState.newRequiredWarmUpTypeMap();
        if (warmExecutionState == WarmExecutionState.WARM) {
            // No need to warm in batches in case of empty row group
            requiredWarmUpTypeMap = warmInBatches(warmDataState.newRequiredWarmUpTypeMap(), warmDataState.existingWarmupMap());
        }
        List<ColumnHandle> dispatcherColumnsToWarm = new ArrayList<>();
        requiredWarmUpTypeMap.keySet().forEach(varadaColumn -> {
            if (varadaColumn instanceof TransformedColumn transformedColumn) {
                varadaColumn = new RegularColumn(transformedColumn.getName(), transformedColumn.getColumnId());
            }
            if (varadaColumn instanceof RegularColumn regularColumn) {
                ColumnHandle columnHandle = columnNameHandleMap.get(regularColumn);
                if (columnHandle != null && !dispatcherColumnsToWarm.contains(columnHandle)) {
                    dispatcherColumnsToWarm.add(columnHandle);
                }
            }
            else {
                logger.warn("varadaColumn is not instance of RegularColumn -> %s", varadaColumn);
            }
        });

        return new WarmData(dispatcherColumnsToWarm, requiredWarmUpTypeMap, warmExecutionState, true, queryContext, warmDataState.warmWarmUpElements);
    }

    private WarmDataState getWarmDataState(RowGroupData rowGroupData,
            Map<VaradaColumn, Map<WarmUpType, WarmupProperties>> requiredlWarmupMap)
    {
        SetMultimap<VaradaColumn, WarmupProperties> newRequiredWarmUpTypeMap = HashMultimap.create();
        List<WarmUpElement> warmWarmUpElements = new ArrayList<>();

        WarmedWarmupTypes warmedWarmupTypes = createExistingWarmupMap(rowGroupData);

        for (VaradaColumn varadaColumn : requiredlWarmupMap.keySet()) {
            Map<WarmUpType, WarmupProperties> requiredWarmUpTypeToProperties = filterRequiredWarmupByPriority(requiredlWarmupMap.get(varadaColumn));

            if (!requiredWarmUpTypeToProperties.isEmpty()) {
                // Map<WarmUpType, WarmUpElement> existingWarmUpTypeToElement = warmedWarmupTypes.is(varadaColumn, Map.of());

                if (warmedWarmupTypes.isNewColumn(varadaColumn)) {
                    logger.debug("new column to warm in an existing row group. newColumn=%s, warmUpTypes=%s", varadaColumn, requiredWarmUpTypeToProperties.keySet());
                    newRequiredWarmUpTypeMap.putAll(varadaColumn, requiredWarmUpTypeToProperties.values());
                }
                else {
                    for (WarmUpType warmUpType : requiredWarmUpTypeToProperties.keySet()) {
                        WarmupProperties warmupProperties = requiredWarmUpTypeToProperties.get(warmUpType);
                        Optional<WarmUpElement> warmUpElements = warmedWarmupTypes.getByTypeAndColumn(warmUpType, varadaColumn, warmupProperties.transformFunction());

                        if (warmUpElements.isEmpty()) {
                            logger.debug("new type to warm. varadaColumn=%s, warmUpType=%s", varadaColumn, warmUpType);
                            newRequiredWarmUpTypeMap.put(varadaColumn, warmupProperties);
                        }
                        warmUpElements.ifPresent(warmUpElement -> {
                            if (warmUpElement.isValid()) {
                                switch (warmUpElement.getWarmState()) {
                                    case HOT ->
                                        logger.debug("column is already warmed locally - nothing to do. columnName=%s, warmUpType=%s", varadaColumn, warmUpType);
                                    case WARM -> {
                                        logger.debug("column is warmed on cloud - import. columnName=%s, warmUpType=%s", varadaColumn, warmUpType);
                                        newRequiredWarmUpTypeMap.put(varadaColumn, warmupProperties);
                                        warmWarmUpElements.add(warmUpElement);
                                    }
                                    default -> logger.warn("unexpected - skip warming. warmUpElement %s", warmUpElement);
                                }
                            }
                            else if (shouldAllowWarm(warmUpElement)) {
                                logger.debug("allow warming for varadaColumn=%s, warmUpType=%s", varadaColumn, warmUpType);
                                newRequiredWarmUpTypeMap.put(varadaColumn, warmupProperties);
                            }
                            else {
                                logger.debug("skip warming for varadaColumn=%s, warmUpType=%s", varadaColumn, warmUpType);
                            }
                        });
                    }
                }
            }
        }
        return new WarmDataState(newRequiredWarmUpTypeMap, warmWarmUpElements, warmedWarmupTypes);
    }

    // Limit the amount of WarmupProperties to warm in a single time.
    // In addition - retry to warm failed warmup elements one by one and only after warming all new warmup elements.
    private SetMultimap<VaradaColumn, WarmupProperties> warmInBatches(SetMultimap<VaradaColumn, WarmupProperties> newRequiredWarmUpTypeMap,
            WarmedWarmupTypes existingWarmupMap)
    {
        int originalSize = newRequiredWarmUpTypeMap.size();

        // limit the amount of WarmupProperties to warm in a single time
        if (newRequiredWarmUpTypeMap.size() > batchSize) {
            AtomicInteger elementsToRemove = new AtomicInteger(newRequiredWarmUpTypeMap.size() - batchSize);
            while (elementsToRemove.get() > 0) {
                VaradaColumn varadaColumn = newRequiredWarmUpTypeMap.keys().stream().findAny().orElseThrow();
                Set<WarmupProperties> warmupProperties = newRequiredWarmUpTypeMap.get(varadaColumn);
                warmupProperties.removeIf(x -> elementsToRemove.getAndDecrement() > 0);
            }
        }

        // WarmedWarmupTypes warmedWarmupTypes = null;
        // retry to warm failed warmup elements one by one and only after warming all new warmup elements
        SetMultimap<VaradaColumn, WarmupProperties> actualProxiedElementsToWarm = newRequiredWarmUpTypeMap.entries().stream()
                .filter(entry -> !existingWarmupMap.contains(entry.getKey(), entry.getValue().warmUpType(), entry.getValue().transformFunction()))
                .collect(Multimaps.toMultimap(Map.Entry::getKey, Map.Entry::getValue, HashMultimap::create));
        if (actualProxiedElementsToWarm.isEmpty()) {
            actualProxiedElementsToWarm = newRequiredWarmUpTypeMap.entries().stream()
                    .sorted(Comparator.comparingInt(entry ->
                    {
                        Optional<WarmUpElement> warmUpElement = existingWarmupMap.getByTypeAndColumn(entry.getValue().warmUpType(), entry.getKey(), entry.getValue().transformFunction());
                        return warmUpElement.map(element -> element.getState().temporaryFailureCount()).orElse(0);
                    }))
                    .collect(Multimaps.toMultimap(Map.Entry::getKey, Map.Entry::getValue, HashMultimap::create));
        }

        if (logger.isDebugEnabled() && originalSize != actualProxiedElementsToWarm.size()) {
            SetMultimap<VaradaColumn, WarmupProperties> finalActualProxiedElementsToWarm = actualProxiedElementsToWarm;
            logger.debug("Splitting warmup into batches. Current batch: %s. Remaining: %s",
                    actualProxiedElementsToWarm.entries().stream()
                            .map(entry -> entry.getKey() + ":" + entry.getValue().warmUpType())
                            .collect(Collectors.joining(", ")),
                    newRequiredWarmUpTypeMap.entries().stream()
                            .filter(entry -> !finalActualProxiedElementsToWarm.containsKey(entry.getKey()) || !finalActualProxiedElementsToWarm.get(entry.getKey()).contains(entry.getValue()))
                            .map(entry -> entry.getKey() + ":" + entry.getValue().warmUpType())
                            .collect(Collectors.joining(", ")));
        }

        return actualProxiedElementsToWarm;
    }

    private WarmedWarmupTypes createExistingWarmupMap(RowGroupData rowGroupData)
    {
        WarmedWarmupTypes.Builder warmedWarmupTypes = new WarmedWarmupTypes.Builder();
        if (rowGroupData == null) {
            return warmedWarmupTypes.build();
        }
        rowGroupData.getWarmUpElements().forEach(warmedWarmupTypes::add);
        return warmedWarmupTypes.build();
    }

    private boolean shouldAllowWarm(WarmUpElement warmUpElement)
    {
        WarmUpElementState.State state = warmUpElement.getState().state();

        if (WarmUpElementState.State.VALID.equals(state)) {
            return !warmUpElement.isHot();
        }
        if (WarmUpElementState.State.FAILED_PERMANENTLY.equals(state)) {
            logger.debug("won't retry to warm a permanent failed warmUpElement. columnKey=%s, warmUpType=%s", warmUpElement.getVaradaColumn().getName(), warmUpElement.getWarmUpType());
            statsWarmingService.incwarm_skip_permanent_failed_warmup_element();
            return false;
        }

        // Backoff is required because otherwise we'll run all the retries one after the other (upon successful warmup, ProxyExecutionTask.warming() re-triggers
        // workerWarmingService.warm() if workerWarmingService.getWarmData() indicates that there are still columns to be warmed)
        int temporaryFailureCount = warmUpElement.getState().temporaryFailureCount();

        if (temporaryFailureCount == 1) {
            // Because at the first try we warm in a batch, we don't know if the failure is related to this specific WarmUpElement and we should try again immediately
            return true;
        }

        double nextAttemptTime = warmUpElement.getState().lastTemporaryFailure() + globalConfiguration.getWarmRetryBackoffFactorInMillis() * Math.pow(2, temporaryFailureCount - 1);
        boolean backoffElapsed = System.currentTimeMillis() > nextAttemptTime;

        if (backoffElapsed) {
            logger.debug("will retry to warm a failed warmUpElement (failed %d / %d times). columnKey=%s, warmUpType=%s",
                    temporaryFailureCount, globalConfiguration.getMaxWarmRetries(), warmUpElement.getVaradaColumn().getName(), warmUpElement.getWarmUpType());
        }
        else {
            logger.debug("won't retry to warm a temporary failed warmUpElement, backoff is until timestamp %f. columnKey=%s, warmUpType=%s",
                    nextAttemptTime, warmUpElement.getVaradaColumn().getName(), warmUpElement.getWarmUpType());
            statsWarmingService.incwarm_skip_temporary_failed_warmup_element();
        }

        return backoffElapsed;
    }

    private boolean shouldWarm(List<ColumnHandle> columns)
    {
        boolean shouldWarm = true;
        if (columns.isEmpty()) {
            statsWarmingService.incempty_column_list();
            shouldWarm = false;
        }
        return shouldWarm;
    }

    private boolean canAddDefaultRules(ConnectorSession session, QueryContext queryContext)
    {
        return isDefaultWarmingEnabled(session, queryContext.getRemainingCollectColumns().size()) &&
                warmupDemoterService.canAllowWarmup(warmupDemoterConfiguration.getDefaultRulePriority());
    }

    private boolean isDefaultWarmingEnabled(ConnectorSession session, int collectColumnsCount)
    {
        Boolean sessionEnabled = VaradaSessionProperties.isDefaultWarmingEnabled(session);
        boolean defaultWarmingEnabled = sessionEnabled != null ? sessionEnabled : globalConfiguration.isEnableDefaultWarming();
        if (defaultWarmingEnabled) {
            int maxElementsToCollect = globalConfiguration.getMaxCollectColumnsSkipDefaultWarming();
            return collectColumnsCount <= maxElementsToCollect;
        }
        return false;
    }

    private Map<VaradaColumn, Set<WarmupProperties>> getDefaultPropertiesRules(List<ColumnHandle> columns,
            QueryContext queryContext,
            ConnectorSession session)
    {
        Map<VaradaColumn, Type> columnNameToColumnType = columns
                .stream()
                .filter(c -> !(dispatcherProxiedConnectorTransformer.getColumnType(c) instanceof MapType))
                .collect(Collectors.toMap(
                        dispatcherProxiedConnectorTransformer::getVaradaRegularColumn,
                        dispatcherProxiedConnectorTransformer::getColumnType));
        Map<VaradaColumn, Set<WarmupProperties>> result = new HashMap<>();
        Set<VaradaColumn> varadaColumns = new HashSet<>(queryContext.getPredicateContextData().getRemainingColumns());
        if (VaradaSessionProperties.isDefaultWarmingIndex(session)) {
            varadaColumns.addAll(columnNameToColumnType.keySet());
        }

        if (!globalConfiguration.isDataOnlyWarming()) {
            varadaColumns.forEach(varadaColumn -> {
                Set<WarmupProperties> properties = new HashSet<>();
                PredicateContextData predicateContextData = queryContext.getPredicateContextData();
                Type type = columnNameToColumnType.get(varadaColumn);
                if (TypeUtils.isWarmLuceneSupported(type) &&
                        predicateContextData.isLuceneColumn(varadaColumn)) {
                    properties.add(defaultRules.get(WarmUpType.WARM_UP_TYPE_LUCENE));
                }
                else if (varadaColumn instanceof TransformedColumn transformedColumn) {
                    // we already validated that TransformedColumn isWarmBasicSupported at Coordinator.
                    WarmupProperties warmingProperty = new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC,
                            warmupDemoterConfiguration.getDefaultRulePriority(),
                            NA_TTL,
                            transformedColumn.getTransformFunction());
                    properties.add(warmingProperty);
                }
                else {
                    if (isWarmBasicSupported(type)) {
                        List<PredicateContext> remainingPredicatesByColumn = queryContext.getPredicateContextData().getRemainingPredicatesByColumn((RegularColumn) varadaColumn);
                        if (remainingPredicatesByColumn.isEmpty()) {
                            //in case of default warming + default index, we will warm default column with basic
                            properties.add(defaultRules.get(WarmUpType.WARM_UP_TYPE_BASIC));
                        }
                        else {
                            for (PredicateContext remainingPredicates : remainingPredicatesByColumn) {
                                WarmupProperties defaultWarmingProperty = new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, warmupDemoterConfiguration.getDefaultRulePriority(), NA_TTL, remainingPredicates.getTransformedColumn());
                                properties.add(defaultWarmingProperty);
                            }
                        }
                    }
                }
                if (!properties.isEmpty()) {
                    result.put(varadaColumn, properties);
                }
            });
        }
        queryContext.getRemainingCollectColumns()
                .stream()
                .filter(column -> TypeUtils.isWarmDataSupported(columnNameToColumnType.get(dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(column))))
                .forEach(column -> {
                    RegularColumn varadaColumn = dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(column);
                    Set<WarmupProperties> properties = result.computeIfAbsent(varadaColumn, v -> new HashSet<>());
                    properties.add(defaultRules.get(WarmUpType.WARM_UP_TYPE_DATA));
                });
        return result;
    }

    private Map<VaradaColumn, Map<WarmUpType, WarmupProperties>> getMatchingRules(DispatcherSplit dispatcherSplit,
            Map<RegularColumn, ColumnHandle> varadaColumnToColumnHandle)
    {
        Map<RegularColumn, String> partitionKeysMap = dispatcherSplit.getPartitionKeys().stream().collect(Collectors.toMap(PartitionKey::regularColumn, PartitionKey::partitionValue));
        List<WarmupRule> schemaAndTableRules = workerWarmupRuleService.getWarmupRules(new SchemaTableName(dispatcherSplit.getSchemaName(), dispatcherSplit.getTableName()));
        Map<VaradaColumn, Map<WarmUpType, WarmupProperties>> matchingRules = new HashMap<>();
        Map<String, ColumnHandle> columnNameToColumnHandle = varadaColumnToColumnHandle.entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getName(), Map.Entry::getValue));

        for (WarmupRule warmupRule : schemaAndTableRules.stream().sorted(warmupRuleComparator).toList()) {
            if (!(warmupRule.getVaradaColumn() instanceof WildcardColumn) &&
                    !columnNameToColumnHandle.containsKey(warmupRule.getVaradaColumn().getName())) {
                continue;
            }
            if (!(partitionKeysMap.isEmpty() ||
                    CollectionUtils.isNullOrEmpty(warmupRule.getPredicates()) ||
                    warmupRule.getPredicates().stream().allMatch(warmupPredicateRule -> warmupPredicateRule.test(partitionKeysMap)))) {
                continue;
            }

            List<VaradaColumn> varadaColumns;
            if (warmupRule.getVaradaColumn() instanceof WildcardColumn) {
                varadaColumns = columnNameToColumnHandle.values()
                        .stream()
                        .map(dispatcherProxiedConnectorTransformer::getVaradaRegularColumn)
                        .collect(Collectors.toList());
            }
            else {
                ColumnHandle columnHandle = columnNameToColumnHandle.get(warmupRule.getVaradaColumn().getName());
                RegularColumn regularColumn = dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(columnHandle);

                if (warmupRule.getVaradaColumn() instanceof TransformedColumn warmupRuleColumn) {
                    TransformedColumn transformedColumn =
                            new TransformedColumn(regularColumn.getName(), regularColumn.getColumnId(), warmupRuleColumn.getTransformFunction());
                    varadaColumns = List.of(transformedColumn);
                }
                else {
                    varadaColumns = List.of(regularColumn);
                }
            }

            varadaColumns.stream()
                    .filter(varadaColumn -> {
                        List<ColumnHandle> columnHandles = List.of(columnNameToColumnHandle.get(varadaColumn.getName()));
                        return columnHandles.stream()
                                .allMatch(columnHandle -> warmupTypeValidators.getOrDefault(warmupRule.getWarmUpType(), x -> false)
                                        .test(dispatcherProxiedConnectorTransformer.getColumnType(columnHandle)));
                    })
                    .forEach(varadaColumn -> {
                        Map<WarmUpType, WarmupProperties> warmUpTypeToProperties = matchingRules.computeIfAbsent(varadaColumn, c -> new HashMap<>());
                        TransformFunction transformFunction = (varadaColumn instanceof TransformedColumn transformedColumn) ?
                                transformedColumn.getTransformFunction() : TransformFunction.NONE;
                        warmUpTypeToProperties.put(warmupRule.getWarmUpType(),
                                new WarmupProperties(warmupRule.getWarmUpType(), warmupRule.getPriority(), warmupRule.getTtl(), transformFunction));
                    });
        }

        return matchingRules;
    }

    private void addDefaultRulesToRequiredColumns(Map<VaradaColumn, Map<WarmUpType, WarmupProperties>> requiredWarmUpTypeMap, Map<VaradaColumn, Set<WarmupProperties>> colNameToDefaultRules)
    {
        for (Map.Entry<VaradaColumn, Set<WarmupProperties>> defaultRules : colNameToDefaultRules.entrySet()) {
            if (requiredWarmUpTypeMap.containsKey(defaultRules.getKey())) {
                Map<WarmUpType, WarmupProperties> requiredWarmUpTypeToProperties = requiredWarmUpTypeMap.get(defaultRules.getKey());
                defaultRules.getValue().forEach(defaultRuleWarmupProperties -> {
                    if (requiredWarmUpTypeToProperties.containsKey(defaultRuleWarmupProperties.warmUpType())) {
                        if (!Objects.equals(defaultRuleWarmupProperties.transformFunction(), TransformFunction.NONE)) {
                            requiredWarmUpTypeToProperties.put(defaultRuleWarmupProperties.warmUpType(), defaultRuleWarmupProperties);
                        }
                    }
                    else {
                        requiredWarmUpTypeToProperties.put(defaultRuleWarmupProperties.warmUpType(), defaultRuleWarmupProperties);
                    }
                });
            }
            else {
                requiredWarmUpTypeMap.put(defaultRules.getKey(), defaultRules.getValue().stream().collect(Collectors.toMap(WarmupProperties::warmUpType, Function.identity())));
            }
        }
        logger.debug("add default warmup rules: %s, all rules:%s", colNameToDefaultRules, requiredWarmUpTypeMap);
    }

    private Map<WarmUpType, WarmupProperties> filterRequiredWarmupByPriority(Map<WarmUpType, WarmupProperties> requiredWarmUpTypeToProperties)
    {
        return requiredWarmUpTypeToProperties
                .entrySet()
                .stream()
                .filter(entry -> warmupDemoterService.canAllowWarmup(entry.getValue().priority()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void initDefaultRules()
    {
        this.defaultRules = ImmutableMap.<WarmUpType, WarmupProperties>builder()
                .put(WarmUpType.WARM_UP_TYPE_DATA, new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, warmupDemoterConfiguration.getDefaultRulePriority(), NA_TTL, TransformFunction.NONE))
                .put(WarmUpType.WARM_UP_TYPE_BASIC, new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, warmupDemoterConfiguration.getDefaultRulePriority(), NA_TTL, TransformFunction.NONE))
                .put(WarmUpType.WARM_UP_TYPE_LUCENE, new WarmupProperties(WarmUpType.WARM_UP_TYPE_LUCENE, warmupDemoterConfiguration.getDefaultRulePriority(), NA_TTL, TransformFunction.NONE))
                .buildOrThrow();
    }

    private void initWarmUpTypeValidators()
    {
        warmupTypeValidators = Map.of(
                WarmUpType.WARM_UP_TYPE_DATA, TypeUtils::isWarmDataSupported,
                WarmUpType.WARM_UP_TYPE_BASIC, TypeUtils::isWarmBasicSupported,
                WarmUpType.WARM_UP_TYPE_LUCENE, TypeUtils::isWarmLuceneSupported,
                WarmUpType.WARM_UP_TYPE_BLOOM_HIGH, TypeUtils::isWarmBloomSupported,
                WarmUpType.WARM_UP_TYPE_BLOOM_MEDIUM, TypeUtils::isWarmBloomSupported,
                WarmUpType.WARM_UP_TYPE_BLOOM_LOW, TypeUtils::isWarmBloomSupported);
    }

    WarmData updateWarmData(RowGroupData rowGroupData, WarmData warmData)
    {
        SetMultimap<VaradaColumn, WarmupProperties> newRequiredWarmUpTypeMap =
                getRequiredWarmUpTypeMap(warmData.requiredWarmUpTypeMap(), rowGroupData.getWarmUpElements());

        return new WarmData(getColumnHandleList(warmData.columnHandleList(), newRequiredWarmUpTypeMap.keySet()),
                newRequiredWarmUpTypeMap,
                warmData.warmExecutionState(),
                warmData.txMemoryReserved(),
                warmData.queryContext(),
                null);
    }

    private SetMultimap<VaradaColumn, WarmupProperties> getRequiredWarmUpTypeMap(SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap,
            Collection<WarmUpElement> warmUpElements)
    {
        Map<VaradaColumn, Map<WarmUpType, WarmupProperties>> requiredWarmupMap = new HashMap<>();
        for (Map.Entry<VaradaColumn, WarmupProperties> entry : requiredWarmUpTypeMap.entries()) {
            Map<WarmUpType, WarmupProperties> existingWarmUpTypeToProperties = requiredWarmupMap.computeIfAbsent(entry.getKey(), c -> new HashMap<>());
            existingWarmUpTypeToProperties.put(entry.getValue().warmUpType(), entry.getValue());
        }

        Map<VaradaColumn, Map<WarmUpType, WarmUpElement>> existingWarmupMap = new HashMap<>();
        for (WarmUpElement warmUpElement : warmUpElements) {
            Map<WarmUpType, WarmUpElement> existingWarmUpTypeToElement = existingWarmupMap.computeIfAbsent(warmUpElement.getVaradaColumn(), c -> new HashMap<>());
            existingWarmUpTypeToElement.put(warmUpElement.getWarmUpType(), warmUpElement);
        }

        SetMultimap<VaradaColumn, WarmupProperties> newRequiredWarmUpTypeMap = HashMultimap.create();
        for (Map.Entry<VaradaColumn, Map<WarmUpType, WarmupProperties>> requiredlWarmupEntry : requiredWarmupMap.entrySet()) {
            VaradaColumn varadaColumn = requiredlWarmupEntry.getKey();
            Map<WarmUpType, WarmupProperties> warmupPropertiesMap = requiredlWarmupEntry.getValue();

            Map<WarmUpType, WarmUpElement> warmUpElementMap = existingWarmupMap.get(varadaColumn);

            if (warmUpElementMap == null) {
                for (WarmupProperties warmupProperties : warmupPropertiesMap.values()) {
                    newRequiredWarmUpTypeMap.put(varadaColumn, warmupProperties);
                }
                continue;
            }

            for (Map.Entry<WarmUpType, WarmupProperties> warmupPropertiesEntry : warmupPropertiesMap.entrySet()) {
                WarmUpElement warmUpElement = warmUpElementMap.get(warmupPropertiesEntry.getKey());

                if ((warmUpElement == null) || shouldAllowWarm(warmUpElement)) {
                    newRequiredWarmUpTypeMap.put(varadaColumn, warmupPropertiesEntry.getValue());
                }
            }
        }
        return newRequiredWarmUpTypeMap;
    }

    private List<ColumnHandle> getColumnHandleList(List<ColumnHandle> columnHandleList, Set<VaradaColumn> columnSet)
    {
        Map<RegularColumn, ColumnHandle> columnNameHandleMap = columnHandleList.stream()
                .collect(Collectors.toMap(dispatcherProxiedConnectorTransformer::getVaradaRegularColumn, Function.identity()));

        List<ColumnHandle> dispatcherColumnsToWarm = new ArrayList<>();

        columnSet.forEach(varadaColumn -> {
            ColumnHandle columnHandle = columnNameHandleMap.get(varadaColumn);

            if (columnHandle != null) {
                dispatcherColumnsToWarm.add(columnHandle);
            }
        });
        return dispatcherColumnsToWarm;
    }

    private record WarmDataState(
            @SuppressWarnings("unused") SetMultimap<VaradaColumn, WarmupProperties> newRequiredWarmUpTypeMap,
            @SuppressWarnings("unused") List<WarmUpElement> warmWarmUpElements,
            @SuppressWarnings("unused") WarmedWarmupTypes existingWarmupMap) {}
}
