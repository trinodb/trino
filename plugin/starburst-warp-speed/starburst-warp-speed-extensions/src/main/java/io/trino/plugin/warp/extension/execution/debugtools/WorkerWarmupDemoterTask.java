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
package io.trino.plugin.warp.extension.execution.debugtools;

import com.amazonaws.util.CollectionUtils;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.WarmupDemoterConfiguration;
import io.trino.plugin.varada.dispatcher.warmup.demoter.TupleFilter;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.dispatcher.warmup.demoter.events.WarmupDemoterFinishEvent;
import io.trino.plugin.varada.execution.debugtools.ColumnFilter;
import io.trino.plugin.varada.execution.debugtools.FileFilter;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupDemoter;
import io.varada.annotation.Audit;
import io.varada.tools.CatalogNameProvider;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService.WARMUP_DEMOTER_STAT_GROUP;
import static io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterTask.WARMUP_DEMOTER_PATH;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(coordinator = false)
@Path(WARMUP_DEMOTER_PATH)
//@Api(value = "Demoter", tags = "Demoter")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class WorkerWarmupDemoterTask
        implements TaskResource
{
    public static final String WARMUP_DEMOTER_START_TASK_NAME = "worker-warmup-demoter-start";
    public static final String WARMUP_DEMOTER_STATUS_TASK_NAME = "worker-warmup-demoter-status";
    public static final String HIGHEST_PRIORITY_KEY = String.format("%s:highestPriority", WARMUP_DEMOTER_STAT_GROUP);
    public static final String MAX_USAGE_THRESHOLD_KEY = String.format("%s:maxUsageThresholdInPercentage", WARMUP_DEMOTER_STAT_GROUP);
    public static final String CLEANUP_USAGE_THRESHOLD_KEY = String.format("%s:cleanupUsageThresholdInPercentage", WARMUP_DEMOTER_STAT_GROUP);
    public static final String TOTAL_USAGE_THRESHOLD_KEY = String.format("%s:totalUsage", WARMUP_DEMOTER_STAT_GROUP);
    public static final String CURRENT_USAGE_THRESHOLD_KEY = String.format("%s:currentUsage", WARMUP_DEMOTER_STAT_GROUP);
    public static final String BATCH_SIZE_KEY = String.format("%s:batchSize", WARMUP_DEMOTER_STAT_GROUP);
    public static final String DEMOTE_SEQUENCE_KEY = String.format("%s:demoteSequence", WARMUP_DEMOTER_STAT_GROUP);
    public static final String EPSILON_KEY = String.format("%s:epsilon", WARMUP_DEMOTER_STAT_GROUP);
    public static final String MAX_ELEMENTS_TO_DEMOTE_ITERATION_KEY = String.format("%s:maxElementsDemoteInIteration", WARMUP_DEMOTER_STAT_GROUP);
    public static final String START_EXECUTION_KEY = String.format("%s:startExecution", WARMUP_DEMOTER_STAT_GROUP);
    public static final String END_EXECUTION_KEY = String.format("%s:endExecution", WARMUP_DEMOTER_STAT_GROUP);
    private static final Logger logger = Logger.get(WorkerWarmupDemoterTask.class);
    private final WarmupDemoterService warmupDemoterService;
    private final WarmupDemoterConfiguration warmupDemoterConfiguration;
    private final WorkerCapacityManager workerCapacityManager;
    private final CatalogNameProvider catalogNameProvider;
    private final VaradaStatsWarmupDemoter globalStatsDemoter;
    private final Map<Integer, CompletableFuture<WarmupDemoterFinishEvent>> demoteFutures = new ConcurrentHashMap<>();

    @Inject
    public WorkerWarmupDemoterTask(WarmupDemoterService warmupDemoterService,
            WarmupDemoterConfiguration warmupDemoterConfiguration,
            WorkerCapacityManager workerCapacityManager,
            CatalogNameProvider catalogNameProvider,
            MetricsManager metricsManager,
            EventBus eventBus)
    {
        this.warmupDemoterService = requireNonNull(warmupDemoterService);
        this.warmupDemoterConfiguration = warmupDemoterConfiguration;
        this.workerCapacityManager = workerCapacityManager;
        this.catalogNameProvider = catalogNameProvider;
        eventBus.register(this);
        this.globalStatsDemoter = metricsManager.registerMetric(VaradaStatsWarmupDemoter.create(WARMUP_DEMOTER_STAT_GROUP));
    }

    @POST
    @Audit
    @Path(WARMUP_DEMOTER_START_TASK_NAME)
    //@ApiOperation(value = "start", nickname = "startDemoter", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public Map<String, Object> start(WarmupDemoterData warmupDemoterData)
    {
        logger.debug("%s: start warmup demote task", catalogNameProvider.get());
        modifyConfigurationIfRequired(warmupDemoterData);
        if (!warmupDemoterData.isExecuteDemoter()) {
            return getConfigurationResults();
        }
        Instant start = Instant.now();
        Map<String, Object> result = new HashMap<>();
        int demoteSequence = WarmupDemoterService.FAILED_DEMOTE_SQUENCE;
        try {
            demoteSequence = warmupDemoterService.tryDemoteStart();
            if (demoteSequence > WarmupDemoterService.FAILED_DEMOTE_SQUENCE) {
                CompletableFuture<WarmupDemoterFinishEvent> future = new CompletableFuture<>();
                demoteFutures.put(demoteSequence, future);
                warmupDemoterService.setDeleteEmptyRowGroups(true);
                WarmupDemoterFinishEvent finishEvent = future.get();
                if (finishEvent.success()) {
                    result.putAll(finishEvent.runResults());
                }
                else {
                    result.putAll(getSkippedResult());
                }
                result.put(DEMOTE_SEQUENCE_KEY, finishEvent.demoteSequence());
            }
            result.putAll(getConfigurationResults());
            workerCapacityManager.setCurrentUsage();
        }
        catch (Exception e) {
            result.putAll(getSkippedResult());
            logger.error(e);
        }
        finally {
            CompletableFuture<WarmupDemoterFinishEvent> removed = demoteFutures.remove(demoteSequence);
            if (removed != null && !removed.isDone()) {
                removed.cancel(true);
            }
        }

        result.put(START_EXECUTION_KEY, LocalTime.ofInstant(start, ZoneId.systemDefault()));
        result.put(END_EXECUTION_KEY, LocalTime.now(ZoneId.systemDefault()));
        overrideUsageValuesFromGlobalDemote(result);
        return result;
    }

    @GET
    @Path(WARMUP_DEMOTER_STATUS_TASK_NAME)
    public DemoterStatus status()
    {
        return new DemoterStatus(warmupDemoterService.isExecuting(), warmupDemoterService.getLastExecutionTime(), warmupDemoterService.getCurrentRunSequence());
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getSkippedResult()
    {
        logger.debug("return skipped results");
        Map<String, Object> result = getConfigurationResults();
        result.putAll(globalStatsDemoter.statsCounterMapper());
        return result;
    }

    public Map<String, Object> getConfigurationResults()
    {
        Map<String, Object> result = new HashMap<>();
        result.put(HIGHEST_PRIORITY_KEY, warmupDemoterService.getDemoterHighestPriority());
        result.put(MAX_USAGE_THRESHOLD_KEY, warmupDemoterConfiguration.getMaxUsageThresholdPercentage());
        result.put(CLEANUP_USAGE_THRESHOLD_KEY, warmupDemoterConfiguration.getCleanupUsageThresholdPercentage());
        result.put(BATCH_SIZE_KEY, warmupDemoterConfiguration.getBatchSize());
        result.put(EPSILON_KEY, warmupDemoterConfiguration.getEpsilon());
        result.put(MAX_ELEMENTS_TO_DEMOTE_ITERATION_KEY, warmupDemoterConfiguration.getMaxElementsToDemoteInIteration());
        return result;
    }

    private void overrideUsageValuesFromGlobalDemote(Map<String, Object> result)
    {
        result.put(CURRENT_USAGE_THRESHOLD_KEY, globalStatsDemoter.getcurrentUsage());
        result.put(TOTAL_USAGE_THRESHOLD_KEY, globalStatsDemoter.gettotalUsage());
    }

    private Thresholds getThresholds(WarmupDemoterData warmupDemoterData)
    {
        if (warmupDemoterData.getWarmupDemoterThreshold() == null) {
            return new Thresholds(warmupDemoterData.getMaxUsageThresholdInPercentage(), warmupDemoterData.getCleanupUsageThresholdInPercentage());
        }

        long currentUsage = workerCapacityManager.getCurrentUsage();
        long totalCapacity = workerCapacityManager.getTotalCapacity();
        WarmupDemoterThreshold threshold = warmupDemoterData.getWarmupDemoterThreshold();
        return new Thresholds(calculateThreshold(threshold.maxPercentageFactorThreshold(), currentUsage, totalCapacity),
                calculateThreshold(threshold.cleanupPercentageFactorThreshold(), currentUsage, totalCapacity));
    }

    private double calculateThreshold(double factorThreshold, long currentUsage, long totalCapacity)
    {
        return (currentUsage * factorThreshold) * 100 / totalCapacity;
    }

    private void modifyConfigurationIfRequired(WarmupDemoterData warmupDemoterData)
    {
        if (warmupDemoterData.isModifyConfiguration()) {
            Thresholds thresholds = getThresholds(warmupDemoterData);
            if (warmupDemoterData.getBatchSize() > -1) {
                warmupDemoterConfiguration.setBatchSize(warmupDemoterData.getBatchSize());
            }
            if (thresholds.maxUsageThreshold > -1) {
                warmupDemoterConfiguration.setMaxUsageThresholdPercentage(thresholds.maxUsageThreshold);
            }
            if (thresholds.cleanupUsageThreshold > -1) {
                warmupDemoterConfiguration.setCleanupUsageThresholdPercentage(thresholds.cleanupUsageThreshold);
            }
            if (warmupDemoterData.getEpsilon() > -1) {
                warmupDemoterConfiguration.setEpsilon(warmupDemoterData.getEpsilon());
            }
            if (warmupDemoterData.getMaxElementsToDemoteInIteration() > -1) {
                warmupDemoterConfiguration.setMaxElementsToDemoteInIteration(warmupDemoterData.getMaxElementsToDemoteInIteration());
            }
            warmupDemoterService.setTupleFilters(calculateTupleFilter(warmupDemoterData));
            warmupDemoterService.setForceDeleteDeadObjects(warmupDemoterData.isForceExecuteDeadObjects());
            warmupDemoterService.setForceDeleteFailedObjects(warmupDemoterData.isForceDeleteFailedObjects());
            warmupDemoterService.setResetHigestPriority(warmupDemoterData.isResetHighestPriority());
            warmupDemoterService.setEnableDemote(warmupDemoterData.isEnableDemoteFeature());
        }
        logger.debug("%s, modifyConfigurationIfRequired: current configuration = batchSize=%d, maxUsageThresholdPercentage=%f, cleanupUsageThresholdPercentage=%f, enableDemoteFeature=%b", catalogNameProvider.get(), warmupDemoterConfiguration.getBatchSize(), warmupDemoterConfiguration.getMaxUsageThresholdPercentage(), warmupDemoterConfiguration.getCleanupUsageThresholdPercentage(), warmupDemoterData.isEnableDemoteFeature());
    }

    private List<TupleFilter> calculateTupleFilter(WarmupDemoterData warmupDemoterData)
    {
        List<TupleFilter> tupleFilters = new ArrayList<>();
        if (warmupDemoterData.getSchemaTableName() != null || !CollectionUtils.isNullOrEmpty(warmupDemoterData.getWarmupElementsData())) {
            tupleFilters.add(new ColumnFilter(warmupDemoterData.getSchemaTableName(), warmupDemoterData.getWarmupElementsData()));
        }
        if (!CollectionUtils.isNullOrEmpty(warmupDemoterData.getFilePaths())) {
            tupleFilters.add(new FileFilter(warmupDemoterData.getFilePaths()));
        }
        return tupleFilters;
    }

    @SuppressWarnings("unused")
    @Subscribe
    private void demoteFinished(WarmupDemoterFinishEvent demoterFinishEvent)
    {
        int demoteSequence = demoterFinishEvent.demoteSequence();
        if (demoteFutures.containsKey(demoteSequence)) {
            demoteFutures.get(demoteSequence).complete(demoterFinishEvent);
        }
    }

    private record Thresholds(
            @SuppressWarnings("unused") double maxUsageThreshold,
            @SuppressWarnings("unused") double cleanupUsageThreshold) {}
}
