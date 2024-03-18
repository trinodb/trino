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

import com.google.common.eventbus.EventBus;
import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.PartitionKey;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.events.WarmingFinishedEvent;
import io.trino.plugin.varada.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmingManager;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmupElementsCreator;
import io.trino.plugin.varada.storage.flows.FlowIdGenerator;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.varada.tools.util.StopWatch;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ProxyExecutionTask
        extends WorkerWarmerBaseTask
{
    private static final Logger logger = Logger.get(ProxyExecutionTask.class);

    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final EventBus eventBus;
    private final GlobalConfiguration globalConfiguration;
    private final int executionTaskPriority;
    private final StorageWarmerService storageWarmerService;

    public ProxyExecutionTask(WarmExecutionTaskFactory warmExecutionTaskFactory,
            EventBus eventBus,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            WarmingManager warmingManager,
            VaradaStatsWarmingService varadaStatsWarmingService,
            WorkerWarmingService workerWarmingService,
            ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherTableHandle dispatcherTableHandle,
            RowGroupKey rowGroupKey,
            List<ColumnHandle> columns,
            DispatcherSplit dispatcherSplit,
            DynamicFilter dynamicFilter,
            RowGroupDataService rowGroupDataService,
            GlobalConfiguration globalConfiguration,
            QueryClassifier queryClassifier,
            WarmupElementsCreator warmupElementsCreator,
            int iterationCount,
            int executionTaskPriority,
            WorkerTaskExecutorService workerTaskExecutorService,
            StorageWarmerService storageWarmerService)
    {
        super(warmExecutionTaskFactory, workerTaskExecutorService, varadaStatsWarmingService, warmingManager, workerWarmingService, connectorPageSourceProvider, transactionHandle, session, dispatcherTableHandle, rowGroupKey, columns, dispatcherSplit, dynamicFilter, rowGroupDataService, queryClassifier, warmupElementsCreator, iterationCount);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.eventBus = requireNonNull(eventBus);
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.executionTaskPriority = executionTaskPriority;
        this.storageWarmerService = storageWarmerService;
    }

    @Override
    public int getPriority()
    {
        return executionTaskPriority;
    }

    @Override
    public void taskScheduled()
    {
        statsWarmingService.incwarm_scheduled();
    }

    @Override
    protected void warm(WarmData dataToWarm)
    {
        SchemaTableName schemaTableName = new SchemaTableName(rowGroupKey.schema(), rowGroupKey.table());
        List<WarmUpElement> warmupElements;
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        try {
            warmupElements = warmupElementsCreator.createWarmupElements(rowGroupKey,
                    dataToWarm.requiredWarmUpTypeMap(),
                    schemaTableName,
                    dataToWarm.columnHandleList());
        }
        catch (Exception e) {
            logFailure(e);
            statsWarmingService.incwarm_failed();
            boolean releaseTx = dataToWarm != null && dataToWarm.txMemoryReserved();
            storageWarmerService.finishWarm(releaseTx);
            return;
        }

        if (warmupElements.isEmpty()) {
            boolean releaseTx = dataToWarm.txMemoryReserved();
            storageWarmerService.finishWarm(releaseTx);
            return;
        }
        Map<VaradaColumn, String> partitionKeys = getPartitionKeys(dispatcherSplit);
        if (dataToWarm.warmExecutionState() == WarmExecutionState.EMPTY_ROW_GROUP) {
            boolean warmSuccess = true;
            try {
                statsWarmingService.incwarm_started();
                if (rowGroupData == null) {
                    warmingManager.saveEmptyRowGroup(rowGroupKey, warmupElements, partitionKeys);
                }
                else {
                    warmingManager.warmEmptyRowGroup(rowGroupKey, warmupElements);
                }
            }
            catch (Exception e) {
                logFailure(e);
                statsWarmingService.incwarm_failed();
                warmSuccess = false;
            }
            finally {
                boolean releaseTx = dataToWarm.txMemoryReserved();
                storageWarmerService.finishWarm(releaseTx);
                if (warmSuccess) {
                    eventBus.post(new WarmingFinishedEvent(rowGroupKey, session));
                }
                statsWarmingService.incwarm_accomplished();
            }
            return;
        }

        long flowId = FlowIdGenerator.generateFlowId();
        StopWatch stopWatch = new StopWatch();
        boolean started = false;
        boolean skipWait = false;
        try {
            skipWait = storageWarmerService.isLoaderAvailable();
            if (!skipWait) {
                storageWarmerService.waitForLoaders();
            }
            storageWarmerService.tryRunningWarmFlow(flowId, rowGroupKey);
            stopWatch.start();
            statsWarmingService.incwarm_started();
            started = true;
            statsWarmingService.addwaiting_for_lock_nano(stopWatch.getNanoTime());
            warmingManager.warm(rowGroupKey,
                    connectorPageSourceProvider,
                    transactionHandle,
                    session,
                    dispatcherTableHandle,
                    dispatcherSplit,
                    dataToWarm.columnHandleList(),
                    dataToWarm.requiredWarmUpTypeMap(),
                    warmupElements,
                    partitionKeys,
                    skipWait);
        }
        catch (Exception e) {
            logFailure(e);
            if (started) {
                statsWarmingService.incwarm_failed();
            }
        }
        finally {
            boolean releaseTx = dataToWarm.txMemoryReserved();
            storageWarmerService.releaseLoaderThread(skipWait);
            storageWarmerService.finishWarm(flowId, releaseTx, true, true);
            stopWatch.stop();
            statsWarmingService.addexecution_time_nano(stopWatch.getNanoTime());
            logger.debug("warm flow finished nano sec = %d", stopWatch.getNanoTime());
            if (started) {
                statsWarmingService.incwarm_accomplished();
            }
        }

        // Just a precaution - make sure we're not stuck on an infinite loop of warmups.
        if (iterationCount >= globalConfiguration.getMaxWarmupIterationsPerQuery()) {
            logger.error("Max iteration count has reached (%d), won't try to warm again. rowGroupKey=%s, varadaColumns=%s, dataToWarm=%s",
                    globalConfiguration.getMaxWarmupIterationsPerQuery(),
                    rowGroupKey,
                    columns.stream().map(dispatcherProxiedConnectorTransformer::getVaradaRegularColumn).collect(Collectors.toList()),
                    dataToWarm);
            eventBus.post(new WarmingFinishedEvent(rowGroupKey, session));
        }
        else {
            runAnotherWarmUpIteration();
        }
    }

    private void runAnotherWarmUpIteration()
    {
        WarmData dataToWarm = getWarmData(true);
        WarmExecutionState warmExecutionState = dataToWarm.warmExecutionState();
        switch (warmExecutionState) {
            case WARM:
                nextTask = Optional.of(createProxyExecutionTask((int) dataToWarm.highestPriority()));
                break;
            case EMPTY_ROW_GROUP:
            case NOTHING_TO_WARM:
                eventBus.post(new WarmingFinishedEvent(rowGroupKey, session));
                break;
            default:
                throw new RuntimeException(String.format("state: %s is not valid, only warm or NOTHING_TO_WARM are valid", warmExecutionState));
        }
    }

    private Map<VaradaColumn, String> getPartitionKeys(DispatcherSplit dispatcherSplit)
    {
        return dispatcherSplit.getPartitionKeys().stream().collect(Collectors.toMap(PartitionKey::regularColumn, PartitionKey::partitionValue));
    }

    @Override
    protected WarmData getWarmData()
    {
        return getWarmData(false);
    }
}
