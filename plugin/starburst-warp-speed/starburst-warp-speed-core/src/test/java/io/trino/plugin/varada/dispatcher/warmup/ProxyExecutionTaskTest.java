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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.connector.TestingConnectorColumnHandle;
import io.trino.plugin.varada.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.classifier.ClassificationType;
import io.trino.plugin.varada.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.dispatcher.warmup.events.WarmingFinishedEvent;
import io.trino.plugin.varada.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmingManager;
import io.trino.plugin.varada.dispatcher.warmup.warmers.WarmupElementsCreator;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.juffer.StorageEngineTxService;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.engine.StubsStorageEngine;
import io.trino.plugin.varada.storage.flows.FlowType;
import io.trino.plugin.varada.storage.flows.FlowsSequencer;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.varada.VaradaSessionProperties.PREDICATE_SIMPLIFY_THRESHOLD;
import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyExecutionTaskTest
{
    private WarmupDemoterService warmupDemoterService;
    private ConnectorPageSourceProvider connectorPageSourceProvider;
    private ConnectorTransactionHandle connectorTransactionHandle;
    private DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private ConnectorSession connectorSession;
    private QueryContext queryContext;
    private WarmingManager warmingManager;
    private DispatcherTableHandle dispatcherTableHandle;
    private WorkerWarmingService workerWarmingService;
    private RowGroupKey rowGroupKey;
    private RowGroupData rowGroupData;
    private DispatcherSplit dispatcherSplit;
    private List<ColumnHandle> columnHandleList;
    private FlowsSequencer flowsSequencer;
    private GlobalConfiguration globalConfiguration;
    private StorageEngineTxService storageEngineTxService;
    private QueryClassifier queryClassifier;
    private RowGroupDataService rowGroupDataService;
    private WarmupElementsCreator warmupElementsCreator;
    private SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap;

    @BeforeEach
    public void before()
    {
        warmupDemoterService = mock(WarmupDemoterService.class);
        connectorPageSourceProvider = mock(ConnectorPageSourceProvider.class);
        connectorSession = mock(ConnectorSession.class);
        when(connectorSession.getProperty(eq(PREDICATE_SIMPLIFY_THRESHOLD), eq(Integer.class))).thenReturn(1_000_000);
        dispatcherTableHandle = mock(DispatcherTableHandle.class);
        workerWarmingService = mock(WorkerWarmingService.class);
        queryContext = mock(QueryContext.class);
        queryClassifier = mock(QueryClassifier.class);
        dispatcherProxiedConnectorTransformer = mock(DispatcherProxiedConnectorTransformer.class);
        when(queryClassifier.classify(any(), any(), any(), eq(Optional.of(connectorSession)), eq(Optional.empty()), any())).thenReturn(queryContext);
        warmingManager = mock(WarmingManager.class);
        when(warmingManager.importWeGroup(any(ConnectorSession.class), any(RowGroupKey.class), anyList())).thenReturn(Optional.empty());
        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        rowGroupKey = new RowGroupKey(schemaTableName.getSchemaName(), schemaTableName.getTableName(), "file_path", 0, 1L, 0, "", "");
        rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(List.of())
                //.totalRecords(10)
                .build();
        rowGroupDataService = mock(RowGroupDataService.class);
        when(rowGroupDataService.get(any())).thenReturn(rowGroupData);
        dispatcherSplit = mock(DispatcherSplit.class);
        connectorTransactionHandle = mock(ConnectorTransactionHandle.class);
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(IntegerType.INTEGER, "column");
        columnHandleList = List.of(columnHandle);
        flowsSequencer = mock(FlowsSequencer.class);
        globalConfiguration = new GlobalConfiguration();
        NativeConfiguration nativeConfiguration = new NativeConfiguration();
        nativeConfiguration.setTaskMinWarmingThreads(2);
        RegularColumn regularColumn = new RegularColumn(columnHandle.name());
        storageEngineTxService = new StorageEngineTxService(nativeConfiguration, mock(MetricsManager.class));
        requiredWarmUpTypeMap = HashMultimap.create();
        requiredWarmUpTypeMap.put(regularColumn, new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 0, 0, TransformFunction.NONE));
        MetricsManager metricsManager = mock(MetricsManager.class);
        warmupElementsCreator = new WarmupElementsCreator(
                rowGroupDataService,
                metricsManager,
                mock(StorageEngineConstants.class),
                new TestingConnectorProxiedConnectorTransformer(), globalConfiguration);
    }

    @Test
    public void testWarmExecutionTask_warmDataIsEmpty()
    {
        VaradaStatsWarmingService varadaStatsWarmingService = VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP);

        when(workerWarmingService.getWarmData(eq(columnHandleList),
                eq(rowGroupKey),
                eq(dispatcherSplit),
                eq(connectorSession),
                any(QueryContext.class),
                anyBoolean()
        )).thenReturn(new WarmData(columnHandleList, requiredWarmUpTypeMap, WarmExecutionState.EMPTY_ROW_GROUP, true, queryContext, null));

        EventBus eventBus = mock(EventBus.class);
        ProxyExecutionTask proxyExecutionTask = createWarmExecutionTask(varadaStatsWarmingService, eventBus);
        when(queryClassifier.getBasicQueryContext(eq(columnHandleList), eq(dispatcherTableHandle), eq(DynamicFilter.EMPTY), eq(connectorSession))).thenReturn(queryContext);
        when(queryClassifier.classify(eq(queryContext), eq(rowGroupData), eq(dispatcherTableHandle), eq(Optional.of(connectorSession)), eq(Optional.empty()), eq(ClassificationType.QUERY))).thenReturn(queryContext);
        proxyExecutionTask.run();

        assertThat(varadaStatsWarmingService.getwarm_finished()).isEqualTo(1);
        verify(workerWarmingService, times(1)).removeRowGroupFromSubmittedRowGroup(eq(rowGroupKey));
        verify(eventBus, times(1)).post(isA(WarmingFinishedEvent.class));
        assertThat(storageEngineTxService.getRunningLoaders()).isZero();
    }

    @Test
    public void testFailedTypeShouldReleaseAllocation()
    {
        VaradaStatsWarmingService varadaStatsWarmingService = VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP);
        when(workerWarmingService.getWarmData(eq(columnHandleList),
                eq(rowGroupKey),
                eq(dispatcherSplit),
                eq(connectorSession),
                any(QueryContext.class),
                anyBoolean()
        )).thenReturn(new WarmData(columnHandleList, requiredWarmUpTypeMap, WarmExecutionState.WARM, true, queryContext, null),
                new WarmData(columnHandleList, requiredWarmUpTypeMap, WarmExecutionState.NOTHING_TO_WARM, true, queryContext, null));
        when(flowsSequencer.tryRunningFlow(eq(FlowType.WARMUP), anyLong(), any())).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(true));

        EventBus eventBus = mock(EventBus.class);
        ProxyExecutionTask proxyExecutionTask = createWarmExecutionTask(varadaStatsWarmingService, eventBus);
        when(queryClassifier.getBasicQueryContext(eq(columnHandleList), eq(dispatcherTableHandle), eq(DynamicFilter.EMPTY), any())).thenReturn(queryContext);
        when(queryClassifier.classify(eq(queryContext), eq(rowGroupData), eq(dispatcherTableHandle), eq(Optional.of(connectorSession)), eq(Optional.empty()), eq(ClassificationType.QUERY))).thenReturn(queryContext);
        proxyExecutionTask.run();
        verify(warmupDemoterService, times(1)).releaseTx();
        verify(workerWarmingService, times(1)).removeRowGroupFromSubmittedRowGroup(any());
    }

    /**
     * validate more tha a single iteration nothing to warm
     */
    @Test
    public void testWarmExecutionTask_SingleIteration_nothingToWarm()
    {
        VaradaStatsWarmingService varadaStatsWarmingService = VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP);

        WarmData warmData = new WarmData(columnHandleList, requiredWarmUpTypeMap, WarmExecutionState.WARM, true, queryContext, null);
        WarmData nothingToWarm = new WarmData(columnHandleList, requiredWarmUpTypeMap, WarmExecutionState.NOTHING_TO_WARM, false, queryContext, null);
        when(workerWarmingService.getWarmData(any(),
                any(),
                any(),
                any(),
                any(),
                anyBoolean()
        )).thenReturn(warmData).thenReturn(nothingToWarm);

        testAndAssert2Iterations(varadaStatsWarmingService);
    }

    /**
     * validate more tha a single iteration empty row group
     */
    @Test
    public void testWarmExecutionTask_SingleIteration_emptyRowGroup()
    {
        VaradaStatsWarmingService varadaStatsWarmingService = VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP);

        WarmData emptyRowGroup = new WarmData(columnHandleList, requiredWarmUpTypeMap, WarmExecutionState.EMPTY_ROW_GROUP, false, queryContext, null);
        when(workerWarmingService.getWarmData(any(),
                any(),
                any(),
                any(),
                any(),
                anyBoolean()
        )).thenReturn(emptyRowGroup);
        testAndAssertNoIterations(varadaStatsWarmingService);
    }

    @Test
    public void testWarmExecutionTask_max_warmup_iterations()
    {
        VaradaStatsWarmingService varadaStatsWarmingService = VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP);
        globalConfiguration.setMaxWarmupIterationsPerQuery(1);
        WarmData warmData = new WarmData(columnHandleList, requiredWarmUpTypeMap, WarmExecutionState.WARM, true, queryContext, null);
        when(workerWarmingService.getWarmData(any(),
                any(),
                any(),
                any(),
                any(),
                anyBoolean()
        )).thenReturn(warmData);
        testAndAssert2Iterations(varadaStatsWarmingService);
        verify(workerWarmingService, times(1))
                .getWarmData(any(),
                        any(),
                        any(),
                        any(),
                        any(),
                        anyBoolean());
    }

    private void testAndAssertNoIterations(VaradaStatsWarmingService varadaStatsWarmingService)
    {
        when(flowsSequencer.tryRunningFlow(eq(FlowType.WARMUP), anyLong(), any())).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(true));
        EventBus eventBus = mock(EventBus.class);
        ProxyExecutionTask proxyExecutionTask = createWarmExecutionTask(varadaStatsWarmingService, eventBus);

        proxyExecutionTask.run();
        verify(eventBus, times(1)).post(isA(WarmingFinishedEvent.class));
        assertThat(varadaStatsWarmingService.getwarm_finished()).isEqualTo(1);
        assertThat(varadaStatsWarmingService.getwarm_started()).isEqualTo(1);
        verify(warmingManager, times(1)).warmEmptyRowGroup(eq(rowGroupKey), anyList());
        verify(flowsSequencer, never()).flowFinished(eq(FlowType.WARMUP), anyLong(), eq(true));
        verify(warmupDemoterService, never()).tryDemoteStart();
        verify(workerWarmingService, times(1)).removeRowGroupFromSubmittedRowGroup(eq(rowGroupKey));
        assertThat(storageEngineTxService.getRunningLoaders()).isZero();
    }

    private void testAndAssert2Iterations(VaradaStatsWarmingService varadaStatsWarmingService)
    {
        when(flowsSequencer.tryRunningFlow(eq(FlowType.WARMUP), anyLong(), any())).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(true));
        EventBus eventBus = mock(EventBus.class);
        ProxyExecutionTask proxyExecutionTask = createWarmExecutionTask(varadaStatsWarmingService, eventBus);

        proxyExecutionTask.run();
        verify(eventBus, times(1)).post(isA(WarmingFinishedEvent.class));
        verify(warmupDemoterService, times(1)).releaseTx();
        assertThat(varadaStatsWarmingService.getwarm_finished()).isEqualTo(1);
        assertThat(varadaStatsWarmingService.getwarm_started()).isEqualTo(1);
        verify(flowsSequencer, times(1)).flowFinished(eq(FlowType.WARMUP), anyLong(), eq(true));
        verify(warmupDemoterService, times(1)).tryDemoteStart();
        verify(workerWarmingService, times(1)).removeRowGroupFromSubmittedRowGroup(eq(rowGroupKey));
        assertThat(storageEngineTxService.getRunningLoaders()).isZero();
    }

    private ProxyExecutionTask createWarmExecutionTask(VaradaStatsWarmingService varadaStatsWarmingService, EventBus eventBus)
    {
        StorageWarmerService storageWarmerService = new StorageWarmerService(rowGroupDataService, new StubsStorageEngine(), globalConfiguration, mock(ConnectorSync.class), warmupDemoterService, storageEngineTxService, flowsSequencer, TestingTxService.createMetricsManager());
        return new ProxyExecutionTask(mock(WarmExecutionTaskFactory.class),
                eventBus,
                dispatcherProxiedConnectorTransformer,
                warmingManager,
                varadaStatsWarmingService,
                workerWarmingService,
                connectorPageSourceProvider,
                connectorTransactionHandle,
                connectorSession,
                dispatcherTableHandle,
                rowGroupKey,
                columnHandleList,
                dispatcherSplit,
                DynamicFilter.EMPTY,
                rowGroupDataService,
                globalConfiguration,
                queryClassifier,
                warmupElementsCreator,
                1,
                0,
                mock(WorkerTaskExecutorService.class),
                storageWarmerService);
    }
}
