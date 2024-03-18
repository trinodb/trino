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
package io.trino.plugin.varada.dispatcher.warmup.warmers;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.juffer.StorageEngineTxService;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StubsStorageEngine;
import io.trino.plugin.varada.storage.flows.FlowsSequencer;
import io.trino.plugin.varada.storage.write.StorageWriterService;
import io.trino.plugin.varada.storage.write.VaradaPageSinkFactory;
import io.trino.plugin.varada.util.FailureGeneratorInvocationHandler;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsTxService;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import io.varada.tools.util.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.createRegularWarmupElements;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.mockColumns;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.mockConnectorSplit;
import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static io.trino.plugin.varada.storage.flows.FlowsSequencer.STATS_GROUP_NAME;
import static io.trino.plugin.varada.util.NodeUtils.mockNodeManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class VaradaProxiedWarmerTest
{
    private final int defaultPriority = 2;
    private final int notEmptyTTL = 2;
    private GlobalConfiguration globalConfiguration;
    private DispatcherTableHandle dispatcherTableHandle;
    private ConnectorTransactionHandle connectorTransactionHandle;
    private ConnectorPageSourceProvider connectorPageSourceProvider;
    private StorageEngineTxService storageEngineTxService;
    private StorageEngine storageEngine;
    private RowGroupDataService rowGroupDataService;
    private DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private ConnectorSession connectorSession;
    private ConnectorPageSource connectorPageSource;

    @SuppressWarnings("MockNotUsedInProduction")
    @BeforeEach
    public void before()
            throws IOException
    {
        storageEngine = new StubsStorageEngine();
        rowGroupDataService = mock(RowGroupDataService.class);
        doAnswer(invocation -> {
            RowGroupData rowGroupData = (RowGroupData) invocation.getArguments()[0];
            WarmUpElement warmUpElement = (WarmUpElement) invocation.getArguments()[1];
            List<WarmUpElement> updatedWarmupElement = new ArrayList<>(rowGroupData.getWarmUpElements());
            updatedWarmupElement.add(warmUpElement);
            return RowGroupData.builder(rowGroupData).warmUpElements(updatedWarmupElement).build();
        }).when(rowGroupDataService).updateRowGroupData(any(RowGroupData.class), any(WarmUpElement.class), anyInt(), anyInt());
        dispatcherProxiedConnectorTransformer = mock(DispatcherProxiedConnectorTransformer.class);
        connectorTransactionHandle = mock(ConnectorTransactionHandle.class);
        connectorPageSourceProvider = mock(ConnectorPageSourceProvider.class);
        dispatcherTableHandle = mock(DispatcherTableHandle.class);
        MetricsManager metricsManager = mock(MetricsManager.class);
        connectorSession = mock(ConnectorSession.class);
        when(connectorSession.getProperty(eq("warm_data_varchar_max_length"), any())).thenReturn(1000);
        VaradaStatsWarmingService varadaStatsWarmingService = VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP);
        VaradaStatsTxService varadaStatsTxService = VaradaStatsTxService.create(STATS_GROUP_NAME);
        when(metricsManager.registerMetric(isA(VaradaStatsWarmingService.class))).thenReturn(varadaStatsWarmingService);
        when(metricsManager.registerMetric(isA(VaradaStatsTxService.class))).thenReturn(varadaStatsTxService);
        when(metricsManager.get(WARMING_SERVICE_STAT_GROUP)).thenReturn(varadaStatsWarmingService);
        when(metricsManager.get(STATS_GROUP_NAME)).thenReturn(varadaStatsTxService);
        storageEngineTxService = mock(StorageEngineTxService.class);
        connectorPageSource = mock(ConnectorPageSource.class);
        globalConfiguration = new GlobalConfiguration();
        globalConfiguration.setLocalStorePath(
                Files.createTempDirectory(this.getClass().getName()).toFile().getAbsolutePath());
        when(connectorPageSourceProvider.createPageSource(any(),
                any(),
                any(),
                any(),
                any(),
                isA(DynamicFilter.class))).thenReturn(connectorPageSource);
    }

    @Test
    public void testVaradaWarmerWarmSuccessSingleElementNonLucene()
            throws IOException
    {
        VaradaProxiedWarmer varadaProxiedWarmer = createVaradaProxiedWarmer();

        Pair<DispatcherSplit, RowGroupKey> dispatcherSplitRowGroupKeyPair = mockConnectorSplit();
        String column1 = "C1";
        List<ColumnHandle> columnsToWarm = mockColumns(dispatcherProxiedConnectorTransformer, List.of(Pair.of(column1, IntegerType.INTEGER)));
        Multimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = ArrayListMultimap.create();
        columnNameToWarmUpType.put(new RegularColumn(column1), WarmUpType.WARM_UP_TYPE_BASIC);
        SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap = createWarmupPriorityMap(columnNameToWarmUpType);
        List<WarmUpElement> warmupElements = createRegularWarmupElements(columnNameToWarmUpType);

        ConnectorPageSource connectorPageSource = mockConnectorPageSourceForEachColumn(columnsToWarm).stream().findAny().orElseThrow();
        when(connectorPageSource.isFinished()).thenReturn(false).thenReturn(true);
        RowGroupData rowGroupData = act(varadaProxiedWarmer, dispatcherSplitRowGroupKeyPair, columnsToWarm, warmupElements, requiredWarmUpTypeMap);
        verify(connectorPageSource, times(1)).close();
        assertThat(rowGroupData.getValidWarmUpElements().size()).isEqualTo(warmupElements.size());
    }

    @Test
    public void testVaradaWarmerWarmSuccessMixedWithLucene()
            throws IOException
    {
        VaradaProxiedWarmer varadaProxiedWarmer = createVaradaProxiedWarmer();

        Pair<DispatcherSplit, RowGroupKey> dispatcherSplitRowGroupKeyPair = mockConnectorSplit();
        String column1 = "C1";
        String column2 = "C2";
        String column3 = "C3";
        List<ColumnHandle> columnsToWarm = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(column1, VarcharType.VARCHAR),
                        Pair.of(column2, IntegerType.INTEGER),
                        Pair.of(column3, VarcharType.VARCHAR)));
        Multimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = ArrayListMultimap.create();
        columnNameToWarmUpType.putAll(new RegularColumn(column1), List.of(WarmUpType.WARM_UP_TYPE_BASIC,
                WarmUpType.WARM_UP_TYPE_DATA,
                WarmUpType.WARM_UP_TYPE_LUCENE));
        columnNameToWarmUpType.put(new RegularColumn(column2), WarmUpType.WARM_UP_TYPE_BASIC);
        columnNameToWarmUpType.putAll(new RegularColumn(column3), List.of(WarmUpType.WARM_UP_TYPE_BASIC,
                WarmUpType.WARM_UP_TYPE_DATA));
        List<WarmUpElement> warmupElements = createRegularWarmupElements(columnNameToWarmUpType);
        SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap = createWarmupPriorityMap(columnNameToWarmUpType);
        List<ConnectorPageSource> connectorPageSources = mockConnectorPageSourceForEachColumn(columnsToWarm);
        connectorPageSources.forEach(c -> when(c.isFinished()).thenReturn(false).thenReturn(true));
        RowGroupData rowGroupData = act(varadaProxiedWarmer, dispatcherSplitRowGroupKeyPair, columnsToWarm, warmupElements, requiredWarmUpTypeMap);
        verify(connectorPageSources.get(0), times(3)).close();
        verify(connectorPageSources.get(1), times(1)).close();
        verify(connectorPageSources.get(2), times(2)).close();
        assertThat(rowGroupData.getValidWarmUpElements().size()).isEqualTo(columnNameToWarmUpType.values().size());
    }

    private List<ConnectorPageSource> mockConnectorPageSourceForEachColumn(List<ColumnHandle> columns)
    {
        return columns.stream().map(columnHandles -> {
            ConnectorPageSource connectorPageSource = mock(ConnectorPageSource.class);
            when(connectorPageSourceProvider.createPageSource(any(),
                    any(),
                    any(),
                    any(),
                    eq(List.of(columnHandles)),
                    isA(DynamicFilter.class))).thenReturn(connectorPageSource);
            return connectorPageSource;
        }).toList();
    }

    @Test
    public void testAllocateByPriorityDifferentWarmupElements()
            throws IOException
    {
        VaradaProxiedWarmer varadaProxiedWarmer = createVaradaProxiedWarmer();
        Pair<DispatcherSplit, RowGroupKey> dispatcherSplitRowGroupKeyPair = mockConnectorSplit();
        RegularColumn column0 = new RegularColumn("c0");
        RegularColumn column1 = new RegularColumn("c1");
        RegularColumn column2 = new RegularColumn("c2");
        RegularColumn column3 = new RegularColumn("c3");
        RegularColumn column4 = new RegularColumn("c4");
        List<ColumnHandle> columnsToWarm = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(column0.getName(), VarcharType.VARCHAR),
                        Pair.of(column1.getName(), VarcharType.VARCHAR),
                        Pair.of(column2.getName(), VarcharType.VARCHAR),
                        Pair.of(column3.getName(), VarcharType.VARCHAR),
                        Pair.of(column4.getName(), VarcharType.VARCHAR)));
        Multimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = ArrayListMultimap.create();
        columnNameToWarmUpType.putAll(column0, List.of(WarmUpType.WARM_UP_TYPE_DATA, WarmUpType.WARM_UP_TYPE_BASIC));
        columnNameToWarmUpType.put(column1, WarmUpType.WARM_UP_TYPE_DATA);
        columnNameToWarmUpType.put(column2, WarmUpType.WARM_UP_TYPE_DATA);
        columnNameToWarmUpType.put(column3, WarmUpType.WARM_UP_TYPE_DATA);
        columnNameToWarmUpType.put(column4, WarmUpType.WARM_UP_TYPE_DATA);
        List<WarmUpElement> warmupElements = createRegularWarmupElements(columnNameToWarmUpType);
        SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap = HashMultimap.create();
        requiredWarmUpTypeMap.putAll(column0, Set.of(new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, 3, 0, TransformFunction.NONE),
                new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 7, 0, TransformFunction.NONE)));
        requiredWarmUpTypeMap.put(column1, new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, 9, 0, TransformFunction.NONE));
        requiredWarmUpTypeMap.put(column2, new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, 5, 0, TransformFunction.NONE));
        requiredWarmUpTypeMap.put(column3, new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, 6, 0, TransformFunction.NONE));
        requiredWarmUpTypeMap.put(column4, new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, 2, 0, TransformFunction.NONE));

        when(connectorPageSource.isFinished()).thenReturn(false).thenReturn(true);

        RowGroupData rowGroupData = act(varadaProxiedWarmer, dispatcherSplitRowGroupKeyPair, columnsToWarm, warmupElements, requiredWarmUpTypeMap);

        verify(connectorPageSource, times(6)).close();
        assertThat(rowGroupData.getValidWarmUpElements().size()).isEqualTo(columnNameToWarmUpType.values().size());
        assertThat(rowGroupData.getValidWarmUpElements().get(0).getVaradaColumn()).isEqualTo(column1);
        assertThat(rowGroupData.getValidWarmUpElements().get(1).getVaradaColumn()).isEqualTo(column0);
        assertThat(rowGroupData.getValidWarmUpElements().get(2).getVaradaColumn()).isEqualTo(column0);
        assertThat(rowGroupData.getValidWarmUpElements().get(3).getVaradaColumn()).isEqualTo(column3);
        assertThat(rowGroupData.getValidWarmUpElements().get(4).getVaradaColumn()).isEqualTo(column2);
        assertThat(rowGroupData.getValidWarmUpElements().get(5).getVaradaColumn()).isEqualTo(column4);
    }

    @Disabled("Todo: do we need to sort column's elements by priority?")
    @Test
    public void testSameColumnDifferentPriority()
            throws IOException
    {
        VaradaProxiedWarmer varadaProxiedWarmer = createVaradaProxiedWarmer();
        Pair<DispatcherSplit, RowGroupKey> dispatcherSplitRowGroupKeyPair = mockConnectorSplit();
        String column1 = "C1";
        VaradaColumn varadaColumn1 = new RegularColumn(column1);
        List<ColumnHandle> columnsToWarm = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(column1, VarcharType.VARCHAR)));
        Multimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = ArrayListMultimap.create();
        columnNameToWarmUpType.putAll(varadaColumn1, List.of(WarmUpType.WARM_UP_TYPE_DATA,
                WarmUpType.WARM_UP_TYPE_BASIC));

        List<WarmUpElement> warmupElements = createRegularWarmupElements(columnNameToWarmUpType);
        SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap = HashMultimap.create();
        requiredWarmUpTypeMap.putAll(varadaColumn1, Set.of(new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, 2, 0, TransformFunction.NONE),
                new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 3, 0, TransformFunction.NONE)));

        when(connectorPageSource.isFinished()).thenReturn(true);

        RowGroupData rowGroupData = act(varadaProxiedWarmer, dispatcherSplitRowGroupKeyPair, columnsToWarm, warmupElements, requiredWarmUpTypeMap);

        verify(connectorPageSource, times(1)).close();
        assertThat(rowGroupData.getValidWarmUpElements().size()).isEqualTo(columnNameToWarmUpType.values().size());
        assertThat(rowGroupData.getValidWarmUpElements().get(0).getWarmUpType()).isEqualTo(WarmUpType.WARM_UP_TYPE_BASIC);
        assertThat(rowGroupData.getValidWarmUpElements().get(1).getWarmUpType()).isEqualTo(WarmUpType.WARM_UP_TYPE_BASIC);
        assertThat(rowGroupData.getValidWarmUpElements().get(0).getVaradaColumn()).isEqualTo(varadaColumn1);
    }

    @Test
    public void testVaradaWarmerWarmFailed()
    {
        Assertions.assertThrows(RuntimeException.class, () -> {
            VaradaProxiedWarmer varadaProxiedWarmer = createVaradaProxiedWarmer();

            Pair<DispatcherSplit, RowGroupKey> dispatcherSplitRowGroupKeyPair = mockConnectorSplit();
            RegularColumn column1 = new RegularColumn("c3");
            List<ColumnHandle> columnsToWarm = mockColumns(dispatcherProxiedConnectorTransformer,
                    List.of(Pair.of(column1.getName(), IntegerType.INTEGER)));
            Multimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = ArrayListMultimap.create();
            columnNameToWarmUpType.put(column1, WarmUpType.WARM_UP_TYPE_BASIC);
            SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap = createWarmupPriorityMap(columnNameToWarmUpType);
            List<WarmUpElement> warmupElements = createRegularWarmupElements(columnNameToWarmUpType);
            when(connectorPageSource.isFinished()).thenThrow(new RuntimeException("some exception"));

            try {
                act(varadaProxiedWarmer, dispatcherSplitRowGroupKeyPair, columnsToWarm, warmupElements, requiredWarmUpTypeMap);
            }
            catch (Exception e) {
                verify(connectorPageSource, times(1)).close();
                throw e;
            }
        });
    }

    @Test
    public void testVaradaWarmerWarmFailedDuringWarm()
    {
        Assertions.assertThrows(RuntimeException.class, () -> {
            VaradaProxiedWarmer varadaProxiedWarmer = createVaradaProxiedWarmer();

            Pair<DispatcherSplit, RowGroupKey> dispatcherSplitRowGroupKeyPair = mockConnectorSplit();
            RegularColumn column1 = new RegularColumn("c1");
            List<ColumnHandle> columnsToWarm = mockColumns(dispatcherProxiedConnectorTransformer,
                    List.of(Pair.of(column1.getName(), IntegerType.INTEGER)));
            Multimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = ArrayListMultimap.create();
            columnNameToWarmUpType.put(column1, WarmUpType.WARM_UP_TYPE_BASIC);
            SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap = createWarmupPriorityMap(columnNameToWarmUpType);
            List<WarmUpElement> warmupElements = createRegularWarmupElements(columnNameToWarmUpType);

            when(connectorPageSource.isFinished()).thenReturn(false);
            when(connectorPageSource.getNextPage()).thenThrow(new RuntimeException("exception during warm"));
            try {
                act(varadaProxiedWarmer, dispatcherSplitRowGroupKeyPair, columnsToWarm, warmupElements, requiredWarmUpTypeMap);
            }
            catch (Exception e) {
                verify(connectorPageSource, times(1)).close();
                throw e;
            }
        });
    }

    private RowGroupData act(VaradaProxiedWarmer varadaProxiedWarmer,
            Pair<DispatcherSplit, RowGroupKey> dispatcherSplitRowGroupKeyPair,
            List<ColumnHandle> columnsToWarm,
            List<WarmUpElement> warmupElements,
            SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypeMap)
    {
        RowGroupData rowGroupData = RowGroupData.builder().rowGroupKey(dispatcherSplitRowGroupKeyPair.getRight()).warmUpElements(List.of()).build();
        return varadaProxiedWarmer.warm(connectorPageSourceProvider,
                connectorTransactionHandle,
                connectorSession,
                dispatcherTableHandle,
                dispatcherSplitRowGroupKeyPair.getRight(),
                rowGroupData,
                dispatcherSplitRowGroupKeyPair.getLeft(),
                columnsToWarm,
                warmupElements,
                requiredWarmUpTypeMap,
                true,
                0,
                new ArrayList<>());
    }

    private VaradaProxiedWarmer createVaradaProxiedWarmer()
    {
        NodeManager nodeManager = mockNodeManager();
        StorageWriterService storageWriterService = mock(StorageWriterService.class);
        VaradaPageSinkFactory varadaPageSinkFactory = new VaradaPageSinkFactory(
                mock(FailureGeneratorInvocationHandler.class),
                storageWriterService,
                new GlobalConfiguration());
        ConnectorSync connectorSync = mock(ConnectorSync.class);
        StorageWarmerService storageWarmerService = new StorageWarmerService(rowGroupDataService, storageEngine, globalConfiguration, connectorSync, mock(WarmupDemoterService.class), storageEngineTxService, mock(FlowsSequencer.class), TestingTxService.createMetricsManager());
        return new VaradaProxiedWarmer(varadaPageSinkFactory,
                dispatcherProxiedConnectorTransformer,
                nodeManager,
                connectorSync,
                globalConfiguration,
                rowGroupDataService,
                storageWarmerService,
                storageWriterService);
    }

    private SetMultimap<VaradaColumn, WarmupProperties> createWarmupPriorityMap(Multimap<VaradaColumn, WarmUpType> columnNameToWarmUpType)
    {
        SetMultimap<VaradaColumn, WarmupProperties> warmupPriorityMap = HashMultimap.create();
        columnNameToWarmUpType.entries().forEach(entry -> {
            Set<WarmupProperties> warmupProperties = warmupPriorityMap.get(entry.getKey());
            warmupProperties.add(new WarmupProperties(entry.getValue(), defaultPriority, notEmptyTTL, TransformFunction.NONE));
        });
        return warmupPriorityMap;
    }
}
