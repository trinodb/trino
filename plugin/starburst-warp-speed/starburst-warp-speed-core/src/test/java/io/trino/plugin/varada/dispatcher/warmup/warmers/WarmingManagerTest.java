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

import com.google.common.collect.SetMultimap;
import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.connector.TestingConnectorColumnHandle;
import io.trino.plugin.varada.dictionary.DictionaryCacheService;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.dal.RowGroupDataDao;
import io.trino.plugin.varada.dispatcher.model.RecordData;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WarmData;
import io.trino.plugin.varada.dispatcher.warmup.WarmExecutionState;
import io.trino.plugin.varada.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.juffer.StorageEngineTxService;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StubsStorageEngine;
import io.trino.plugin.varada.storage.flows.FlowsSequencer;
import io.trino.plugin.varada.storage.write.StorageWriterService;
import io.trino.plugin.varada.storage.write.VaradaPageSinkFactory;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.varada.util.FailureGeneratorInvocationHandler;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.NodeManager;
import io.trino.spi.Page;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;
import io.varada.tools.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.createRequiredWarmUpTypes;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.mockColumns;
import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static io.trino.plugin.varada.util.NodeUtils.mockNodeManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class WarmingManagerTest
{
    private final RuntimeException testException = new RuntimeException("TEST EXCEPTION");

    protected DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private SchemaTableName schemaTableName;
    private RowGroupKey rowGroupKey;
    private List<RecordData> recordDataList;
    private GlobalConfiguration globalConfiguration;
    private CloudVendorConfiguration cloudVendorConfiguration;
    private VaradaStatsWarmingService varadaStatsWarmingService;
    private StorageEngine storageEngine;
    private ConnectorSession connectorSession;
    private DispatcherTableHandle dispatcherTableHandle;
    private DispatcherSplit dispatcherSplit;
    private ConnectorTransactionHandle connectorTransactionHandle;
    private DictionaryCacheService dictionaryCacheService;
    private MetricsManager metricsManager;
    private RowGroupDataService rowGroupDataService;
    private WarmingManager warmingManager;
    private NodeManager nodeManager;

    @BeforeEach
    public void before()
            throws IOException
    {
        schemaTableName = new SchemaTableName("schema", "table");
        rowGroupKey = new RowGroupKey(schemaTableName.getSchemaName(), schemaTableName.getTableName(), "file_path", 0, 1L, 0, "", "");
        globalConfiguration = new GlobalConfiguration();
        globalConfiguration.setMaxWarmRetries(5);
        globalConfiguration.setLocalStorePath(
                Files.createTempDirectory(this.getClass().getName()).toFile().getAbsolutePath());
        cloudVendorConfiguration = new CloudVendorConfiguration();
        varadaStatsWarmingService = VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP);
        storageEngine = new StubsStorageEngine();
        connectorSession = mock(ConnectorSession.class);
        when(connectorSession.getProperty(eq("warm_data_varchar_max_length"), any())).thenReturn(1000);
        dispatcherTableHandle = mock(DispatcherTableHandle.class);
        when(dispatcherTableHandle.getSchemaTableName()).thenReturn(schemaTableName);
        dispatcherSplit = mock(DispatcherSplit.class);
        connectorTransactionHandle = mock(ConnectorTransactionHandle.class);
        dictionaryCacheService = mock(DictionaryCacheService.class);
        metricsManager = mockMetricsManager();
        nodeManager = mockNodeManager();
        rowGroupDataService = mockRowGroupDataService();
        dispatcherProxiedConnectorTransformer = mock(DispatcherProxiedConnectorTransformer.class);

        warmingManager = createWarmingManager();
    }

    private RowGroupDataService mockRowGroupDataService()
    {
        RowGroupDataService rowGroupDataService = spy(new RowGroupDataService(mock(RowGroupDataDao.class),
                storageEngine,
                globalConfiguration,
                metricsManager,
                nodeManager,
                mock(ConnectorSync.class)));
        //Whitebox.setInternalState(rowGroupDataService, "varadaStatsWarmingService", varadaStatsWarmingService);

        // Mock get(rowGroupKey) to return the last saved RowGroupData
        ArgumentCaptor<RowGroupData> savedRowGroupData = ArgumentCaptor.forClass(RowGroupData.class);
        doAnswer((Answer<Void>) invocation -> {
            when(rowGroupDataService.get(rowGroupKey)).thenReturn((RowGroupData) invocation.getArguments()[0]);
            return null;
        }).when(rowGroupDataService).save(savedRowGroupData.capture());

        return rowGroupDataService;
    }

    private MetricsManager mockMetricsManager()
    {
        MetricsManager metricsManager = mock(MetricsManager.class);
        when(metricsManager.registerMetric(any())).thenAnswer(invocation -> invocation.getArguments()[0] instanceof VaradaStatsWarmingService
                ? varadaStatsWarmingService
                : invocation.getArguments()[0]);
        when(metricsManager.get(VaradaStatsWarmingService.createKey(WARMING_SERVICE_STAT_GROUP))).thenReturn(varadaStatsWarmingService);
        return metricsManager;
    }

    private WarmingManager createWarmingManager()
    {
        VaradaProxiedWarmer varadaProxiedWarmer = createVaradaProxiedWarmer(metricsManager, globalConfiguration, new NativeConfiguration());
        EmptyRowGroupWarmer emptyRowGroupWarmer = new EmptyRowGroupWarmer(rowGroupDataService);
        WeGroupWarmer weGroupWarmer = mock(WeGroupWarmer.class);
        return new WarmingManager(
                varadaProxiedWarmer,
                emptyRowGroupWarmer,
                globalConfiguration,
                cloudVendorConfiguration,
                rowGroupDataService,
                metricsManager,
                dictionaryCacheService,
                weGroupWarmer,
                mock(StorageWarmerService.class));
    }

    private VaradaProxiedWarmer createVaradaProxiedWarmer(
            MetricsManager metricsManager,
            GlobalConfiguration globalConfiguration,
            NativeConfiguration nativeConfiguration)
    {
        StorageEngineTxService storageEngineTxService = new StorageEngineTxService(nativeConfiguration, metricsManager);
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

    @Test
    public void testSingleColumnFailSeveralTimesAndSuccess()
            throws InterruptedException
    {
        WarmData dataToWarm = arrange();

        warmFirstTimeAndFail(dataToWarm);
        for (int i = 0; i < globalConfiguration.getMaxWarmRetries() - 1; i++) {
            retryAndFail(dataToWarm);
        }

        warmSuccessfully(dataToWarm);
    }

    @Test
    public void testSingleColumnFailMaxRetries()
            throws InterruptedException
    {
        WarmData dataToWarm = arrange();

        warmFirstTimeAndFail(dataToWarm);
        for (int i = 0; i < globalConfiguration.getMaxWarmRetries(); i++) {
            retryAndFail(dataToWarm);
        }
    }

    @Test
    public void testTwoColumnTwoWarmUpTypesFailSeveralTimesAndSuccess()
            throws InterruptedException
    {
        List<Pair<String, Type>> columnsMetadata = List.of(Pair.of("col1", IntegerType.INTEGER),
                Pair.of("col2", IntegerType.INTEGER));
        List<WarmUpType> warmUpTypeList = List.of(WarmUpType.WARM_UP_TYPE_BASIC,
                WarmUpType.WARM_UP_TYPE_DATA);
        WarmData dataToWarm = arrange(columnsMetadata, warmUpTypeList);

        warmFirstTimeAndFail(dataToWarm);
        for (int i = 0; i < globalConfiguration.getMaxWarmRetries() - 1; i++) {
            retryAndFail(dataToWarm);
        }

        warmSuccessfully(dataToWarm);
    }

    @Test
    public void testColFailThenAnotherColSuccess()
            throws InterruptedException
    {
        List<Pair<String, Type>> columnsMetadata = List.of(Pair.of("col1", IntegerType.INTEGER));
        List<WarmUpType> warmUpTypeList = List.of(WarmUpType.WARM_UP_TYPE_BASIC,
                WarmUpType.WARM_UP_TYPE_DATA);
        WarmData dataToWarm = arrange(columnsMetadata, warmUpTypeList);
        warmFirstTimeAndFail(dataToWarm);

        columnsMetadata = List.of(Pair.of("col2", IntegerType.INTEGER));
        dataToWarm = arrange(columnsMetadata, warmUpTypeList);
        warmSuccessfully(dataToWarm);
    }

    @Test
    public void testFailThenSuccessColumnAfterColumn()
            throws InterruptedException
    {
        List<Pair<String, Type>> columnsMetadata = List.of(Pair.of("col1", IntegerType.INTEGER));
        List<WarmUpType> warmUpTypeList = List.of(WarmUpType.WARM_UP_TYPE_BASIC);
        WarmData dataToWarm = arrange(columnsMetadata, warmUpTypeList);
        warmFirstTimeAndFail(dataToWarm);
        warmSuccessfully(dataToWarm);

        columnsMetadata = List.of(Pair.of("col2", IntegerType.INTEGER));
        dataToWarm = arrange(columnsMetadata, warmUpTypeList);
        warmFirstTimeAndFail(dataToWarm);
        warmSuccessfully(dataToWarm);
    }

    @Test
    public void testFailOneColThenFailAnotherColThenSuccess()
            throws InterruptedException
    {
        List<Pair<String, Type>> columnsMetadata = List.of(Pair.of("col1", IntegerType.INTEGER));
        List<WarmUpType> warmUpTypeList = List.of(WarmUpType.WARM_UP_TYPE_BASIC);
        WarmData dataToWarm = arrange(columnsMetadata, warmUpTypeList);
        warmFirstTimeAndFail(dataToWarm);
        retryAndFail(dataToWarm);

        columnsMetadata = List.of(Pair.of("col2", IntegerType.INTEGER));
        dataToWarm = arrange(columnsMetadata, warmUpTypeList);
        warmFirstTimeAndFail(dataToWarm);
        retryAndFail(dataToWarm);
        warmSuccessfully(dataToWarm);
    }

    @Test
    public void testSuccessThenFailAnotherCol()
            throws InterruptedException
    {
        List<Pair<String, Type>> columnsMetadata = List.of(Pair.of("col1", IntegerType.INTEGER));
        List<WarmUpType> warmUpTypeList = List.of(WarmUpType.WARM_UP_TYPE_BASIC,
                WarmUpType.WARM_UP_TYPE_DATA);
        WarmData dataToWarm = arrange(columnsMetadata, warmUpTypeList);
        warmSuccessfully(dataToWarm);

        columnsMetadata = List.of(Pair.of("col2", IntegerType.INTEGER));
        dataToWarm = arrange(columnsMetadata, warmUpTypeList);
        warmFirstTimeAndFail(dataToWarm);
        retryAndFail(dataToWarm);
        warmSuccessfully(dataToWarm);
    }

    private WarmData arrange()
    {
        return arrange(List.of(Pair.of("col1", IntegerType.INTEGER)),
                List.of(WarmUpType.WARM_UP_TYPE_BASIC),
                List.of(RecTypeCode.REC_TYPE_DECIMAL_SHORT));
    }

    private WarmData arrange(List<Pair<String, Type>> columnsMetadata, List<WarmUpType> warmUpTypeList)
    {
        List<RecTypeCode> types = columnsMetadata.stream()
                .map((a) -> RecTypeCode.REC_TYPE_DECIMAL_SHORT)
                .collect(Collectors.toList());

        return arrange(columnsMetadata, warmUpTypeList, types);
    }

    private WarmData arrange(
            List<Pair<String, Type>> columnsMetadata,
            List<WarmUpType> warmUpTypeList,
            List<RecTypeCode> recTypeCodes)
    {
        List<ColumnHandle> columns = mockColumns(columnsMetadata);
        for (ColumnHandle ch : columns) {
            RegularColumn regularColumn = new RegularColumn(((TestingConnectorColumnHandle) ch).name());
            when(dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(eq(ch))).thenReturn(regularColumn);
            when(dispatcherProxiedConnectorTransformer.getColumnType(eq(ch))).thenReturn(((TestingConnectorColumnHandle) ch).type());
        }
        int index = 0;
        recordDataList = new ArrayList<>(columnsMetadata.size());
        for (Pair<String, Type> columnMetadata : columnsMetadata) {
            RecordData recordData = new RecordData(
                    new SchemaTableColumn(schemaTableName, columnMetadata.getKey()),
                    columnMetadata.getValue(),
                    recTypeCodes.get(index),
                    TypeUtils.getTypeLength(columnMetadata.getValue(), 10));

            recordDataList.add(recordData);
            index++;
        }

        SetMultimap<VaradaColumn, WarmupProperties> requiredWarmUpTypes = createRequiredWarmUpTypes(columns, warmUpTypeList);
        return new WarmData(columns, requiredWarmUpTypes, WarmExecutionState.WARM, true, mock(QueryContext.class), null);
    }

    // Unsuccessfully warm X columns for the first time while there might be Y columns which are already exist (valid \ invalid).
    // At the first time we warm in a single batch.
    private void warmFirstTimeAndFail(WarmData dataToWarm)
            throws InterruptedException
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        long expectedNewWarmUpElements = countNewWarmUpElements(dataToWarm, rowGroupData);
        long alreadyExistWarmUpElements = rowGroupData == null ? 0 : rowGroupData.getWarmUpElements().size();

        RowGroupData newRowGroupData = executeWarmAndFail(dataToWarm);

        assertThat(newRowGroupData.getRowGroupKey()).isEqualTo(rowGroupKey);
        assertThat(newRowGroupData.getWarmUpElements().size()).isEqualTo(alreadyExistWarmUpElements + expectedNewWarmUpElements);

        List<WarmUpElement> newWarmUpElements = newRowGroupData.getWarmUpElements().stream()
                .filter(we -> isRequired(dataToWarm, we)).toList();
        assertThat(newWarmUpElements.stream().allMatch(we -> WarmUpElementState.State.FAILED_TEMPORARILY.equals(we.getState().state()))).isTrue();
        assertThat(newWarmUpElements.stream().allMatch(we -> we.getState().temporaryFailureCount() == 1)).isTrue();
    }

    // Retry unsuccessfully all temporary failed warmUpElements
    // You should call warmFirstTimeAndFail() before calling this method
    private void retryAndFail(WarmData dataToWarm)
            throws InterruptedException
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        long alreadyExistWarmUpElements = rowGroupData.getWarmUpElements().size();

        RowGroupData newRowGroupData = executeWarmAndFail(dataToWarm);

        assertThat(newRowGroupData.getRowGroupKey()).isEqualTo(rowGroupKey);
        assertThat(newRowGroupData.getWarmUpElements().size()).isEqualTo(alreadyExistWarmUpElements);

        for (WarmUpElement newWarmUpElement : newRowGroupData.getWarmUpElements()) {
            List<WarmUpElement> oldWarmUpElement = rowGroupData.getWarmUpElements().stream()
                    .filter(we -> we.isSameColNameAndWarmUpType(newWarmUpElement)).toList();
            assertThat(oldWarmUpElement.size()).isEqualTo(1);
            WarmUpElementState oldWarmUpElementState = oldWarmUpElement.get(0).getState();
            if (oldWarmUpElementState.state().equals(WarmUpElementState.State.FAILED_TEMPORARILY)) {
                int oldRetriesCount = oldWarmUpElementState.temporaryFailureCount();
                int newRetriesCount = newWarmUpElement.getState().temporaryFailureCount();
                if (oldRetriesCount < newRetriesCount) {
                    if (oldRetriesCount == globalConfiguration.getMaxWarmRetries()) {
                        assertThat(newWarmUpElement.getState().state()).isEqualTo(WarmUpElementState.State.FAILED_PERMANENTLY);
                    }
                    else {
                        assertThat(newWarmUpElement.getState().state()).isEqualTo(WarmUpElementState.State.FAILED_TEMPORARILY);
                        assertThat(newWarmUpElement.getState().temporaryFailureCount()).isEqualTo(oldRetriesCount + 1);
                    }
                }
            }
        }
    }

    // Call this method in order to:
    // 1. Retry successfully all temporary failed warmUpElements
    // 2. Warm successfully on the first try (in this case rowGroupData will be null)
    private void warmSuccessfully(WarmData dataToWarm)
            throws InterruptedException
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        int alreadyExistWarmUpElements = rowGroupData == null ? 0 : rowGroupData.getWarmUpElements().size();
        int alreadyValidWarmUpElements = rowGroupData == null ? 0 : rowGroupData.getValidWarmUpElements().size();
        long alreadySuccessRetryCount = varadaStatsWarmingService.getwarm_success_retry_warmup_element();
        long expectedNewWarmUpElements = countNewWarmUpElements(dataToWarm, rowGroupData);
        long expectedRetriedSuccessfully = countRetryWarmUpElements(dataToWarm, rowGroupData);

        RowGroupData newRowGroupData = executeWarmAndSuccess(dataToWarm);

        assertThat(newRowGroupData.getRowGroupKey()).isEqualTo(rowGroupKey);
        assertThat(newRowGroupData.getWarmUpElements().size()).isEqualTo(alreadyExistWarmUpElements + expectedNewWarmUpElements);
        assertThat(newRowGroupData.getValidWarmUpElements().size()).isEqualTo(alreadyValidWarmUpElements + expectedNewWarmUpElements + expectedRetriedSuccessfully);
        assertThat(varadaStatsWarmingService.getwarm_success_retry_warmup_element()).isEqualTo(alreadySuccessRetryCount + expectedRetriedSuccessfully);
    }

    private RowGroupData executeWarmAndFail(WarmData dataToWarm)
            throws InterruptedException
    {
        ConnectorPageSource failedPageSourceProvider = mock(ConnectorPageSource.class);
        when(failedPageSourceProvider.getNextPage()).thenThrow(testException);

        try {
            executeWarm(dataToWarm, failedPageSourceProvider);
        }
        catch (RuntimeException e) {
            return rowGroupDataService.get(rowGroupKey);
        }
        throw new RuntimeException("Expected to warmup fail");
    }

    private RowGroupData executeWarmAndSuccess(WarmData dataToWarm)
            throws InterruptedException
    {
        ConnectorPageSource successfulPageSourceProvider = mock(ConnectorPageSource.class);
        Page page = new Page(new IntArrayBlock(0, Optional.empty(), new int[0]));
        when(successfulPageSourceProvider.getNextPage()).thenReturn(page).thenReturn(null);
        when(successfulPageSourceProvider.isFinished()).thenReturn(false).thenReturn(true);

        executeWarm(dataToWarm, successfulPageSourceProvider);
        return rowGroupDataService.get(rowGroupKey);
    }

    private void executeWarm(WarmData dataToWarm, ConnectorPageSource pageSourceProvider)
            throws InterruptedException
    {
        ConnectorPageSourceProvider connectorPageSourceProvider = mock(ConnectorPageSourceProvider.class);
        when(connectorPageSourceProvider.createPageSource(any(), any(), any(), any(), any(), isA(DynamicFilter.class))).thenReturn(pageSourceProvider);

        List<WarmUpElement> warmupElements = recordDataList.stream()
                .flatMap(recordData -> {
                    List<Map.Entry<VaradaColumn, WarmupProperties>> list = dataToWarm.requiredWarmUpTypeMap()
                            .entries()
                            .stream()
                            .filter(entry -> entry.getKey().equals(recordData.schemaTableColumn().varadaColumn()))
                            .toList();
                    return list.stream()
                            .map(entry -> WarmUpElement.builder()
                                    .warmUpType(entry.getValue().warmUpType())
                                    .recTypeCode(recordData.recTypeCode())
                                    .recTypeLength(recordData.recTypeLength())
                                    .varadaColumn(entry.getKey())
                                    .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                                    .build());
                }).toList();

        warmingManager.warm(rowGroupKey,
                connectorPageSourceProvider,
                connectorTransactionHandle,
                connectorSession,
                dispatcherTableHandle,
                dispatcherSplit,
                dataToWarm.columnHandleList(),
                dataToWarm.requiredWarmUpTypeMap(),
                warmupElements,
                Collections.emptyMap(),
                true);
    }

    private long countNewWarmUpElements(WarmData dataToWarm, RowGroupData rowGroupData)
    {
        if (rowGroupData == null) {
            return dataToWarm.requiredWarmUpTypeMap().size();
        }

        return dataToWarm.requiredWarmUpTypeMap().entries().stream()
                .filter(entry -> rowGroupData.getWarmUpElements().stream()
                        .noneMatch(we -> entry.getKey().equals(we.getVaradaColumn()) &&
                                entry.getValue().warmUpType().equals(we.getWarmUpType())))
                .count();
    }

    private long countRetryWarmUpElements(WarmData dataToWarm, RowGroupData rowGroupData)
    {
        if (rowGroupData == null) {
            return 0;
        }
        List<WarmUpElement> warmUpElementsToRetry = getTemporaryFailedWarmUpElements(rowGroupData);
        return warmUpElementsToRetry.stream()
                .filter(we -> isRequired(dataToWarm, we))
                .count();
    }

    private List<WarmUpElement> getTemporaryFailedWarmUpElements(RowGroupData rowGroupData)
    {
        return rowGroupData.getWarmUpElements().stream()
                .filter(we -> WarmUpElementState.State.FAILED_TEMPORARILY.equals(we.getState().state()))
                .collect(Collectors.toList());
    }

    // Check if a given warmUpElement is in dataToWarm
    private boolean isRequired(WarmData dataToWarm, WarmUpElement warmUpElement)
    {
        return dataToWarm.requiredWarmUpTypeMap().entries().stream()
                .anyMatch(entry -> entry.getKey().equals(warmUpElement.getVaradaColumn()) &&
                        entry.getValue().warmUpType().equals(warmUpElement.getWarmUpType()));
    }
}
