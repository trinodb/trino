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
package io.trino.plugin.varada.dispatcher.services;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.dal.RowGroupDataDao;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmState;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StubsStorageEngine;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.NodeManager;
import io.varada.tools.util.VaradaReadWriteLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static io.trino.plugin.varada.util.NodeUtils.mockNodeManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RowGroupDataServiceTest
{
    String nodeIdentifier;
    final ArgumentCaptor<RowGroupData> rowGroupDataCapture = ArgumentCaptor.forClass(RowGroupData.class);
    private StorageEngine storageEngine;
    private RowGroupKey rowGroupKey;
    private MetricsManager metricsManager;
    private RowGroupDataService rowGroupDataService;
    private NodeManager nodeManager;

    @BeforeEach
    public void before()
    {
        storageEngine = new StubsStorageEngine();
        GlobalConfiguration globalConfiguration = new GlobalConfiguration();
        rowGroupKey = mock(RowGroupKey.class);
        RowGroupDataDao rowGroupDataDao = mock(RowGroupDataDao.class);
        when(rowGroupDataDao.get(rowGroupKey)).thenReturn(null);
        metricsManager = TestingTxService.createMetricsManager();
        nodeManager = mockNodeManager();
        nodeIdentifier = nodeManager.getCurrentNode().getNodeIdentifier();
        rowGroupDataService = spy(new RowGroupDataService(rowGroupDataDao,
                storageEngine,
                globalConfiguration,
                metricsManager,
                nodeManager,
                mock(ConnectorSync.class)));
    }

    @Test
    public void testCreateRowGroupData()
    {
        Map<VaradaColumn, String> partitionKeys = Map.of(new RegularColumn("a"), "b");

        rowGroupDataService.getOrCreateRowGroupData(rowGroupKey, partitionKeys);

        verify(rowGroupDataService, times(1)).save(rowGroupDataCapture.capture());
        RowGroupData savedRowGroupData = rowGroupDataCapture.getValue();
        assertThat(savedRowGroupData.getRowGroupKey()).isEqualTo(rowGroupKey);
        assertThat(savedRowGroupData.getPartitionKeys()).isEqualTo(partitionKeys);
        assertThat(savedRowGroupData.getWarmUpElements()).isEmpty();

        VaradaStatsWarmingService varadaStatsWarmingService = (VaradaStatsWarmingService) metricsManager.get(WARMING_SERVICE_STAT_GROUP);
        assertThat(varadaStatsWarmingService.getrow_group_count()).isEqualTo(1);
    }

    @Test
    public void testUpdateRowGroupData()
    {
        WarmUpElement existingInvalidWarmupElement = createWarmUpElement("col1", false);
        WarmUpElement existingValidWarmupElement = createWarmUpElement("col2", true);
        List<WarmUpElement> existingWarmupElements = List.of(existingInvalidWarmupElement, existingValidWarmupElement);
        WarmUpElement updatedWarmupElement = createWarmUpElement("col1", true);

        RowGroupData rowGroupData = createRowGroupData(existingWarmupElements);
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);

        rowGroupDataService.updateRowGroupData(rowGroupData, updatedWarmupElement, 10, updatedWarmupElement.getTotalRecords());
        verify(rowGroupDataService, times(1)).save(rowGroupDataCapture.capture());
        RowGroupData savedRowGroupData = rowGroupDataCapture.getValue();
        assertThat(savedRowGroupData.getRowGroupKey()).isEqualTo(rowGroupKey);
        assertThat(savedRowGroupData.getPartitionKeys()).isEqualTo(rowGroupData.getPartitionKeys());
        assertThat(savedRowGroupData.getPartitionKeys()).isEqualTo(rowGroupData.getPartitionKeys());
        assertThat(savedRowGroupData.getWarmUpElements().size()).isEqualTo(existingWarmupElements.size());
        assertThat(savedRowGroupData.getWarmUpElements()).contains(existingValidWarmupElement);
        assertThat(savedRowGroupData.getWarmUpElements()).contains(updatedWarmupElement);

        VaradaStatsWarmingService varadaStatsWarmingService = (VaradaStatsWarmingService) metricsManager.get(WARMING_SERVICE_STAT_GROUP);
        assertThat(varadaStatsWarmingService.getwarm_success_retry_warmup_element()).isEqualTo(1);
        assertThat(varadaStatsWarmingService.getrow_group_count()).isZero();
        assertThat(varadaStatsWarmingService.getwarmup_elements_count()).isEqualTo(1);
        assertThat(varadaStatsWarmingService.getdeleted_row_group_count()).isZero();
        assertThat(varadaStatsWarmingService.getdeleted_warmup_elements_count()).isZero();
    }

    @Test
    public void testMarkAsFailedNewRowGroup()
    {
        Map<VaradaColumn, String> partitionKeys = Map.of(new RegularColumn("1"), "2");
        List<WarmUpElement> newWarmUpElements = IntStream.range(0, 5)
                .mapToObj(i -> createWarmUpElement("col" + i, true))
                .collect(Collectors.toList());
        rowGroupDataService.markAsFailed(rowGroupKey, newWarmUpElements, partitionKeys);

        verify(rowGroupDataService, times(1)).save(rowGroupDataCapture.capture());
        RowGroupData rowGroupData = rowGroupDataCapture.getValue();
        assertThat(rowGroupData.getRowGroupKey()).isEqualTo(rowGroupKey);
        assertThat(rowGroupData.getPartitionKeys()).isEqualTo(partitionKeys);
        assertThat(rowGroupData.getWarmUpElements().stream().allMatch(we -> we.getState().state().equals(WarmUpElementState.State.FAILED_TEMPORARILY))).isTrue();
        assertThat(rowGroupData.getWarmUpElements().stream().allMatch(we -> we.getState().temporaryFailureCount() == 1)).isTrue();
    }

    @Test
    public void testMarkAsFailedExistingRowGroup()
    {
        List<WarmUpElement> existingWarmUpElements = IntStream.range(0, 5)
                .mapToObj(i -> createWarmUpElement("col" + i, true))
                .collect(Collectors.toList());
        RowGroupData rowGroupData = createRowGroupData(existingWarmUpElements);
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);

        List<WarmUpElement> newWarmUpElements = IntStream.range(3, 8).mapToObj(i -> createWarmUpElement("col" + i, true)).collect(Collectors.toList());
        rowGroupDataService.markAsFailed(rowGroupKey, newWarmUpElements, rowGroupData.getPartitionKeys());

        verify(rowGroupDataService, times(1)).save(rowGroupDataCapture.capture());
        RowGroupData updatedRowGroupData = rowGroupDataCapture.getValue();
        assertThat(updatedRowGroupData.getRowGroupKey()).isEqualTo(rowGroupKey);
        assertThat(updatedRowGroupData.getPartitionKeys()).isEqualTo(rowGroupData.getPartitionKeys());
        assertThat(rowGroupData.getValidWarmUpElements().stream().allMatch(x -> x.getTotalRecords() == existingWarmUpElements.get(0).getTotalRecords())).isTrue();
        // warmUpElements 3-5 were already exist and should be updated to FAILED_TEMPORARILY. warmUpElements 6-7 are new FAILED_TEMPORARILY
        assertThat(updatedRowGroupData.getWarmUpElements().stream()
                .filter(we -> we.getState().state().equals(WarmUpElementState.State.FAILED_TEMPORARILY) &&
                        we.getState().temporaryFailureCount() == 1).count())
                .isEqualTo(5);
        assertThat(updatedRowGroupData.getWarmUpElements().stream()
                .filter(we -> we.getState().state().equals(WarmUpElementState.State.VALID))
                .count())
                .isEqualTo(3);
    }

    @Test
    public void testMarkAsFailedGeneralException()
    {
        GlobalConfiguration globalConfiguration = new GlobalConfiguration();
        RowGroupDataDao rowGroupDataDao = mock(RowGroupDataDao.class);
        when(rowGroupDataDao.get(rowGroupKey)).thenReturn(null);

        RowGroupDataService rowGroupDataService = spy(new RowGroupDataService(rowGroupDataDao,
                storageEngine,
                globalConfiguration,
                metricsManager,
                nodeManager,
                mock(ConnectorSync.class)));

        WarmUpElement validWarmUpElement = createWarmUpElement("col1", true);
        WarmUpElement failedWarmUpElement = createWarmUpElement("col2", false);
        List<WarmUpElement> warmUpElements = List.of(validWarmUpElement, failedWarmUpElement);
        rowGroupDataService.markAsFailed(rowGroupKey, warmUpElements, new HashMap<>());

        ArgumentCaptor<RowGroupData> savedRowGroupDataCapture = ArgumentCaptor.forClass(RowGroupData.class);
        verify(rowGroupDataService, times(1))
                .save(savedRowGroupDataCapture.capture());
        RowGroupData savedRowGroupData = savedRowGroupDataCapture.getValue();
        assertThat(savedRowGroupData.getNodeIdentifier()).isEqualTo(nodeIdentifier);
    }

    @Test
    public void testAliasKeysSavedBySharedKey()
    {
        RowGroupDataDao rowGroupDataDao = mock(RowGroupDataDao.class);
        rowGroupDataService = new RowGroupDataService(rowGroupDataDao,
                storageEngine,
                new GlobalConfiguration(),
                metricsManager,
                nodeManager,
                mock(ConnectorSync.class));
        RowGroupKey k1 = new RowGroupKey("s1", "t1", "f", 0, 1, 100, "", "");
        RowGroupKey k2 = new RowGroupKey("s2", "t2", "f", 0, 1, 100, "", "");
        WarmUpElement warmUpElement = createWarmUpElement("c1", true);
        RowGroupData rg1 = RowGroupData.builder(createRowGroupData(List.of(warmUpElement))).rowGroupKey(k1).build();
        RowGroupData rg2 = RowGroupData.builder(createRowGroupData(List.of(warmUpElement))).rowGroupKey(k2).build();
        ArgumentCaptor<RowGroupData> captor = ArgumentCaptor.forClass(RowGroupData.class);
        rowGroupDataService.save(rg1);
        verify(rowGroupDataDao, times(1)).save(captor.capture());
        assertThat(captor.getAllValues().get(0).getRowGroupKey()).isEqualTo(k1);
        rowGroupDataService.save(rg2);
        verify(rowGroupDataDao, times(2)).save(captor.capture());
        assertThat(captor.getAllValues().get(1).getRowGroupKey()).isEqualTo(k1); // validate that both tables stored with same shared key
    }

    @Test
    void test_removeElements()
    {
        RowGroupDataDao rowGroupDataDao = mock(RowGroupDataDao.class);
        rowGroupDataService = new RowGroupDataService(rowGroupDataDao,
                storageEngine,
                new GlobalConfiguration(),
                metricsManager,
                nodeManager,
                mock(ConnectorSync.class));

        WarmUpElement warmUpElement1 = createWarmUpElement("col1", true);
        WarmUpElement warmUpElement2 = createWarmUpElement("col2", true);
        WarmUpElement warmUpElement3 = createWarmUpElement("col3", true);
        WarmUpElement warmUpElement4 = createWarmUpElement("col4", true);
        Collection<WarmUpElement> warmUpElements = ImmutableList.of(warmUpElement1, warmUpElement2, warmUpElement3, warmUpElement4);
        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(new RowGroupKey("schema", "table", "file_path", 0, 1L, 0, "", ""))
                .warmUpElements(warmUpElements)
                .nextExportOffset(1)
                .build();
        Collection<WarmUpElement> deletedWarmUpElements = ImmutableList.of(warmUpElement2, warmUpElement4);

        rowGroupDataService.removeElements(rowGroupData, deletedWarmUpElements);

        ArgumentCaptor<RowGroupData> captor = ArgumentCaptor.forClass(RowGroupData.class);
        verify(rowGroupDataDao, times(1)).save(captor.capture());

        RowGroupData outRowGroupData = captor.getAllValues().get(0);
        assertThat(outRowGroupData.isSparseFile()).isTrue();

        List<WarmUpElement> outWarmUpElements = (List<WarmUpElement>) outRowGroupData.getWarmUpElements();
        assertThat(outWarmUpElements.get(0)).isEqualTo(warmUpElement1);
        assertThat(outWarmUpElements.get(2)).isEqualTo(warmUpElement3);
        assertThat(outWarmUpElements.get(1).getWarmState()).isEqualTo(WarmState.WARM);
        assertThat(outWarmUpElements.get(3).getWarmState()).isEqualTo(WarmState.WARM);

        VaradaStatsWarmingService varadaStatsWarmingService = (VaradaStatsWarmingService) metricsManager.get(WARMING_SERVICE_STAT_GROUP);
        assertThat(varadaStatsWarmingService.getdeleted_warmup_elements_count()).isEqualTo(2);
    }

    @Test
    void test_removeElements_all()
    {
        RowGroupDataDao rowGroupDataDao = mock(RowGroupDataDao.class);
        rowGroupDataService = new RowGroupDataService(rowGroupDataDao,
                storageEngine,
                new GlobalConfiguration(),
                metricsManager,
                nodeManager,
                mock(ConnectorSync.class));
        WarmUpElement warmUpElement1 = createWarmUpElement("col1", true);
        WarmUpElement warmUpElement2 = createWarmUpElement("col2", true);
        WarmUpElement warmUpElement3 = createWarmUpElement("col3", true);
        WarmUpElement warmUpElement4 = createWarmUpElement("col4", true);
        Collection<WarmUpElement> warmUpElements = ImmutableList.of(warmUpElement1, warmUpElement2, warmUpElement3, warmUpElement4);
        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(new RowGroupKey("schema", "table", "file_path", 0, 1L, 0, "", ""))
                .warmUpElements(warmUpElements)
                .nextExportOffset(1)
                .build();

        rowGroupDataService.removeElements(rowGroupData);

        ArgumentCaptor<RowGroupData> captor = ArgumentCaptor.forClass(RowGroupData.class);
        verify(rowGroupDataDao, times(1)).save(captor.capture());

        RowGroupData outRowGroupData = captor.getAllValues().get(0);
        assertThat(outRowGroupData.isSparseFile()).isTrue();

        List<WarmUpElement> outWarmUpElements = (List<WarmUpElement>) outRowGroupData.getWarmUpElements();
        assertThat(outWarmUpElements.stream().filter(warmUpElement -> WarmState.WARM.equals(warmUpElement.getWarmState())).count())
                .isEqualTo(outWarmUpElements.size());

        VaradaStatsWarmingService varadaStatsWarmingService = (VaradaStatsWarmingService) metricsManager.get(WARMING_SERVICE_STAT_GROUP);
        assertThat(varadaStatsWarmingService.getdeleted_warmup_elements_count()).isEqualTo(outWarmUpElements.size());
    }

    private WarmUpElement createWarmUpElement(String colName, boolean isValid)
    {
        return WarmUpElement.builder()
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .totalRecords(7)
                .colName(colName)
                .state(isValid ? WarmUpElementState.VALID : WarmUpElementState.FAILED_PERMANENTLY)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();
    }

    private RowGroupData createRowGroupData(Collection<WarmUpElement> warmUpElements)
    {
        Map<VaradaColumn, String> partitionKeys = Map.of(new RegularColumn("1"), "2");
        int totalRecords = 7;
        warmUpElements = warmUpElements.stream().map(x -> WarmUpElement.builder(x).totalRecords(totalRecords).build()).collect(Collectors.toList());
        return RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .lock(new VaradaReadWriteLock())
                .warmUpElements(warmUpElements)
                .partitionKeys(partitionKeys)
                .build();
    }
}
