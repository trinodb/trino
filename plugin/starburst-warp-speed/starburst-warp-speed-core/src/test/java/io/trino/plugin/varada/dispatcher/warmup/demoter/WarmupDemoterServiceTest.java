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

import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.AtomicDouble;
import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.configuration.WarmupDemoterConfiguration;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.varada.dispatcher.warmup.WorkerWarmupRuleService;
import io.trino.plugin.varada.execution.debugtools.ColumnFilter;
import io.trino.plugin.varada.execution.debugtools.FileFilter;
import io.trino.plugin.varada.execution.debugtools.WarmupDemoterWarmupElementData;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.flows.FlowsSequencer;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.warmup.model.PartitionValueWarmupPredicateRule;
import io.trino.plugin.varada.warmup.model.WarmupPredicateRule;
import io.trino.plugin.varada.warmup.model.WarmupRule;
import io.trino.plugin.warp.gen.constants.DemoteStatus;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupDemoter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.varada.tools.CatalogNameProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService.WARMUP_DEMOTER_STAT_GROUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("SuspiciousMethodCalls")
public class WarmupDemoterServiceTest
{
    private static final String DEFAULT_FILE_PATH = "file_path";

    private final Map<RowGroupKey, RowGroupData> rowGroupDataMap = new HashMap<>();
    private final List<WarmupRule> warmupRules = new ArrayList<>();
    private final List<WarmUpElement> warmUpElements = new ArrayList<>();
    private final String defaultSchemaName = "schema1";
    private final String defaultTableName = "table1";
    private final WarmUpType defaultWarmupType = WarmUpType.WARM_UP_TYPE_BASIC;
    private final int defaultPriority = 0;
    private final double highPriority = 8.5;
    private final int notEmptyTTL = 100;
    private final int defaultEpsilon = 1;
    private final double defaultMaxThreshold = 95;
    private final double defaultCleanThreshold = 90;
    private final int defaultBatchSize = 2;
    private final long defaultMaxElementsToDemote = 100;
    private final AcquireResult failResult = new AcquireResult(-1);
    private final AcquireResult successResult = new AcquireResult(1);

    private final Set<WarmupPredicateRule> defaultPredicates = Set.of();
    private WorkerCapacityManager workerCapacityManager;
    private RowGroupDataService rowGroupDataService;
    private WarmupDemoterService warmupDemoterService;
    private WorkerWarmupRuleService workerWarmupRuleService;
    private WarmupDemoterConfiguration warmupDemoterConfiguration;
    private ConnectorSync connectorSync;
    private CatalogNameProvider catalogNameProvider;
    private List<TupleRank> failedObjects;
    private List<TupleRank> deadObjects;
    private List<TupleRank> tupleRanks;
    private MetricsManager metricsManager;

    public static WarmUpElement buildWarmupElement(int columnId, long lastUsed)
    {
        return buildWarmupElement(columnId, lastUsed, WarmUpType.WARM_UP_TYPE_BASIC, true);
    }

    public static WarmUpElement buildWarmupElement(int columnId, long lastUsed, WarmUpType warmUpType)
    {
        return buildWarmupElement(columnId, lastUsed, warmUpType, true);
    }

    public static WarmUpElement buildWarmupElement(int columnId, long lastUsed, WarmUpType warmUpType, boolean isValid)
    {
        return WarmUpElement.builder()
                .warmUpType(warmUpType)
                .state(isValid ? WarmUpElementState.VALID : WarmUpElementState.FAILED_PERMANENTLY)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .totalRecords(10)
                .colName("c" + columnId)
                .lastUsedTimestamp(lastUsed)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();
    }

    @BeforeEach
    public void before()
    {
        workerCapacityManager = mock(WorkerCapacityManager.class);
        rowGroupDataService = mock(RowGroupDataService.class);
        workerWarmupRuleService = mock(WorkerWarmupRuleService.class);
        EventBus eventBus = mock(EventBus.class);
        when(workerWarmupRuleService.fetchRulesFromCoordinator()).thenReturn(warmupRules);
        metricsManager = TestingTxService.createMetricsManager();

        FlowsSequencer flowsSequencer = spy(new FlowsSequencer(metricsManager));
        connectorSync = mock(ConnectorSync.class);
        when(connectorSync.tryAcquireAllocation()).thenReturn(successResult);
        warmupDemoterConfiguration = new WarmupDemoterConfiguration();
        warmupDemoterConfiguration.setEnableDemote(true);
        catalogNameProvider = mock(CatalogNameProvider.class);
        when(catalogNameProvider.get()).thenReturn("catalogTest");
        warmupDemoterService = spy(new WarmupDemoterService(
                workerCapacityManager,
                rowGroupDataService,
                workerWarmupRuleService,
                warmupDemoterConfiguration,
                new NativeConfiguration(),
                metricsManager,
                flowsSequencer,
                connectorSync,
                catalogNameProvider,
                eventBus));
        deadObjects = new ArrayList<>();
        failedObjects = new ArrayList<>();
        tupleRanks = new ArrayList<>();
        initDefaultMembers();
    }

    private void initDefaultMembers()
    {
        IntStream.range(0, 20).forEach(index -> {
            warmupRules.add(buildWarmupRule(defaultSchemaName, defaultTableName, index, defaultWarmupType, defaultPriority, notEmptyTTL, defaultPredicates));
            warmUpElements.add(buildWarmupElement(index, Instant.now().toEpochMilli()));
        });
        RowGroupData rowGroupData = buildRowGroupData(defaultSchemaName, defaultTableName, warmUpElements, Map.of(), 0, false);
        rowGroupDataMap.put(rowGroupData.getRowGroupKey(), rowGroupData);
    }

    @Test
    public void testTryReserve()
    {
        VaradaStatsWarmupDemoter varadaStatsWarmupDemoter = (VaradaStatsWarmupDemoter) metricsManager.get(WARMUP_DEMOTER_STAT_GROUP);
        doAnswer(invocation -> {
            Integer reservedTx = (Integer) invocation.getArguments()[0];
            varadaStatsWarmupDemoter.addreserved_tx(reservedTx);
            return reservedTx;
        }).when(workerCapacityManager).setExecutingTx(anyInt());
        List<WarmupRule> warmupRules = new ArrayList<>();
        List<WarmUpElement> warmUpElements = new ArrayList<>();
        IntStream.range(0, 100).forEach(index -> {
            warmupRules.add(buildWarmupRule(defaultSchemaName, defaultTableName, index, defaultWarmupType, defaultPriority, notEmptyTTL, defaultPredicates));
            warmUpElements.add(buildWarmupElement(index, Instant.now().toEpochMilli()));
        });
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(0.98);
        List<RowGroupData> rowGroupDataList = List.of(buildRowGroupData(defaultSchemaName,
                defaultTableName,
                warmUpElements,
                Map.of(), 0, false));
        when(rowGroupDataService.getAll()).thenReturn(rowGroupDataList);
        when(rowGroupDataService.get(eq(rowGroupDataList.get(0).getRowGroupKey()))).thenReturn(rowGroupDataList.get(0));
        when(workerWarmupRuleService.fetchRulesFromCoordinator()).thenReturn(warmupRules);
        when(connectorSync.tryAcquireAllocation()).thenReturn(failResult, failResult, failResult, successResult);
        setConfiguration(85, 80, 100, 100, List.of());
        warmupDemoterService.connectorSyncStartDemote(warmupDemoterService.getCurrentRunSequence());
        warmupDemoterService.connectorSyncStartDemoteCycle(10, true);
        warmupDemoterService.connectorSyncDemoteEnd(warmupDemoterService.getCurrentRunSequence(), warmupDemoterService.getDemoterHighestPriority().get());
        verify(connectorSync, times(1))
                .syncDemoteCycleEnd(anyInt(), anyDouble(), anyDouble(), eq(DemoteStatus.DEMOTE_STATUS_NOT_COMPLETED));

        assertThat(varadaStatsWarmupDemoter.getnumber_fail_acquire()).isEqualTo(3);
        assertThat(varadaStatsWarmupDemoter.getreserved_tx()).isEqualTo(0);
    }

    @Test
    public void testTryReserveFailure()
    {
        List<WarmupRule> warmupRules = new ArrayList<>();
        List<WarmUpElement> warmUpElements = new ArrayList<>();
        IntStream.range(0, 100).forEach(index -> {
            warmupRules.add(buildWarmupRule(defaultSchemaName, defaultTableName, index, defaultWarmupType, defaultPriority, notEmptyTTL, defaultPredicates));
            warmUpElements.add(buildWarmupElement(index, Instant.now().toEpochMilli()));
        });
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(0.98);
        warmupDemoterConfiguration.setMaxRetriesAcquireThread(20);

        RowGroupData rowGroupData = buildRowGroupData(defaultSchemaName,
                defaultTableName,
                warmUpElements,
                Map.of(), 0, false);
        when(rowGroupDataService.getAll()).thenReturn(List.of(rowGroupData));
        when(rowGroupDataService.get(eq(rowGroupData.getRowGroupKey()))).thenReturn(rowGroupData);
        when(workerWarmupRuleService.fetchRulesFromCoordinator()).thenReturn(warmupRules);

        when(connectorSync.tryAcquireAllocation()).thenReturn(failResult);
        setConfiguration(85, 80, 100, 100, List.of());
        warmupDemoterService.connectorSyncStartDemote(warmupDemoterService.getCurrentRunSequence());
        warmupDemoterService.connectorSyncStartDemoteCycle(10, true);
        warmupDemoterService.connectorSyncDemoteEnd(warmupDemoterService.getCurrentRunSequence(), warmupDemoterService.getDemoterHighestPriority().get());
        verify(connectorSync, times(1))
                .syncDemoteCycleEnd(anyInt(), anyDouble(), anyDouble(), eq(DemoteStatus.DEMOTE_STATUS_REACHED_THRESHOLD));
        VaradaStatsWarmupDemoter varadaStatsWarmupDemoter = (VaradaStatsWarmupDemoter) metricsManager.get(WARMUP_DEMOTER_STAT_GROUP);
        assertThat(varadaStatsWarmupDemoter.getnumber_fail_acquire()).isEqualTo(21);
        assertThat(varadaStatsWarmupDemoter.getnumber_of_runs_fail()).isEqualTo(1);
        assertThat(varadaStatsWarmupDemoter.getreserved_tx()).isEqualTo(0);
    }

    @Test
    public void testDeleteNativeFailure()
    {
        List<WarmupRule> warmupRuleList = new ArrayList<>();
        List<WarmUpElement> warmUpElementList = new ArrayList<>();
        IntStream.range(0, 4).forEach(index -> {
            warmupRuleList.add(buildWarmupRule(defaultSchemaName, "aaa", index, defaultWarmupType, defaultPriority, notEmptyTTL, defaultPredicates));
            warmupRuleList.add(buildWarmupRule(defaultSchemaName, "bbb", index, defaultWarmupType, defaultPriority, notEmptyTTL, defaultPredicates));
            warmUpElementList.add(buildWarmupElement(index, Instant.now().toEpochMilli()));
        });
        RowGroupData rowGroupData1 = buildRowGroupData(defaultSchemaName, "aaa", warmUpElementList, Map.of(), 1, false);
        RowGroupData rowGroupData2 = buildRowGroupData(defaultSchemaName, "bbb", warmUpElementList, Map.of(), 2, false);
        Map<RowGroupKey, RowGroupData> rowGroupDataMapTest = new HashMap<>();
        rowGroupDataMapTest.put(rowGroupData1.getRowGroupKey(), rowGroupData1);
        rowGroupDataMapTest.put(rowGroupData2.getRowGroupKey(), rowGroupData2);

        when(workerWarmupRuleService.fetchRulesFromCoordinator()).thenReturn(warmupRuleList);
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(10d);
        when(rowGroupDataService.getAll()).thenReturn(new ArrayList<>(rowGroupDataMapTest.values()));
        when(rowGroupDataService.get(any())).thenAnswer(i -> rowGroupDataMapTest.get(i.getArguments()[0]));
        setConfiguration(0, 0, 1, 1, List.of());
        doThrow(new TrinoException(VaradaErrorCode.VARADA_NATIVE_ERROR, "test"))
                .doNothing()
                .when(rowGroupDataService).removeElements(eq(rowGroupData1), any(Collection.class));

        warmupDemoterService.connectorSyncStartDemote(warmupDemoterService.getCurrentRunSequence());
        warmupDemoterService.connectorSyncStartDemoteCycle(10, true);
        verify(connectorSync, times(1))
                .syncDemoteCycleEnd(anyInt(), anyDouble(), anyDouble(), eq(DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE));
        assertThat(warmupDemoterService.getCurrentRunStats().getfailed_row_group_data()).isEqualTo(1);
        assertThat(warmupDemoterService.getCurrentRunStats().getdeleted_by_low_priority()).isEqualTo(4);
        verify(rowGroupDataService, times(1)).removeElements(eq(rowGroupData1), any(Collection.class));
        verify(rowGroupDataService, times(1)).removeElements(eq(rowGroupData1));
    }

    @Test
    public void testRunWarmupDemoterWithStatusExecuting()
    {
        assertThat(warmupDemoterService.trySetIsExecutingToTrue()).isTrue();
        warmupDemoterService.tryDemoteStart();
        VaradaStatsWarmupDemoter varadaStatsWarmupDemoter = (VaradaStatsWarmupDemoter) metricsManager.get(WARMUP_DEMOTER_STAT_GROUP);
        assertThat(varadaStatsWarmupDemoter.getnumber_of_runs()).isEqualTo(0);
        assertThat(varadaStatsWarmupDemoter.getnot_executed_due_is_already_executing()).isEqualTo(1);
    }

    @Test
    public void testRunFail()
    {
        VaradaStatsWarmupDemoter varadaStatsWarmupDemoter = (VaradaStatsWarmupDemoter) metricsManager.get(WARMUP_DEMOTER_STAT_GROUP);
        varadaStatsWarmupDemoter.setcurrentUsage(1000);
        setConfiguration(defaultMaxThreshold, defaultCleanThreshold, 0, 1, List.of());
        assertThat(varadaStatsWarmupDemoter.getnumber_of_runs()).isEqualTo(0);
        assertThat(varadaStatsWarmupDemoter.getcurrentUsage()).isEqualTo(1000);
        assertThat(varadaStatsWarmupDemoter.getnumber_of_runs_fail()).isEqualTo(0);
        warmupDemoterService.tryDemoteStart();
        assertThat(varadaStatsWarmupDemoter.getnumber_of_runs_fail()).isEqualTo(1);
        assertThat(varadaStatsWarmupDemoter.getnumber_of_runs_fail()).isEqualTo(1);
        assertThat(varadaStatsWarmupDemoter.getnumber_of_runs()).isEqualTo(0);
    }

    @Test
    public void testDeadObject()
    {
        String schemaName = "newSchema";
        List<WarmUpElement> elements = new ArrayList<>();
        long lastUsed = Instant.now().minus(2, ChronoUnit.SECONDS).toEpochMilli();
        IntStream.range(0, 20).forEach(index -> {
            warmupRules.add(buildWarmupRule(schemaName, defaultTableName, index, defaultWarmupType, defaultPriority, 1, defaultPredicates));
            elements.add(buildWarmupElement(index, lastUsed));
        });
        RowGroupData rowGroupData = buildRowGroupData(schemaName,
                defaultTableName,
                elements,
                Map.of(), 0, false);
        when(rowGroupDataService.getAll()).thenReturn(List.of(rowGroupData));
        when(rowGroupDataService.get(eq(rowGroupData.getRowGroupKey()))).thenReturn(rowGroupData);
        when(workerWarmupRuleService.fetchRulesFromCoordinator()).thenReturn(warmupRules);
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(0.98);

        setConfiguration(defaultMaxThreshold, defaultCleanThreshold, defaultBatchSize, defaultMaxElementsToDemote, List.of());
        warmupDemoterService.tryDemoteStart();
        warmupDemoterService.connectorSyncStartDemote(warmupDemoterService.getCurrentRunSequence());
        assertThat(warmupDemoterService.getCurrentRunStats().getdead_objects_deleted()).isEqualTo(elements.size());
        verify(connectorSync, times(1))
                .syncDemoteCycleEnd(anyInt(), anyDouble(), anyDouble(), eq(DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE));
    }

    @Test
    public void testFailedObjects()
    {
        List<WarmUpElement> elements = IntStream.range(0, 20)
                .mapToObj(index -> buildWarmupElement(index, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_DATA, false))
                .collect(Collectors.toList());

        RowGroupData rowGroupData = buildRowGroupData(defaultSchemaName,
                defaultTableName,
                elements,
                Map.of(), 0, false);
        when(rowGroupDataService.getAll()).thenReturn(List.of(rowGroupData));
        when(rowGroupDataService.get(eq(rowGroupData.getRowGroupKey()))).thenReturn(rowGroupData);

        warmupDemoterService.setForceDeleteFailedObjects(true);
        warmupDemoterService.tryDemoteStart();
        warmupDemoterService.connectorSyncStartDemote(warmupDemoterService.getCurrentRunSequence());

        assertThat(warmupDemoterService.getCurrentRunStats().getfailed_objects_deleted()).isEqualTo(elements.size());
        verify(connectorSync, times(1))
                .syncDemoteCycleEnd(anyInt(), anyDouble(), anyDouble(), eq(DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE));
    }

    @Test
    public void testDemoteAll()
    {
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(10d);
        when(rowGroupDataService.getAll()).thenReturn(new ArrayList<>(rowGroupDataMap.values()));
        when(rowGroupDataService.get(any())).thenAnswer(i -> rowGroupDataMap.get(i.getArguments()[0]));
        setConfiguration(0, 0, 1, 100, List.of());
        warmupDemoterService.connectorSyncStartDemote(warmupDemoterService.getCurrentRunSequence());
        warmupDemoterService.connectorSyncStartDemoteCycle(10, true);
        assertThat(warmupDemoterService.getDemoterHighestPriority().get()).isEqualTo(0);
        assertThat(warmupDemoterService.getCurrentRunStats().getdeleted_by_low_priority()).isGreaterThan(1);
        verify(connectorSync, times(1))
                .syncDemoteCycleEnd(eq(warmupDemoterService.getCurrentRunSequence()), eq(0D), eq(0D), eq(DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE));
    }

    @Test
    public void testWarmupDemoterDecreaseMemoryUsageUntilCleanThreshold()
    {
        AtomicDouble usageCapacity = new AtomicDouble(0.92D);
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenAnswer(i -> {
            usageCapacity.set(usageCapacity.addAndGet(-0.1));
            return usageCapacity.get();
        });

        List<WarmupRule> warmupRules = new ArrayList<>();
        List<WarmUpElement> warmUpElements = new ArrayList<>();
        IntStream.range(0, 100).forEach(index -> {
            warmupRules.add(buildWarmupRule(defaultSchemaName, defaultTableName, index, defaultWarmupType, defaultPriority, notEmptyTTL, defaultPredicates));
            warmUpElements.add(buildWarmupElement(index, Instant.now().toEpochMilli()));
        });
        RowGroupData rowGroupData = buildRowGroupData(defaultSchemaName,
                defaultTableName,
                warmUpElements,
                Map.of(),
                0, false);
        RowGroupData rowGroupData2 = buildRowGroupData(defaultSchemaName,
                defaultTableName,
                warmUpElements,
                Map.of(),
                1, false);
        RowGroupData rowGroupData3 = buildRowGroupData(defaultSchemaName,
                defaultTableName,
                warmUpElements,
                Map.of(),
                2, false);
        when(rowGroupDataService.getAll()).thenReturn(List.of(rowGroupData, rowGroupData2, rowGroupData3));
        when(rowGroupDataService.get(eq(rowGroupData.getRowGroupKey()))).thenReturn(rowGroupData);
        when(rowGroupDataService.get(eq(rowGroupData2.getRowGroupKey()))).thenReturn(rowGroupData2);
        when(rowGroupDataService.get(eq(rowGroupData3.getRowGroupKey()))).thenReturn(rowGroupData3);
        when(workerWarmupRuleService.fetchRulesFromCoordinator()).thenReturn(warmupRules);

        setConfiguration(85, 80, 2, 100, List.of());
        warmupDemoterService.connectorSyncStartDemote(warmupDemoterService.getCurrentRunSequence());
        warmupDemoterService.connectorSyncStartDemoteCycle(10, true);
        verify(connectorSync, times(1))
                .syncDemoteCycleEnd(anyInt(), anyDouble(), anyDouble(), eq(DemoteStatus.DEMOTE_STATUS_NOT_COMPLETED));
        verify(connectorSync, times(1))
                .syncDemoteCycleEnd(anyInt(), anyDouble(), anyDouble(), eq(DemoteStatus.DEMOTE_STATUS_REACHED_THRESHOLD));
        assertThat(usageCapacity.get()).isLessThan(0.88);
        assertThat(usageCapacity.get()).isGreaterThan(0.5);
        assertThat(warmupDemoterService.getCurrentRunStats().getdeleted_by_low_priority()).isEqualTo(0);
        assertThat(warmupDemoterService.getCurrentRunStats().getdead_objects_deleted()).isEqualTo(0);
    }

    @Test
    public void testBuildTupleRank()
    {
        List<WarmupRule> warmupRules = new ArrayList<>();
        List<WarmUpElement> warmUpElements = new ArrayList<>();
        IntStream.range(0, 10).forEach(index -> {
            warmupRules.add(buildWarmupRule(defaultSchemaName, defaultTableName, index, defaultWarmupType, 10 - index, index, defaultPredicates));
            warmUpElements.add(buildWarmupElement(index, Instant.now().toEpochMilli()));
        });
        warmupRules.add(buildWarmupRule(defaultSchemaName, defaultTableName, 10, defaultWarmupType, defaultPriority, notEmptyTTL, defaultPredicates));
        warmUpElements.add(buildWarmupElement(10, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_BASIC, false));

        warmupDemoterService.setForceDeleteFailedObjects(true);
        RowGroupData rowGroupData = buildRowGroupData(defaultSchemaName,
                defaultTableName,
                warmUpElements,
                Map.of(), 0, false);
        warmupDemoterService.buildTupleRank(warmupRules, List.of(rowGroupData), deadObjects, failedObjects, tupleRanks, List.of());
        warmupDemoterService.sortTupleRankCollection(tupleRanks);
        assertThat(tupleRanks.size()).isEqualTo(9);
        assertThat(tupleRanks.get(0).warmupProperties().priority()).isLessThan(tupleRanks.get(5).warmupProperties().priority());
    }

    @Test
    public void testTwoWarmupsOnSameColumn()
    {
        WarmupProperties propBasic = new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 1, 0, TransformFunction.NONE);
        WarmupProperties propData = new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, 10, 1000, TransformFunction.NONE);

        List<WarmupRule> warmupRules = buildWarmupRule(
                Set.of(propBasic, propData),
                defaultPredicates);

        List<WarmUpElement> warmUpElements = List.of(
                buildWarmupElement(0, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_BASIC),
                buildWarmupElement(0, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_DATA));

        List<RowGroupData> rowGroupDataList = List.of(
                buildRowGroupData(defaultSchemaName,
                        defaultTableName,
                        warmUpElements,
                        Map.of(), 0, false));

        warmupDemoterService.buildTupleRank(warmupRules, rowGroupDataList, deadObjects, failedObjects, tupleRanks, List.of());

        assertThat(tupleRanks.size()).isEqualTo(1);
        assertThat(deadObjects.size()).isEqualTo(1);
    }

    @Test
    public void testTwoWarmupsOnSameRowGroup()
    {
        List<WarmupRule> warmupRules = List.of(
                buildWarmupRule(
                        defaultSchemaName,
                        defaultTableName,
                        0,
                        WarmUpType.WARM_UP_TYPE_BASIC,
                        1,
                        0,
                        defaultPredicates),
                buildWarmupRule(
                        defaultSchemaName,
                        defaultTableName,
                        1,
                        WarmUpType.WARM_UP_TYPE_DATA,
                        10,
                        1000,
                        defaultPredicates));

        List<WarmUpElement> warmUpElements = List.of(
                buildWarmupElement(0, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_BASIC),
                buildWarmupElement(1, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_DATA));

        RowGroupData rowGroupData = buildRowGroupData(
                defaultSchemaName,
                defaultTableName,
                warmUpElements,
                Map.of(), 0, false);

        warmupDemoterService.buildTupleRank(
                warmupRules,
                List.of(rowGroupData),
                deadObjects,
                failedObjects,
                tupleRanks,
                List.of(new ColumnFilter(
                        new SchemaTableName(rowGroupData.getRowGroupKey().schema(),
                                rowGroupData.getRowGroupKey().table()),
                        List.of(
                                new WarmupDemoterWarmupElementData(
                                        warmUpElements.get(1).getVaradaColumn().getName(),
                                        List.of(warmUpElements.get(1).getWarmUpType()))))));

        assertThat(deadObjects.size()).isEqualTo(1);
    }

    @Test
    public void testMatchPredicates()
    {
        VaradaColumn partitionKey = new RegularColumn("p1");
        String partitionValue = "v1";
        String partitionValue2 = "v2";
        int priority2 = 5;
        String schemaName2 = "schema2";

        int ttlInTheFuture = 1;
        int weId = 1;

        Map<VaradaColumn, String> hivePartitionKeys = Map.of(partitionKey, partitionValue);
        Map<VaradaColumn, String> hivePartitionKeys2 = Map.of(partitionKey, partitionValue2);
        Set<WarmupPredicateRule> predicates = Set.of(new PartitionValueWarmupPredicateRule(partitionKey.getName(), partitionValue));
        Set<WarmupPredicateRule> predicates2 = Set.of(new PartitionValueWarmupPredicateRule(partitionKey.getName(), partitionValue2));
        List<WarmupRule> warmupRules = List.of(buildWarmupRule(defaultSchemaName, defaultTableName, weId, defaultWarmupType, defaultPriority, ttlInTheFuture, predicates),
                buildWarmupRule(schemaName2, defaultTableName, weId, defaultWarmupType, priority2, ttlInTheFuture, predicates2));
        List<WarmUpElement> warmUpElements = List.of(buildWarmupElement(weId, Instant.now().toEpochMilli()));
        List<RowGroupData> rowGroupDataList = List.of(buildRowGroupData(defaultSchemaName, defaultTableName, warmUpElements, hivePartitionKeys, 0, false),
                buildRowGroupData(schemaName2, defaultTableName, warmUpElements, hivePartitionKeys2, 1, false));
        warmupDemoterService.buildTupleRank(warmupRules, rowGroupDataList, deadObjects, failedObjects, tupleRanks, List.of());

        warmupDemoterService.sortTupleRankCollection(tupleRanks);
        assertThat(tupleRanks).isNotEmpty();
    }

    @Test
    public void testBestMatchOverlappingPredicates()
    {
        VaradaColumn partitionKey = new RegularColumn("p1");
        String partitionValue = "v1";
        int priorityHigh = 10;
        int ttlInTheFuture = 1;
        int weId = 1;

        Map<VaradaColumn, String> hivePartitionKeys = Map.of(partitionKey, partitionValue);
        Set<WarmupPredicateRule> predicates = Set.of(new PartitionValueWarmupPredicateRule(partitionKey.getName(), partitionValue));
        List<WarmupRule> warmupRules = List.of(
                buildWarmupRule(defaultSchemaName, defaultTableName, weId, defaultWarmupType, defaultPriority, ttlInTheFuture, predicates),
                buildWarmupRule(defaultSchemaName, defaultTableName, weId, defaultWarmupType, priorityHigh, ttlInTheFuture, Set.of()));
        List<WarmUpElement> warmUpElements = List.of(buildWarmupElement(weId, Instant.now().toEpochMilli() + 10000));
        List<RowGroupData> rowGroupDataList = List.of(buildRowGroupData(defaultSchemaName, defaultTableName, warmUpElements, hivePartitionKeys, 0, false));
        warmupDemoterService.buildTupleRank(warmupRules, rowGroupDataList, deadObjects, failedObjects, tupleRanks, List.of());

        assertThat(tupleRanks).isNotEmpty();
    }

    @Test
    public void testBestMatchPredicatesNotMatch()
    {
        VaradaColumn partitionKey = new RegularColumn("p1");
        String partitionValue = "v1";
        String partitionKey2 = "p2";
        String partitionValue2 = "v2";
        int priorityHigh = 10;
        int ttlInTheFuture = 1;
        int weId = 1;

        Map<VaradaColumn, String> hivePartitionKeys = Map.of(partitionKey, partitionValue);

        Set<WarmupPredicateRule> predicates1 = Set.of(new PartitionValueWarmupPredicateRule(partitionKey.getName(), partitionValue));
        Set<WarmupPredicateRule> predicates2 = new HashSet<>(List.of(new PartitionValueWarmupPredicateRule(partitionKey.getName(), partitionValue), new PartitionValueWarmupPredicateRule(partitionKey2, partitionValue2)));
        List<WarmupRule> warmupRules = List.of(buildWarmupRule(defaultSchemaName, defaultTableName, weId, defaultWarmupType, defaultPriority, ttlInTheFuture, predicates1),
                buildWarmupRule(defaultSchemaName, defaultTableName, weId, defaultWarmupType, priorityHigh, ttlInTheFuture, predicates2));
        List<WarmUpElement> warmUpElements = List.of(buildWarmupElement(weId, Instant.now().toEpochMilli()));
        List<RowGroupData> rowGroupDataList = List.of(buildRowGroupData(defaultSchemaName, defaultTableName, warmUpElements, hivePartitionKeys, 0, false));
        warmupDemoterService.buildTupleRank(warmupRules, rowGroupDataList, deadObjects, failedObjects, tupleRanks, List.of());

        assertThat(tupleRanks).isNotEmpty();
    }

    @Test
    public void testForceDeleteFailedObjectsWithColumnFilter()
    {
        ColumnFilter columnFilterMatch = createColumnFilter(List.of(WarmUpType.WARM_UP_TYPE_BASIC));
        List<WarmupRule> warmupRules = ImmutableList.of(buildWarmupRule(defaultSchemaName, defaultTableName, 0, WarmUpType.WARM_UP_TYPE_BASIC, defaultPriority, notEmptyTTL, defaultPredicates),
                buildWarmupRule(defaultSchemaName, defaultTableName, 0, WarmUpType.WARM_UP_TYPE_DATA, defaultPriority, notEmptyTTL, defaultPredicates));
        WarmUpElement warmUpElementToDelete = buildWarmupElement(0, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_BASIC, false);
        WarmUpElement warmUpElementNotToDelete = buildWarmupElement(0, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_DATA, false);
        List<WarmUpElement> warmUpElements = ImmutableList.of(warmUpElementToDelete, warmUpElementNotToDelete);

        warmupDemoterService.setForceDeleteFailedObjects(true);
        List<RowGroupData> rowGroupDataList = List.of(buildRowGroupData(defaultSchemaName, defaultTableName, warmUpElements, Map.of(), 0, false));
        warmupDemoterService.buildTupleRank(warmupRules, rowGroupDataList, deadObjects, failedObjects, tupleRanks, ImmutableList.of(columnFilterMatch));
        assertThat(deadObjects.size()).isEqualTo(0);
        assertThat(failedObjects.size()).isEqualTo(1);
        assertThat(tupleRanks.size()).isEqualTo(0);
    }

    @Test
    public void testRetryDeleteRejectedObjects()
            throws ExecutionException, InterruptedException
    {
        String schemaName = "newSchema";
        long lastUsed = Instant.now().minus(2, ChronoUnit.SECONDS).toEpochMilli();
        List<RowGroupData> rowGroupDataList = new ArrayList<>();
        warmupRules.clear();
        IntStream.range(0, 20).forEach(index -> {
            String tableName = "tableName" + index;
            warmupRules.add(buildWarmupRule(schemaName, tableName, index, defaultWarmupType, defaultPriority, 0, defaultPredicates));
            List<WarmUpElement> elements = List.of(buildWarmupElement(index, lastUsed));
            rowGroupDataList.add(buildRowGroupData(schemaName, tableName, elements, Map.of(), index, false));
        });

        warmupDemoterConfiguration.setTasksExecutorQueueSize(5);
        warmupDemoterConfiguration.setEnableDemote(true);
        NativeConfiguration nativeConfiguration = new NativeConfiguration();
        nativeConfiguration.setTaskMaxWorkerThreads(1);
        warmupDemoterService = new WarmupDemoterService(workerCapacityManager,
                rowGroupDataService,
                workerWarmupRuleService,
                warmupDemoterConfiguration,
                nativeConfiguration,
                metricsManager,
                mock(FlowsSequencer.class),
                connectorSync,
                catalogNameProvider,
                mock(EventBus.class));

        warmupDemoterService.initDemoteArguments(1);
        warmupDemoterService.buildTupleRank(warmupRules, rowGroupDataList, deadObjects, failedObjects, tupleRanks, List.of());
        Map<RowGroupKey, RowGroupData> map = rowGroupDataList.stream().collect(Collectors.toMap(RowGroupData::getRowGroupKey, Function.identity()));
        doAnswer(invocation -> {
            RowGroupKey rowGroupKey = (RowGroupKey) invocation.getArguments()[0];
            return map.get(rowGroupKey);
        }).when(rowGroupDataService).get(any());

        long deleted = warmupDemoterService.delete(deadObjects);
        assertThat(deleted).isEqualTo(20);
    }

    @Test
    public void testDemoteAllEmptyRowGroup()
    {
        setConfiguration(0, 0, 100, 100, List.of());
        List<WarmUpElement> elements = IntStream.range(0, 20)
                .mapToObj(index -> buildWarmupElement(index, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_DATA, false))
                .collect(Collectors.toList());

        List<RowGroupData> rowGroupDataList = List.of(buildRowGroupData(defaultSchemaName, defaultTableName, elements, Map.of(), 0, true));
        when(rowGroupDataService.getAll()).thenReturn(rowGroupDataList);
        when(rowGroupDataService.get(eq(rowGroupDataList.get(0).getRowGroupKey()))).thenReturn(rowGroupDataList.get(0));
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(10d);

        warmupDemoterService.tryDemoteStart();
        warmupDemoterService.connectorSyncStartDemote(warmupDemoterService.getCurrentRunSequence());
        warmupDemoterService.connectorSyncStartDemoteCycle(10, true);
        verify(rowGroupDataService, times(1)).deleteData(eq(rowGroupDataList.get(0)), eq(true));

        verify(connectorSync, times(1))
                .syncDemoteCycleEnd(anyInt(), anyDouble(), anyDouble(), eq(DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE));
    }

    @Test
    public void testDemotePartialEmptyRowGroup()
    {
        ColumnFilter columnFilterNonMatch = createColumnFilter(List.of(WarmUpType.WARM_UP_TYPE_DATA));
        setConfiguration(0, 0, 100, 100, List.of(columnFilterNonMatch));
        List<WarmUpElement> elements = IntStream.range(0, 20)
                .mapToObj(index -> buildWarmupElement(index, Instant.now().toEpochMilli(), WarmUpType.WARM_UP_TYPE_DATA, false))
                .collect(Collectors.toList());

        RowGroupData rowGroupData = buildRowGroupData(defaultSchemaName, defaultTableName, elements, Map.of(), 0, true);
        when(rowGroupDataService.getAll()).thenReturn(List.of(rowGroupData));
        when(rowGroupDataService.get(eq(rowGroupData.getRowGroupKey()))).thenReturn(rowGroupData);
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(10d);

        warmupDemoterService.setDeleteEmptyRowGroups(true);
        warmupDemoterService.tryDemoteStart();
        warmupDemoterService.connectorSyncStartDemote(warmupDemoterService.getCurrentRunSequence());

        //first delete old objects due to columnFilterNonMatch
        verify(connectorSync, times(1))
                .syncDemoteCycleEnd(anyInt(), anyDouble(), anyDouble(), eq(DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE));

        warmupDemoterService.connectorSyncStartDemoteCycle(10, true);
        verify(rowGroupDataService, times(1))
                .updateEmptyRowGroup(eq(rowGroupData),
                        eq(List.of()),
                        eq(List.of(elements.get(0))));

        verify(connectorSync, times(2))
                .syncDemoteCycleEnd(anyInt(), anyDouble(), anyDouble(), eq(DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE));
    }

    @Test
    public void testWarmupDemoterFilterByColumn()
    {
        ColumnFilter columnFilter = createColumnFilter(List.of());
        executeFilterTest(List.of(columnFilter), 1);
        assertThat(warmupDemoterService.getDemoterHighestPriority().get()).isEqualTo(0);
    }

    @Test
    public void testWarmupDemoterFilterByWarmupElement()
    {
        ColumnFilter columnFilterMatch = createColumnFilter(ImmutableList.of(WarmUpType.WARM_UP_TYPE_BASIC));

        executeFilterTest(List.of(columnFilterMatch), 1);
        assertThat(warmupDemoterService.getDemoterHighestPriority().get()).isLessThan(highPriority);
    }

    @Test
    public void testWarmupDemoterFilterByWarmupElementNonMatch()
    {
        ColumnFilter columnFilterNonMatch = createColumnFilter(ImmutableList.of(WarmUpType.WARM_UP_TYPE_DATA));
        executeFilterTest(List.of(columnFilterNonMatch), 0);
    }

    @Test
    public void testWarmupDemoterFilterByFilePaths()
    {
        FileFilter fileFilter = new FileFilter(Set.of(DEFAULT_FILE_PATH + "_" + 0));
        executeFilterTest(List.of(fileFilter), 20);
    }

    @Test
    public void testWarmupDemoterFilterByFilePathsNonMatch()
    {
        FileFilter fileFilterNonMatch = new FileFilter(Set.of("non existing file path"));
        executeFilterTest(List.of(fileFilterNonMatch), 0);
    }

    @Test
    public void testWarmupDemoterTwoFilters()
    {
        ColumnFilter columnFilterMatch = createColumnFilter(List.of(WarmUpType.WARM_UP_TYPE_BASIC));
        FileFilter fileFilter = new FileFilter(Set.of(DEFAULT_FILE_PATH + "_" + 0));
        executeFilterTest(List.of(columnFilterMatch, fileFilter), 1);
    }

    @Test
    public void testInitiateSyncDemoteProcessRequestRejected()
    {
        when(connectorSync.syncDemotePrepare(defaultEpsilon)).thenReturn(-1);
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(0.98);
        setConfiguration(defaultMaxThreshold, defaultCleanThreshold, defaultBatchSize, defaultMaxElementsToDemote, List.of());
        warmupDemoterService.initiateDemoteProcess();
        VaradaStatsWarmupDemoter varadaStatsWarmupDemoter = (VaradaStatsWarmupDemoter) metricsManager.get(WARMUP_DEMOTER_STAT_GROUP);
        assertThat(varadaStatsWarmupDemoter.getnot_executed_due_sync_demote_start_rejected()).isEqualTo(1);
    }

    @Test
    public void testInitiateSyncDemoteProcessArgsNotValid()
    {
        when(connectorSync.syncDemotePrepare(defaultEpsilon)).thenReturn(1);
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(0.98);
        setConfiguration(defaultMaxThreshold, defaultCleanThreshold, defaultBatchSize, defaultMaxElementsToDemote, List.of());
        int demoteProcess = warmupDemoterService.initiateDemoteProcess();
        warmupDemoterService.initDemoteArguments(demoteProcess);
        warmupDemoterService.initiateDemoteProcess();
        VaradaStatsWarmupDemoter varadaStatsWarmupDemoter = (VaradaStatsWarmupDemoter) metricsManager.get(WARMUP_DEMOTER_STAT_GROUP);
        assertThat(varadaStatsWarmupDemoter.getnot_executed_due_is_already_executing()).isEqualTo(1);
    }

    @Test
    public void testInitiateSyncDemoteProcess()
    {
        when(connectorSync.syncDemotePrepare(defaultEpsilon)).thenReturn(1);
        setConfiguration(defaultMaxThreshold, defaultCleanThreshold, defaultBatchSize, defaultMaxElementsToDemote, List.of());
        warmupDemoterService.initiateDemoteProcess();
    }

    @Test
    public void testTryAllocateTx()
    {
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(0.99);
        when(connectorSync.tryAcquireAllocation()).thenReturn(new AcquireResult(1));
        AcquireWarmupStatus tryAllocateTxWithHighUsage = warmupDemoterService.tryAllocateNativeResourceForWarmup();
        assertThat(tryAllocateTxWithHighUsage).isEqualTo(AcquireWarmupStatus.REACHED_THRESHOLD);
        verify(workerCapacityManager, times(1)).setExecutingTx(eq(1));
        verify(workerCapacityManager, times(1)).decreaseExecutingTx();
        when(workerCapacityManager.getFractionCurrentUsageFromTotal()).thenReturn(0.5);
        AcquireWarmupStatus tryAllocateTxWithLowUsage = warmupDemoterService.tryAllocateNativeResourceForWarmup();
        assertThat(tryAllocateTxWithLowUsage).isEqualTo(AcquireWarmupStatus.SUCCESS);
        verify(workerCapacityManager, times(2)).setExecutingTx(eq(1));
        verify(workerCapacityManager, times(1)).decreaseExecutingTx();
    }

    @Test
    public void testTupleRankSort()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "/", 0, 0, 0, "", "");
        WarmupProperties warmupProperties1 = new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 3, 80, TransformFunction.NONE);
        TupleRank t1 = new TupleRank(warmupProperties1, null, rowGroupKey);
        WarmupProperties warmupProperties2 = new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 1, 80, TransformFunction.NONE);
        TupleRank t2 = new TupleRank(warmupProperties2, null, rowGroupKey);
        WarmupProperties warmupProperties3 = new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 2, 80, TransformFunction.NONE);
        TupleRank t3 = new TupleRank(warmupProperties3, null, rowGroupKey);
        List<TupleRank> list = new ArrayList<>(List.of(t1, t2, t3));
        Collections.sort(list);
        assertThat(list.get(0)).isEqualTo(t2);
        assertThat(list.get(1)).isEqualTo(t3);
        assertThat(list.get(2)).isEqualTo(t1);
    }

    @Test
    public void testTupleRanksToDemote()
            throws ExecutionException, InterruptedException
    {
        when(rowGroupDataService.get(any())).thenAnswer(i -> rowGroupDataMap.get(i.getArguments()[0]));

        List<TupleRank> tupleRanksToDemote = rowGroupDataMap.keySet()
                .stream()
                .limit(rowGroupDataMap.keySet().size() / 2)
                .map(rowGroupKey -> {
                    WarmupProperties warmupProperties = new WarmupProperties(WarmUpType.WARM_UP_TYPE_BASIC, 1, 80, TransformFunction.NONE);
                    return new TupleRank(warmupProperties, null, rowGroupKey);
                }).collect(Collectors.toList());

        setConfiguration(0, 0, 1, 100, List.of());
        warmupDemoterService.connectorSyncStartDemote(warmupDemoterService.getCurrentRunSequence());
        assertThat(warmupDemoterService.delete(tupleRanksToDemote)).isEqualTo(tupleRanksToDemote.size());
    }

    private void setConfiguration(double maxUsageThresholdPercentage,
            double cleanupUsageThresholdPercentage,
            int batchSize,
            long maxElementsToDemote,
            List<TupleFilter> tupleFilters)
    {
        warmupDemoterConfiguration.setBatchSize(batchSize);
        warmupDemoterConfiguration.setMaxUsageThresholdPercentage(maxUsageThresholdPercentage);
        warmupDemoterConfiguration.setCleanupUsageThresholdPercentage(cleanupUsageThresholdPercentage);
        warmupDemoterConfiguration.setEpsilon(1);
        warmupDemoterConfiguration.setMaxElementsToDemoteInIteration(maxElementsToDemote);
        warmupDemoterService.setTupleFilters(tupleFilters);
        warmupDemoterService.setForceDeleteDeadObjects(false);
    }

    private ColumnFilter createColumnFilter(List<WarmUpType> warmupTypes)
    {
        return new ColumnFilter(new SchemaTableName(defaultSchemaName, defaultTableName),
                ImmutableList.of(new WarmupDemoterWarmupElementData("c0", warmupTypes)));
    }

    private void executeFilterTest(List<TupleFilter> tupleFilters,
            int expectedDeletedByTupleFilter)
    {
        warmupRules.add(buildWarmupRule(defaultSchemaName, defaultTableName, 30, WarmUpType.WARM_UP_TYPE_BASIC, highPriority, notEmptyTTL, defaultPredicates));
        warmUpElements.add(buildWarmupElement(30, Instant.now().toEpochMilli()));
        List<TupleRank> deadObjects = new ArrayList<>();
        List<TupleRank> failedObjects = new ArrayList<>();
        List<TupleRank> tupleRankList = new ArrayList<>();
        warmupDemoterService.buildTupleRank(warmupRules,
                new ArrayList<>(rowGroupDataMap.values()),
                deadObjects,
                failedObjects,
                tupleRankList,
                tupleFilters);

        assertThat(deadObjects.size()).isEqualTo(expectedDeletedByTupleFilter);
        assertThat(failedObjects.size()).isEqualTo(0);
        assertThat(tupleRankList.size()).isEqualTo(0);
    }

    private RowGroupData buildRowGroupData(String schemaName, String tableName, List<WarmUpElement> warmUpElements, Map<VaradaColumn, String> hivePartitionKeys, int fileIndex, boolean isEmpty)
    {
        return RowGroupData.builder()
                .rowGroupKey(new RowGroupKey(schemaName, tableName, DEFAULT_FILE_PATH + "_" + fileIndex, 0, 1L, 0, "", ""))
                .warmUpElements(warmUpElements)
                .partitionKeys(hivePartitionKeys)
                .isEmpty(isEmpty)
                .build();
    }

    private WarmupRule buildWarmupRule(String schema,
            String table,
            int weId,
            WarmUpType warmUpType,
            double priority,
            int ttl,
            Set<WarmupPredicateRule> predicates)
    {
        return WarmupRule.builder()
                .schema(schema)
                .table(table)
                .varadaColumn(new RegularColumn("c" + weId))
                .warmUpType(warmUpType)
                .priority(priority)
                .ttl(ttl)
                .predicates(predicates)
                .build();
    }

    private List<WarmupRule> buildWarmupRule(
            Set<WarmupProperties> warmupProperties,
            Set<WarmupPredicateRule> predicates)
    {
        return warmupProperties.stream()
                .map((prop) -> WarmupRule.builder()
                        .schema("schema1")
                        .table("table1")
                        .varadaColumn(new RegularColumn("c" + 0))
                        .warmUpType(prop.warmUpType())
                        .priority(prop.priority())
                        .ttl(prop.ttl())
                        .predicates(predicates)
                        .build())
                .toList();
    }
}
