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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.WarmupDemoterConfiguration;
import io.trino.plugin.varada.connector.TestingConnectorColumnHandle;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.dispatcher.model.WildcardColumn;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.classifier.PredicateContextData;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.demoter.AcquireWarmupStatus;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.warmup.model.PartitionValueWarmupPredicateRule;
import io.trino.plugin.varada.warmup.model.WarmupRule;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.varada.tools.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.varada.VaradaSessionProperties.ENABLE_DEFAULT_WARMING;
import static io.trino.plugin.varada.VaradaSessionProperties.ENABLE_DEFAULT_WARMING_INDEX;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.createWarmupRules;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.generateRowGroupData;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.mockColumns;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.mockConnectorSplit;
import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_BASIC;
import static io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_DATA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkerWarmingServiceTest
{
    private static final RowGroupData ROW_GROUP_NOT_EXIST = null;
    private static final VaradaColumn COLUMN1 = new RegularColumn("c1");
    private static final VaradaColumn COLUMN2 = new RegularColumn("c2");
    private final SchemaTableName defaultSchemaTableName = new SchemaTableName("database", "table");
    private MetricsManager metricsManager;
    private VaradaStatsWarmingService varadaStatsWarmingService;
    private WorkerTaskExecutorService workerTaskExecutorService;
    private WarmExecutionTaskFactory warmExecutionTaskFactory;
    private WarmupDemoterConfiguration warmupDemoterConfiguration;
    private GlobalConfiguration globalConfiguration;
    private DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private RowGroupKey rowGroupKey;

    @BeforeEach
    public void before()
    {
        metricsManager = mock(MetricsManager.class);
        varadaStatsWarmingService = VaradaStatsWarmingService.create(WARMING_SERVICE_STAT_GROUP);
        when(metricsManager.registerMetric(any())).thenReturn(varadaStatsWarmingService);
        when(metricsManager.get(any())).thenReturn(varadaStatsWarmingService);
        workerTaskExecutorService = mock(WorkerTaskExecutorService.class);
        warmExecutionTaskFactory = mock(WarmExecutionTaskFactory.class);
        ProxyExecutionTask proxyExecutionTask = mock(ProxyExecutionTask.class);
        when(warmExecutionTaskFactory.createExecutionTask(any(), any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), anyInt(), eq(WorkerTaskExecutorService.TaskExecutionType.PROXY))).thenReturn(proxyExecutionTask);
        warmupDemoterConfiguration = new WarmupDemoterConfiguration();
        globalConfiguration = new GlobalConfiguration();
        rowGroupKey = mock(RowGroupKey.class);
        dispatcherProxiedConnectorTransformer = mock(DispatcherProxiedConnectorTransformer.class);
    }

    /**
     * Required: C1 (DATA, BASIC)
     * empty row group exist -> C1 (DATA)
     * result: return WarmData with empty flag
     */
    @Test
    public void testWarmDataEmptyRowGroup()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer, List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));
        SetMultimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = HashMultimap.create();
        columnNameToWarmUpType.putAll(COLUMN1, List.of(WARM_UP_TYPE_BASIC, WARM_UP_TYPE_DATA));

        List<WarmupRule> warmupRules = createWarmupRules(columnNameToWarmUpType, defaultSchemaTableName);
        List<WarmUpType> alreadyWarmupTypes = List.of(WARM_UP_TYPE_DATA);
        RowGroupData rowGroupData = generateRowGroupData(columns,
                alreadyWarmupTypes,
                rowGroupKey,
                true);

        WarmData warmData = act(columns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);

        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.EMPTY_ROW_GROUP);
    }

    /**
     * Required: C1 (DATA, BASIC)
     * row group exist -> C1 (DATA)
     * result: warm C1 (BASIC)
     */
    @Test
    public void testWarmWarmSingleColType()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);

        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer, List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));
        SetMultimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = HashMultimap.create();
        columnNameToWarmUpType.putAll(COLUMN1, List.of(WARM_UP_TYPE_BASIC, WARM_UP_TYPE_DATA));

        List<WarmupRule> warmupRules = createWarmupRules(columnNameToWarmUpType, defaultSchemaTableName);
        List<WarmUpType> alreadyWarmupTypes = List.of(WARM_UP_TYPE_DATA);
        RowGroupData rowGroupData = generateRowGroupData(columns,
                alreadyWarmupTypes,
                rowGroupKey,
                false);

        WarmData warmData = act(columns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.WARM);
        assertThat(getActualColTypesToWarm(COLUMN1, warmData.requiredWarmUpTypeMap()))
                .isEqualTo(Set.of(WARM_UP_TYPE_BASIC));
        assertThat(warmData.columnHandleList()).isEqualTo(columns);
    }

    @Test
    public void testWildcardWarm()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);

        VaradaColumn expectedNotToWarmColumn = new RegularColumn("c3");
        List<ColumnHandle> allColumns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR),
                        Pair.of(COLUMN2.getName(), IntegerType.INTEGER),
                        Pair.of(expectedNotToWarmColumn.getName(), RowType.rowType(RowType.field(VarcharType.VARCHAR)))));

        List<ColumnHandle> expectedToWarmColumns = allColumns.stream()
                .filter(columnHandle -> !((TestingConnectorColumnHandle) columnHandle).name().equals(expectedNotToWarmColumn.getName()))
                .toList();

        List<WarmupRule> warmupRuleList = List.of(WarmupRule.builder()
                .schema(defaultSchemaTableName.getSchemaName())
                .table(defaultSchemaTableName.getTableName())
                .warmUpType(WARM_UP_TYPE_BASIC)
                .varadaColumn(new WildcardColumn())
                .priority(2)
                .ttl(2)
                .predicates(Set.of())
                .build());

        List<WarmUpType> alreadyWarmupTypes = List.of(WARM_UP_TYPE_DATA);
        RowGroupData rowGroupData = generateRowGroupData(expectedToWarmColumns,
                alreadyWarmupTypes,
                rowGroupKey,
                false);

        WarmData warmData = act(allColumns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRuleList);

        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.WARM);
        assertThat(getActualColTypesToWarm(COLUMN1, warmData.requiredWarmUpTypeMap()))
                .containsExactly(WARM_UP_TYPE_BASIC);
        assertThat(getActualColTypesToWarm(COLUMN2, warmData.requiredWarmUpTypeMap()))
                .containsExactly(WARM_UP_TYPE_BASIC);
        assertThat(getActualColTypesToWarm(expectedNotToWarmColumn, warmData.requiredWarmUpTypeMap()))
                .isEmpty();
        assertThat(warmData.columnHandleList()).isEqualTo(expectedToWarmColumns);
    }

    /**
     * Default warmup enabled!!
     * Required: C1,  C2
     * row group exist -> C1 (BASIC)
     * result: warm C1 (DATA), C2(DATA)
     */
    @Test
    public void testWarmOnlyDataWhenFlagIsOn()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer, List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR),
                Pair.of(COLUMN2.getName(), VarcharType.VARCHAR)));
        SetMultimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = HashMultimap.create();
        columnNameToWarmUpType.putAll(COLUMN1, List.of(WarmUpType.WARM_UP_TYPE_BASIC));

        List<WarmupRule> warmupRules = createWarmupRules(columnNameToWarmUpType, defaultSchemaTableName);
        List<WarmUpType> alreadyWarmupTypes = List.of(WarmUpType.WARM_UP_TYPE_BASIC);
        List<ColumnHandle> alreadyWarmedColumn = columns.stream()
                .filter(x -> ((TestingConnectorColumnHandle) x).name().equals(COLUMN1.getName()))
                .collect(Collectors.toList());

        RowGroupData rowGroupData = generateRowGroupData(alreadyWarmedColumn,
                alreadyWarmupTypes,
                rowGroupKey,
                false);

        globalConfiguration.setDataOnlyWarming(true);
        globalConfiguration.setCreateIndexInDefaultWarming(true);
        WarmData warmData = act(columns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITH_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.WARM);
        assertThat(getActualColTypesToWarm(COLUMN1, warmData.requiredWarmUpTypeMap()))
                .isEqualTo(Set.of(WarmUpType.WARM_UP_TYPE_DATA));
        assertThat(getActualColTypesToWarm(COLUMN2, warmData.requiredWarmUpTypeMap()))
                .isEqualTo(Set.of(WarmUpType.WARM_UP_TYPE_DATA));
    }

    /**
     * Default warmup enabled!!
     * Warm Onlu Data enabled!!
     * Required: C1,  C2
     * row group exist -> C1 (BASIC)
     * result: warm C1 (DATA), C2(DATA)
     */
    @Test
    public void testDefaultRulesWarmup()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer, List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR),
                Pair.of(COLUMN2.getName(), VarcharType.VARCHAR)));
        SetMultimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = HashMultimap.create();
        columnNameToWarmUpType.putAll(COLUMN1, List.of(WARM_UP_TYPE_BASIC));

        List<WarmupRule> warmupRules = createWarmupRules(columnNameToWarmUpType, defaultSchemaTableName);
        List<WarmUpType> alreadyWarmupTypes = List.of(WARM_UP_TYPE_BASIC);
        List<ColumnHandle> alreadyWarmedColumn = columns.stream()
                .filter(x -> ((TestingConnectorColumnHandle) x).name().equals(COLUMN1.getName()))
                .collect(Collectors.toList());

        RowGroupData rowGroupData = generateRowGroupData(alreadyWarmedColumn,
                alreadyWarmupTypes,
                rowGroupKey,
                false);

        WarmData warmData = act(columns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITH_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.WARM);
        assertThat(getActualColTypesToWarm(COLUMN1, warmData.requiredWarmUpTypeMap()))
                .isEqualTo(Set.of(WARM_UP_TYPE_DATA));
        assertThat(getActualColTypesToWarm(COLUMN2, warmData.requiredWarmUpTypeMap()))
                .isEqualTo(Set.of(WARM_UP_TYPE_DATA));
        assertThat(warmData.columnHandleList()).isEqualTo(columns);
        globalConfiguration.setCreateIndexInDefaultWarming(true);
        warmData = act(columns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITH_DEFAULT_WARMING,
                warmupRules);
        assertThat(getActualColTypesToWarm(COLUMN2, warmData.requiredWarmUpTypeMap()))
                .isEqualTo(Set.of(WARM_UP_TYPE_DATA, WARM_UP_TYPE_BASIC));
    }

    /**
     * Default warmup enabled!!
     * Required: C1
     * row group exist -> C1 (DATA)
     * result: warm C1 (BASIC)
     */
    @Test
    public void testDefaultRulesBasicAfterData()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer, List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));
        List<WarmUpType> alreadyWarmupTypes = List.of(WARM_UP_TYPE_DATA);
        RowGroupData rowGroupData = generateRowGroupData(columns,
                alreadyWarmupTypes,
                rowGroupKey,
                false);

        QueryContext queryContext = new QueryContext(new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE), ImmutableList.of(), 0, true);
        queryContext = queryContext.asBuilder().matchData(Optional.empty()).build();
        WarmData warmData = act(columns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITH_DEFAULT_WARMING,
                List.of(),
                queryContext,
                10);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.NOTHING_TO_WARM);
        assertThat(getActualColTypesToWarm(COLUMN1, warmData.requiredWarmUpTypeMap()))
                .isEmpty();
        assertThat(warmData.columnHandleList()).isEmpty();
    }

    static Stream<Arguments> unsupportedTypes()
    {
        return Stream.of(
                arguments(new MapType(VarcharType.VARCHAR, VarcharType.VARCHAR, new TypeOperators()), List.of()),
                arguments(new ArrayType(VarcharType.VARCHAR), List.of(WARM_UP_TYPE_DATA)),
                arguments(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3), List.of(WARM_UP_TYPE_DATA, WARM_UP_TYPE_BASIC)), //short - TimestampWithTimeZoneType
                arguments(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(4), List.of()), //long - TimestampWithTimeZoneType
                arguments(TimeType.createTimeType(4), List.of(WARM_UP_TYPE_DATA, WARM_UP_TYPE_BASIC)),
                arguments(TimeType.createTimeType(12), List.of(WARM_UP_TYPE_DATA, WARM_UP_TYPE_BASIC)),
                arguments(TimeWithTimeZoneType.createTimeWithTimeZoneType(9), List.of(WARM_UP_TYPE_DATA, WARM_UP_TYPE_BASIC)), //Short -TimeWithTimeZoneType
                arguments(TimeWithTimeZoneType.createTimeWithTimeZoneType(10), List.of()), //Long TimeWithTimeZoneType
                arguments(TimestampType.createTimestampType(6), List.of(WARM_UP_TYPE_DATA, WARM_UP_TYPE_BASIC)), // short - TimestampType
                arguments(TimestampType.createTimestampType(7), List.of()), // long TimestampType
                arguments(RowType.rowType(RowType.field(VarcharType.VARCHAR), RowType.field(VarcharType.VARCHAR)), List.of()));
    }

    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    public void testUnsupportedTypes(Type type, List<WarmUpType> expectedWarmupTypes)
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        globalConfiguration.setCreateIndexInDefaultWarming(true);
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer, List.of(Pair.of(type.getBaseName(), type)));
        List<WarmupRule> warmupRules = List.of();
        WarmData warmData = act(columns,
                ROW_GROUP_NOT_EXIST,
                warmupDemoterService,
                DefaultWarmingTestState.WITH_DEFAULT_WARMING,
                warmupRules);
        if (expectedWarmupTypes.isEmpty()) {
            assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.NOTHING_TO_WARM);
            assertThat(warmData.requiredWarmUpTypeMap().size()).isZero();
            assertThat(warmData.columnHandleList().size()).isZero();
        }
        else {
            assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.WARM);
            List<WarmUpType> actualWarmupTypes = warmData.requiredWarmUpTypeMap().values().stream().map(WarmupProperties::warmUpType).collect(Collectors.toList());
            assertThat(actualWarmupTypes).containsAll(expectedWarmupTypes);
            assertThat(warmData.columnHandleList().size()).isOne();
        }
    }

    /**
     * Required: C1 (DATA, BASIC)
     * row group exist -> C1 (DATA, BASIC)
     * result: empty
     */
    @Test
    public void testAllElementsAlreadyWarmed()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));
        SetMultimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = HashMultimap.create();
        columnNameToWarmUpType.putAll(COLUMN1, List.of(WARM_UP_TYPE_BASIC,
                WARM_UP_TYPE_DATA));

        List<WarmupRule> warmupRules = createWarmupRules(columnNameToWarmUpType, defaultSchemaTableName);
        List<WarmUpType> alreadyWarmupTypes = List.of(WARM_UP_TYPE_DATA, WARM_UP_TYPE_BASIC);
        RowGroupData rowGroupData = generateRowGroupData(columns,
                alreadyWarmupTypes,
                rowGroupKey,
                false);

        WarmData warmData = act(columns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.NOTHING_TO_WARM);
        assertThat(varadaStatsWarmingService.getall_elements_warmed_or_skipped()).isEqualTo(1);
    }

    /**
     * Required: C1 (DATA, BASIC), C2(DATA,BASIC)
     * row group exist -> C1 (DATA, BASIC)
     * result: warm C2 (DATA, BASIC)
     */
    @Test
    public void testWarmOneColumnAlreadyWarm()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR),
                        Pair.of(COLUMN2.getName(), VarcharType.VARCHAR)));

        SetMultimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = HashMultimap.create();
        columnNameToWarmUpType.putAll(COLUMN1, List.of(WARM_UP_TYPE_DATA,
                WARM_UP_TYPE_BASIC));
        columnNameToWarmUpType.putAll(COLUMN2, List.of(WARM_UP_TYPE_BASIC,
                WARM_UP_TYPE_DATA));
        List<WarmupRule> warmupRules = createWarmupRules(columnNameToWarmUpType, defaultSchemaTableName);
        List<WarmUpType> alreadyWarmupTypes = List.of(WARM_UP_TYPE_DATA, WARM_UP_TYPE_BASIC);
        List<ColumnHandle> warmedColumn = columns.stream().filter(x -> ((TestingConnectorColumnHandle) x).name().equals(COLUMN1.getName())).collect(Collectors.toList());
        RowGroupData rowGroupData = generateRowGroupData(warmedColumn,
                alreadyWarmupTypes,
                rowGroupKey,
                false);

        WarmData warmData = act(columns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.WARM);
        assertThat(getActualColTypesToWarm(COLUMN2, warmData.requiredWarmUpTypeMap()))
                .isEqualTo(Sets.newHashSet(columnNameToWarmUpType.get(COLUMN2)));
        assertThat(warmData.requiredWarmUpTypeMap().get(COLUMN1)).isEmpty();
        assertThat(warmData.columnHandleList())
                .isEqualTo(columns.stream().filter(x -> ((TestingConnectorColumnHandle) x).name().equals(COLUMN2.getName())).collect(Collectors.toList()));
    }

    /**
     * Required: C1 > DATA, BASIC, LUCENE
     * row group not exist
     * return C1, (Data, Basic, LUCENE)
     */
    @Test
    public void testRowGroupNotExist()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));
        SetMultimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = HashMultimap.create();
        columnNameToWarmUpType.putAll(COLUMN1, List.of(WARM_UP_TYPE_BASIC,
                WARM_UP_TYPE_DATA,
                WarmUpType.WARM_UP_TYPE_LUCENE));

        List<WarmupRule> warmupRules = createWarmupRules(columnNameToWarmUpType, defaultSchemaTableName);
        WarmData warmData = act(columns,
                ROW_GROUP_NOT_EXIST,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.WARM);
        Set<WarmUpType> actualColTypesToWarm = getActualColTypesToWarm(COLUMN1, warmData.requiredWarmUpTypeMap());
        Set<WarmUpType> expectedWarmUpTypes = Sets.newHashSet(columnNameToWarmUpType.get(COLUMN1));
        assertThat(actualColTypesToWarm).isEqualTo(expectedWarmUpTypes);
        assertThat(warmData.columnHandleList()).isEqualTo(columns);
    }

    /**
     * Required: C1 > BASIC
     * row group not exist, query on C2
     * return C1, (Data, Basic, LUCENE)
     */
    @Test
    public void testRequiredColumnNotExistInRules()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN2.getName(), VarcharType.VARCHAR)));
        SetMultimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = HashMultimap.create();
        columnNameToWarmUpType.putAll(COLUMN1, List.of(WARM_UP_TYPE_BASIC));

        List<WarmupRule> warmupRules = createWarmupRules(columnNameToWarmUpType, defaultSchemaTableName);
        WarmData warmData = act(columns,
                ROW_GROUP_NOT_EXIST,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.NOTHING_TO_WARM);
    }

    @Test
    public void testEmptyColumns()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        WarmData warmData = act(Collections.emptyList(),
                ROW_GROUP_NOT_EXIST,
                warmupDemoterService,
                DefaultWarmingTestState.WITH_DEFAULT_WARMING,
                Collections.emptyList());
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.NOTHING_TO_WARM);
        assertThat(varadaStatsWarmingService.getempty_column_list()).isEqualTo(1);
    }

    @Test
    public void testDemoterPreventByPriority_RowGroupNotExist()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        int lowPriority = -5;
        int validPriority = 0;
        when(warmupDemoterService.canAllowWarmup(lowPriority)).thenReturn(false);
        when(warmupDemoterService.canAllowWarmup(validPriority)).thenReturn(true);

        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));
        WarmupProperties validProperties = new WarmupProperties(WARM_UP_TYPE_BASIC, validPriority, 0, TransformFunction.NONE);
        WarmupProperties lowPriorityProperty1 = new WarmupProperties(WARM_UP_TYPE_DATA, lowPriority, 0, TransformFunction.NONE);
        WarmupProperties lowPriorityProperty2 = new WarmupProperties(WarmUpType.WARM_UP_TYPE_LUCENE, lowPriority, 0, TransformFunction.NONE);

        List<WarmupRule> warmupRules = List.of(createRule(COLUMN1, validProperties),
                createRule(COLUMN1, lowPriorityProperty1),
                createRule(COLUMN1, lowPriorityProperty2));
        WarmData warmData = act(columns,
                ROW_GROUP_NOT_EXIST,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.highestPriority()).isEqualTo(validPriority);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.WARM);
        assertThat(warmData.requiredWarmUpTypeMap().size()).isEqualTo(1);
        assertThat(warmData.requiredWarmUpTypeMap().get(COLUMN1)).isEqualTo(Set.of(validProperties));
        assertThat(warmData.columnHandleList()).isEqualTo(columns);
    }

    /**
     * already warmed C1 with BASIC, 2 warmup rules with low priority, expected that demoter will remove rules with low priority
     */
    @Test
    public void testDemoterPreventAllByPriority_RowGroupNotExist()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        int lowPriority = -5;
        when(warmupDemoterService.canAllowWarmup(lowPriority)).thenReturn(false);
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));
        WarmupProperties lowPriorityProperty1 = new WarmupProperties(WARM_UP_TYPE_DATA, lowPriority, 0, TransformFunction.NONE);
        WarmupProperties lowPriorityProperty2 = new WarmupProperties(WarmUpType.WARM_UP_TYPE_LUCENE, lowPriority, 0, TransformFunction.NONE);

        List<WarmupRule> warmupRules = List.of(createRule(COLUMN1, lowPriorityProperty1),
                createRule(COLUMN1, lowPriorityProperty2));

        WarmData warmData = act(columns,
                ROW_GROUP_NOT_EXIST,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.NOTHING_TO_WARM);
    }

    /**
     * already warmed C1 with BASIC, 2 warmup rules with low priority, expected that demoter will remove rules with low priority
     */
    @Test
    public void testDemoterPreventByPriority_RowGroupExist()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        int lowPriority = -5;
        int validPriority = 2;
        when(warmupDemoterService.canAllowWarmup(lowPriority)).thenReturn(false);
        when(warmupDemoterService.canAllowWarmup(validPriority)).thenReturn(true);

        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));
        List<WarmUpType> alreadyWarmupTypes = List.of(WARM_UP_TYPE_BASIC);

        RowGroupData rowGroupData = generateRowGroupData(columns,
                alreadyWarmupTypes,
                rowGroupKey,
                false);
        WarmupProperties validProperties = new WarmupProperties(WARM_UP_TYPE_BASIC, validPriority, 0, TransformFunction.NONE);
        WarmupProperties lowPriorityProperty1 = new WarmupProperties(WARM_UP_TYPE_DATA, lowPriority, 0, TransformFunction.NONE);
        WarmupProperties lowPriorityProperty2 = new WarmupProperties(WarmUpType.WARM_UP_TYPE_LUCENE, lowPriority, 0, TransformFunction.NONE);

        List<WarmupRule> warmupRules = List.of(createRule(COLUMN1, validProperties),
                createRule(COLUMN1, lowPriorityProperty1),
                createRule(COLUMN1, lowPriorityProperty2));

        WarmData warmData = act(columns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.NOTHING_TO_WARM);
    }

    /**
     * C1 temporarily failed to warm only once, expect to retry
     */
    @Test
    public void testRetryColumnThatFailedTemporarilyOnce()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);

        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));
        Map<WarmUpType, WarmUpElementState> warmUpTypeToState = Map.of(WARM_UP_TYPE_BASIC,
                new WarmUpElementState(WarmUpElementState.State.FAILED_TEMPORARILY, 1, System.currentTimeMillis()));

        RowGroupData rowGroupData = generateRowGroupData(columns,
                warmUpTypeToState,
                rowGroupKey,
                false);
        WarmupProperties warmupProperties = new WarmupProperties(WARM_UP_TYPE_BASIC, 2, 0, TransformFunction.NONE);

        List<WarmupRule> warmupRules = List.of(createRule(COLUMN1, warmupProperties));

        WarmData warmData = act(columns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.WARM);
        assertThat(warmData.requiredWarmUpTypeMap().size()).isEqualTo(1);
        assertThat(warmData.requiredWarmUpTypeMap().get(COLUMN1)).isEqualTo(Set.of(warmupProperties));
        assertThat(warmData.columnHandleList()).isEqualTo(columns);
    }

    /**
     * C1 temporarily failed to warm BASIC 2 times a while ago, expect to retry
     */
    @Test
    public void testRetryColumnThatFailedTemporarilyTwiceAWhileAgo()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);

        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));
        Map<WarmUpType, WarmUpElementState> warmUpTypeToState = Map.of(WARM_UP_TYPE_BASIC,
                new WarmUpElementState(WarmUpElementState.State.FAILED_TEMPORARILY, 2, 0));

        RowGroupData rowGroupData = generateRowGroupData(columns,
                warmUpTypeToState,
                rowGroupKey,
                false);
        WarmupProperties warmupProperties = new WarmupProperties(WARM_UP_TYPE_BASIC, 2, 0, TransformFunction.NONE);

        List<WarmupRule> warmupRules = List.of(createRule(COLUMN1, warmupProperties));

        WarmData warmData = act(columns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.WARM);
        assertThat(warmData.requiredWarmUpTypeMap().size()).isEqualTo(1);
        assertThat(warmData.requiredWarmUpTypeMap().get(COLUMN1)).isEqualTo(Set.of(warmupProperties));
        assertThat(warmData.columnHandleList()).isEqualTo(columns);
    }

    /**
     * C1 temporarily failed to warm BASIC 2 times recently, expect not to retry
     */
    @Test
    public void testSkipRetryColumnThatFailedTemporarilyTwiceRecently()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);

        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));
        Map<WarmUpType, WarmUpElementState> warmUpTypeToState = Map.of(WARM_UP_TYPE_BASIC,
                new WarmUpElementState(WarmUpElementState.State.FAILED_TEMPORARILY, 2, System.currentTimeMillis()));

        RowGroupData rowGroupData = generateRowGroupData(columns,
                warmUpTypeToState,
                rowGroupKey,
                false);
        WarmupProperties warmupProperties = new WarmupProperties(WARM_UP_TYPE_BASIC, 2, 0, TransformFunction.NONE);

        List<WarmupRule> warmupRules = List.of(createRule(COLUMN1, warmupProperties));

        WarmData warmData = act(columns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.NOTHING_TO_WARM);
        assertThat(varadaStatsWarmingService.getwarm_skip_temporary_failed_warmup_element()).isEqualTo(1);
    }

    /**
     * C1 temporarily failed to warm BASIC 2 times and DATA 10 times, expect to retry all of them together
     */
    @Test
    public void testRetryWarmTemporarilyFailed()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);

        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));
        Map<WarmUpType, WarmUpElementState> warmUpTypeToState = Map.of(
                WARM_UP_TYPE_BASIC,
                new WarmUpElementState(WarmUpElementState.State.FAILED_TEMPORARILY, 2, 0),
                WARM_UP_TYPE_DATA,
                new WarmUpElementState(WarmUpElementState.State.FAILED_TEMPORARILY, 10, 0));

        RowGroupData rowGroupData = generateRowGroupData(columns,
                warmUpTypeToState,
                rowGroupKey,
                false);
        WarmupProperties warmupPropertiesBasic = new WarmupProperties(WARM_UP_TYPE_BASIC, 2, 0, TransformFunction.NONE);
        WarmupProperties warmupPropertiesData = new WarmupProperties(WARM_UP_TYPE_DATA, 2, 0, TransformFunction.NONE);

        List<WarmupRule> warmupRules = List.of(createRule(COLUMN1, warmupPropertiesBasic),
                createRule(COLUMN1, warmupPropertiesData));

        WarmData warmData = act(columns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.WARM);
        assertThat(warmData.requiredWarmUpTypeMap().size()).isEqualTo(2);
    }

    /**
     * C1 permanently failed to warm BASIC, expect not to retry
     */
    @Test
    public void testSkipRetryPermanentlyFailedColumn()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);

        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));
        Map<WarmUpType, WarmUpElementState> warmUpTypeToState = Map.of(WARM_UP_TYPE_BASIC,
                WarmUpElementState.FAILED_PERMANENTLY);

        RowGroupData rowGroupData = generateRowGroupData(columns,
                warmUpTypeToState,
                rowGroupKey,
                false);
        WarmupProperties warmupProperties = new WarmupProperties(WARM_UP_TYPE_BASIC, 2, 0, TransformFunction.NONE);

        List<WarmupRule> warmupRules = List.of(createRule(COLUMN1, warmupProperties));

        WarmData warmData = act(columns,
                rowGroupData,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.NOTHING_TO_WARM);
        assertThat(varadaStatsWarmingService.getwarm_skip_permanent_failed_warmup_element()).isEqualTo(1);
    }

    @Test
    public void testDemoterPreventByDefaultPriority()
    {
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        int lowPriority = -5;
        int validPriority = 2;

        warmupDemoterConfiguration.setDefaultRulePriority(lowPriority);
        when(warmupDemoterService.canAllowWarmup(lowPriority)).thenReturn(false);
        when(warmupDemoterService.canAllowWarmup(validPriority)).thenReturn(true);
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));

        WarmupProperties validProperties = new WarmupProperties(WARM_UP_TYPE_BASIC, validPriority, 0, TransformFunction.NONE);
        List<WarmupRule> warmupRules = List.of(createRule(COLUMN1, validProperties));

        WarmData warmData = act(columns,
                ROW_GROUP_NOT_EXIST,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.WARM);
        assertThat(warmData.requiredWarmUpTypeMap().size()).isEqualTo(1);
        assertThat(warmData.requiredWarmUpTypeMap().get(COLUMN1)).isEqualTo(Set.of(validProperties));
        assertThat(warmData.columnHandleList()).isEqualTo(columns);
    }

    @Test
    public void testLimitNumberOfElementToMaxBundle_1()
    {
        int maxBundleColumns = 1;
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR),
                        Pair.of(COLUMN2.getName(), VarcharType.VARCHAR)));

        SetMultimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = HashMultimap.create();
        columnNameToWarmUpType.putAll(COLUMN1, List.of(WARM_UP_TYPE_DATA));
        columnNameToWarmUpType.putAll(COLUMN2, List.of(WARM_UP_TYPE_BASIC));

        List<WarmupRule> warmupRules = createWarmupRules(columnNameToWarmUpType, defaultSchemaTableName);

        WarmData warmData = act(columns,
                ROW_GROUP_NOT_EXIST,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules,
                maxBundleColumns);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.WARM);
        assertThat(warmData.requiredWarmUpTypeMap().size()).isEqualTo(maxBundleColumns);
    }

    @Test
    public void testLimitNumberOfElementToMaxBundle_2()
    {
        int maxBundleColumns = 2;
        WarmupDemoterService warmupDemoterService = mockWarmupDemoterService(false, AcquireWarmupStatus.SUCCESS);
        List<ColumnHandle> columns = mockColumns(dispatcherProxiedConnectorTransformer,
                List.of(Pair.of(COLUMN1.getName(), VarcharType.VARCHAR)));

        SetMultimap<VaradaColumn, WarmUpType> columnNameToWarmUpType = HashMultimap.create();
        columnNameToWarmUpType.putAll(COLUMN1, List.of(WARM_UP_TYPE_DATA,
                WarmUpType.WARM_UP_TYPE_LUCENE,
                WARM_UP_TYPE_BASIC));

        List<WarmupRule> warmupRules = createWarmupRules(columnNameToWarmUpType, defaultSchemaTableName);

        WarmData warmData = act(columns,
                ROW_GROUP_NOT_EXIST,
                warmupDemoterService,
                DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING,
                warmupRules,
                maxBundleColumns);
        assertThat(warmData.warmExecutionState()).isEqualTo(WarmExecutionState.WARM);
        assertThat(warmData.requiredWarmUpTypeMap().get(COLUMN1).size()).isEqualTo(maxBundleColumns);
    }

    @Test
    public void testWarmupRuleComparator()
    {
        WarmupRule warmupRuleRegularColumn = WarmupRule.builder()
                .schema(defaultSchemaTableName.getSchemaName())
                .table(defaultSchemaTableName.getTableName())
                .varadaColumn(new RegularColumn("1"))
                .warmUpType(WARM_UP_TYPE_DATA)
                .priority(1)
                .ttl(0)
                .predicates(Set.of())
                .build();
        WarmupRule warmupRuleRegularColumnWithPredicates = WarmupRule.builder(warmupRuleRegularColumn)
                .predicates(Set.of(new PartitionValueWarmupPredicateRule("c1", "val1")))
                .build();
        WarmupRule warmupRuleWildcardColumn = WarmupRule.builder(warmupRuleRegularColumn)
                .varadaColumn(new WildcardColumn())
                .build();
        WarmupRule warmupRuleWildcardColumnWithPredicates = WarmupRule.builder(warmupRuleWildcardColumn)
                .predicates(Set.of(new PartitionValueWarmupPredicateRule("c1", "val1")))
                .build();
        List<WarmupRule> expectedOrder = List.of(
                warmupRuleWildcardColumn,
                warmupRuleWildcardColumnWithPredicates,
                warmupRuleRegularColumn,
                warmupRuleRegularColumnWithPredicates);

        List<WarmupRule> warmupRules = new ArrayList<>(expectedOrder);
        Collections.shuffle(warmupRules, new Random(3));
        assertThat(warmupRules).isNotEqualTo(expectedOrder);
        assertThat(warmupRules.stream()
                .sorted(WorkerWarmingService.warmupRuleComparator)
                .toList()).isEqualTo(expectedOrder);
    }

    private WarmData act(List<ColumnHandle> columns,
            RowGroupData rowGroupData,
            WarmupDemoterService warmupDemoterService,
            DefaultWarmingTestState defaultWarmingTestState,
            List<WarmupRule> warmupRules)
    {
        return act(columns, rowGroupData, warmupDemoterService, defaultWarmingTestState, warmupRules, 10);
    }

    private WarmData act(List<ColumnHandle> columns,
            RowGroupData rowGroupData,
            WarmupDemoterService warmupDemoterService,
            DefaultWarmingTestState defaultWarmingTestState,
            List<WarmupRule> warmupRules,
            int batchSize)
    {
        QueryContext queryContext = new QueryContext(new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE), ImmutableList.copyOf(columns), 0, true);
        return act(columns, rowGroupData, warmupDemoterService, defaultWarmingTestState, warmupRules, queryContext, batchSize);
    }

    private WarmData act(List<ColumnHandle> columns,
            RowGroupData rowGroupData,
            WarmupDemoterService warmupDemoterService,
            DefaultWarmingTestState defaultWarmingTestState,
            List<WarmupRule> warmupRules,
            QueryContext queryContext,
            int batchSize)
    {
        WorkerWarmupRuleService workerWarmupRuleService = mock(WorkerWarmupRuleService.class);
        when(workerWarmupRuleService.getWarmupRules(any())).thenReturn(warmupRules);
        RowGroupDataService rowGroupDataService = mock(RowGroupDataService.class);
        ConnectorSession connectorSession = mock(ConnectorSession.class);
        when(connectorSession.getProperty(eq(ENABLE_DEFAULT_WARMING_INDEX), any())).thenReturn(globalConfiguration.isCreateIndexInDefaultWarming());
        if (defaultWarmingTestState == DefaultWarmingTestState.WITHOUT_DEFAULT_WARMING) {
            globalConfiguration.setEnableDefaultWarming(false);
            when(connectorSession.getProperty(eq(ENABLE_DEFAULT_WARMING), any())).thenReturn(false);
        }
        StorageWarmerService storageWarmerService = mock(StorageWarmerService.class);
        when(storageWarmerService.tryAllocateNativeResourceForWarmup()).thenReturn(true);
        WorkerWarmingService workerWarmingService = new WorkerWarmingService(metricsManager,
                dispatcherProxiedConnectorTransformer,
                workerTaskExecutorService,
                warmExecutionTaskFactory,
                warmupDemoterService,
                workerWarmupRuleService,
                rowGroupDataService,
                warmupDemoterConfiguration,
                globalConfiguration,
                storageWarmerService,
                batchSize);
        Pair<DispatcherSplit, RowGroupKey> dispatcherSplitRowGroupKeyPair = mockConnectorSplit();
        when(rowGroupDataService.get(dispatcherSplitRowGroupKeyPair.getRight())).thenReturn(rowGroupData);

        return workerWarmingService.getWarmData(columns,
                dispatcherSplitRowGroupKeyPair.getRight(),
                dispatcherSplitRowGroupKeyPair.getLeft(),
                connectorSession,
                queryContext,
                false);
    }

    private WarmupRule createRule(VaradaColumn varadaColumn, WarmupProperties validProperties)
    {
        return WarmupRule.builder()
                .schema(defaultSchemaTableName.getSchemaName())
                .table(defaultSchemaTableName.getTableName())
                .varadaColumn(varadaColumn)
                .warmUpType(validProperties.warmUpType())
                .priority(validProperties.priority())
                .ttl(validProperties.ttl())
                .predicates(Set.of())
                .build();
    }

    private Set<WarmUpType> getActualColTypesToWarm(VaradaColumn columnName,
            SetMultimap<VaradaColumn, WarmupProperties> result)
    {
        if (!result.containsKey(columnName)) {
            return Set.of();
        }
        return result.get(columnName)
                .stream()
                .map(WarmupProperties::warmUpType)
                .collect(Collectors.toSet());
    }

    public static WarmupDemoterService mockWarmupDemoterService(boolean isExecuting, AcquireWarmupStatus acquireWarmupStatus)
    {
        WarmupDemoterService warmupDemoterService = mock(WarmupDemoterService.class);
        when(warmupDemoterService.isExecuting()).thenReturn(isExecuting);
        when(warmupDemoterService.canAllowWarmup(anyDouble())).thenReturn(true);
        when(warmupDemoterService.canAllowWarmup()).thenReturn(true);
        when(warmupDemoterService.tryAllocateNativeResourceForWarmup()).thenReturn(acquireWarmupStatus);
        return warmupDemoterService;
    }

    private enum DefaultWarmingTestState
    {
        WITH_DEFAULT_WARMING,
        WITHOUT_DEFAULT_WARMING
    }
}
