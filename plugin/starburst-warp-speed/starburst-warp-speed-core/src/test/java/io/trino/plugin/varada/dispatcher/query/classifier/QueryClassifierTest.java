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
package io.trino.plugin.varada.dispatcher.query.classifier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.connector.TestingConnectorColumnHandle;
import io.trino.plugin.varada.dispatcher.CompletedDynamicFilter;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.SimplifiedColumns;
import io.trino.plugin.varada.dispatcher.SingleValue;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.varada.dispatcher.query.MatchCollectUtils.MatchCollectType;
import io.trino.plugin.varada.dispatcher.query.PredicateData;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.collect.PrefilledQueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.match.BasicBloomQueryMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpressionData;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.varada.expression.VaradaSliceConstant;
import io.trino.plugin.varada.expression.VaradaVariable;
import io.trino.plugin.varada.expression.rewrite.WarpExpression;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.juffer.PredicateBufferInfo;
import io.trino.plugin.varada.juffer.PredicateBufferPoolType;
import io.trino.plugin.varada.juffer.PredicateCacheData;
import io.trino.plugin.varada.juffer.PredicatesCacheService;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static io.trino.plugin.varada.VaradaSessionProperties.ENABLE_MATCH_COLLECT;
import static io.trino.plugin.varada.VaradaSessionProperties.PREDICATE_SIMPLIFY_THRESHOLD;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.createLuceneQueryMatchData;
import static io.trino.plugin.varada.dispatcher.WarmupTestDataUtil.mockColumnHandle;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryClassifierTest
{
    /**
     * H0,H1 are hive
     * V0,V1,V2 are varada warmed where -
     * V0 [data]
     * V1[data,basic]
     * V2[data,basic,lucene]
     */

    private final SchemaTableName schemaTableName = new SchemaTableName("s1", "t1");
    private final Type intType = IntegerType.INTEGER;
    private final Type varcharType = VarcharType.createVarcharType(10);
    private final Type doubleType = DoubleType.DOUBLE;
    private final boolean transformAllowed = false;
    private DispatcherTableHandle dispatcherTableHandle;
    private List<TestingConnectorColumnHandle> testingConnectorColumnHandles;
    private Map<String, TestingConnectorColumnHandle> varadaColumnHandles;
    private Map<ColumnHandle, Map<WarmUpType, WarmUpElement>> weHandleToWarmUpElementByType;
    private List<WarmUpElement> warmUpElements;
    private RowGroupData rowGroupData;
    private RowGroupData rowGroupDataWithPartitionKeys;
    private PredicatesCacheService predicatesCacheService;
    private StorageEngineConstants storageEngineConstants;
    private QueryClassifier queryClassifier;
    private BufferAllocator bufferAllocator;
    private MatchCollectIdService matchCollectIdService;
    private GlobalConfiguration globalConfiguration;
    private NativeConfiguration nativeConfiguration;
    private DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private ConnectorSession session;
    private PredicateContextFactory predicateContextFactory;

    private void createWarmUpElements(String name, Type type, WarmUpType... warmUpTypes)
    {
        createWarmUpElements(name, type, WarmUpElementState.VALID, warmUpTypes);
    }

    private void createWarmUpElements(String name, Type type, WarmUpElementState state, WarmUpType... warmUpTypes)
    {
        TestingConnectorColumnHandle columnHandle = mockColumnHandle(name, type, dispatcherProxiedConnectorTransformer);
        when(dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(eq(columnHandle))).thenReturn(new RegularColumn(name));
        when(dispatcherProxiedConnectorTransformer.getColumnType(eq(columnHandle))).thenReturn(type);
        varadaColumnHandles.put(name, columnHandle);

        for (WarmUpType warmUpType : warmUpTypes) {
            WarmUpElement warmUpElement = WarmUpElement.builder()
                    .colName(name)
                    .warmUpType(warmUpType)
                    .recTypeCode(TypeUtils.convertToRecTypeCode(type, TypeUtils.getTypeLength(type, 8192), 8))
                    .recTypeLength(TypeUtils.getTypeLength(type, 8192))
                    .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                    .state(state)
                    .totalRecords(5)
                    .build();
            weHandleToWarmUpElementByType.computeIfAbsent(columnHandle, e -> new HashMap<>()).put(warmUpType, warmUpElement);
            warmUpElements.add(warmUpElement);
        }
    }

    @BeforeEach
    @SuppressWarnings("MockNotUsedInProduction")
    public void before()
    {
        dispatcherTableHandle = mockDispatcherTableHandle(schemaTableName);
        storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getMatchCollectBufferSize()).thenReturn(1024 * 1024);
        when(storageEngineConstants.getMaxChunksInRange()).thenReturn(1);
        when(storageEngineConstants.getMatchCollectNumIds()).thenReturn(100);
        when(storageEngineConstants.getMaxMatchColumns()).thenReturn(128);
        when(storageEngineConstants.getBundleNonCollectSize()).thenReturn(0);
        predicatesCacheService = mock(PredicatesCacheService.class);
        PredicateBufferInfo predicateBufferInfo = new PredicateBufferInfo(null, PredicateBufferPoolType.INVALID);
        when(predicatesCacheService.getOrCreatePredicateBufferId(isA(PredicateData.class), isA(Domain.class))).thenReturn(Optional.of(new PredicateCacheData(predicateBufferInfo, Optional.empty())));
        when(predicatesCacheService.predicateDataToBuffer(any(), any())).thenReturn(Optional.of(new PredicateCacheData(predicateBufferInfo, Optional.empty())));
        bufferAllocator = mock(BufferAllocator.class);
        when(bufferAllocator.getQueryNullBufferSize(any())).thenReturn(64 * 1024);
        when(bufferAllocator.getCollectRecordBufferSize(any(), eq(4))).thenReturn(256 * 1024);
        when(bufferAllocator.getCollectRecordBufferSize(any(), eq(8))).thenReturn(512 * 1024);
        when(bufferAllocator.getCollectRecordBufferSize(any(), eq(10))).thenReturn(512 * 1024);
        when(bufferAllocator.getMatchCollectRecordBufferSize(eq(4))).thenReturn(256 * 1024);
        when(bufferAllocator.getMatchCollectRecordBufferSize(eq(8))).thenReturn(512 * 1024);
        matchCollectIdService = mock(MatchCollectIdService.class);
        globalConfiguration = new GlobalConfiguration();
        this.dispatcherProxiedConnectorTransformer = mock(DispatcherProxiedConnectorTransformer.class);
        when(dispatcherProxiedConnectorTransformer.getConvertedPartitionValue(any(RowGroupData.class), any(), any())).thenReturn(Optional.empty());
        nativeConfiguration = mock(NativeConfiguration.class);
        when(nativeConfiguration.getBundleSize()).thenReturn(16 * 1024 * 1024);
        when(nativeConfiguration.getCollectTxSize()).thenReturn(8 * 1024 * 1024);
        //when(globalConfiguration.getEnableMatchCollect()).thenReturn(true);
        // in each test we will set the correct record data (int/varchar) as return value from getType
        ClassifierFactory classifierFactory = new ClassifierFactory(storageEngineConstants,
                predicatesCacheService,
                bufferAllocator,
                nativeConfiguration,
                dispatcherProxiedConnectorTransformer,
                matchCollectIdService, globalConfiguration);

        testingConnectorColumnHandles = new ArrayList<>();
        testingConnectorColumnHandles.add(mockColumnHandle("h0", IntegerType.INTEGER, dispatcherProxiedConnectorTransformer));
        testingConnectorColumnHandles.add(mockColumnHandle("h1", IntegerType.INTEGER, dispatcherProxiedConnectorTransformer));

        testingConnectorColumnHandles.forEach((ch) -> {
            String name = ch.name();
            Type type = ch.type();
            when(dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(eq(ch))).thenReturn(new RegularColumn(name));
            when(dispatcherProxiedConnectorTransformer.getColumnType(eq(ch))).thenReturn(type);
        });
        varadaColumnHandles = new HashMap<>();
        weHandleToWarmUpElementByType = new HashMap<>();
        warmUpElements = new ArrayList<>();
        createWarmUpElements("v-int-data", intType, WarmUpType.WARM_UP_TYPE_DATA);
        createWarmUpElements("v-int-data-basic", intType, WarmUpType.WARM_UP_TYPE_DATA, WarmUpType.WARM_UP_TYPE_BASIC);
        createWarmUpElements("v-varchar-all", varcharType, WarmUpType.WARM_UP_TYPE_DATA, WarmUpType.WARM_UP_TYPE_BASIC, WarmUpType.WARM_UP_TYPE_LUCENE);
        createWarmUpElements("v-int-data-basic-2", intType, WarmUpType.WARM_UP_TYPE_DATA, WarmUpType.WARM_UP_TYPE_BASIC);
        createWarmUpElements("v-varchar-data-basic", varcharType, WarmUpType.WARM_UP_TYPE_DATA, WarmUpType.WARM_UP_TYPE_BASIC);
        createWarmUpElements("v-double-basic", doubleType, WarmUpType.WARM_UP_TYPE_BASIC);
        createWarmUpElements("v-varchar-lucene", varcharType, WarmUpType.WARM_UP_TYPE_LUCENE);
        createWarmUpElements("v-varchar-basic", varcharType, WarmUpType.WARM_UP_TYPE_BASIC);
        createWarmUpElements("v-int-data-bloom", intType, WarmUpType.WARM_UP_TYPE_DATA, WarmUpType.WARM_UP_TYPE_BLOOM_HIGH);
        createWarmUpElements("v-int-bloom", intType, WarmUpType.WARM_UP_TYPE_BLOOM_HIGH);
        createWarmUpElements("v-varchar-data-lucene-failed", varcharType, WarmUpType.WARM_UP_TYPE_DATA);
        createWarmUpElements("v-varchar-data-lucene-failed", varcharType, WarmUpElementState.FAILED_PERMANENTLY, WarmUpType.WARM_UP_TYPE_LUCENE);

        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "path", 0, 1, 0, "", "");
        rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(warmUpElements)
                .build();
        rowGroupDataWithPartitionKeys = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(warmUpElements)
                .partitionKeys(Map.of(new RegularColumn("v-int-data"), "1",
                        new RegularColumn("v-varchar-data-basic"), "str"))
                .build();
        predicateContextFactory = new PredicateContextFactory(globalConfiguration, dispatcherProxiedConnectorTransformer);

        session = mock(ConnectorSession.class);
        when(session.getProperty(eq(ENABLE_MATCH_COLLECT), eq(Boolean.class))).thenReturn(true);
        queryClassifier = new QueryClassifier(
                classifierFactory,
                mock(ConnectorSync.class),
                matchCollectIdService,
                predicateContextFactory);
    }

    /**
     * `select count(H1),count(H2) from T`
     */
    @Test
    public void testProxiedCollect()
    {
        PredicateContextData predicateContextData = new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE);
        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.copyOf(testingConnectorColumnHandles), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(0, testingConnectorColumnHandles.get(0),
                1, testingConnectorColumnHandles.get(1)));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * `select count(H1),count(H2) from T where H1 = 1`
     */
    @Test
    public void testProxiedCollectMatch()
    {
        TupleDomain<ColumnHandle> fullPredicate =
                TupleDomain.withColumnDomains(Map.of(testingConnectorColumnHandles.get(0), Domain.singleValue(testingConnectorColumnHandles.get(0).type(), 1L)));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(testingConnectorColumnHandles.get(0), testingConnectorColumnHandles.get(1)), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(0, testingConnectorColumnHandles.get(0),
                1, testingConnectorColumnHandles.get(1)));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEqualTo(predicateContextData.getRemainingColumns());
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * `select count(V1), count(V2) from T`
     */
    @Test
    public void testVaradaCollect()
    {
        ColumnHandle dataIntColumn = varadaColumnHandles.get("v-int-data");
        ColumnHandle dataIntColumn2 = varadaColumnHandles.get("v-int-data-basic");
        WarmUpElement dataIntWarmUpElement = weHandleToWarmUpElementByType.get(dataIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement dataIntWarmUpElement2 = weHandleToWarmUpElementByType.get(dataIntColumn2).get(WarmUpType.WARM_UP_TYPE_DATA);
        PredicateContextData remainingPredicateContext = new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE);
        QueryContext queryContext = queryClassifier.classify(new QueryContext(remainingPredicateContext, ImmutableList.of(dataIntColumn, dataIntColumn2), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(dataIntWarmUpElement, 0),
                createNativeQueryCollectData(dataIntWarmUpElement2, 1));
        assertThat(queryContext.getMatchData()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * `select count(V1), count(V2) from T where V2 in (1, 2)`
     */
    @Test
    public void testVaradaCollectMatch()
    {
        TestingConnectorColumnHandle dataIntColumn = varadaColumnHandles.get("v-int-data");
        TestingConnectorColumnHandle matchCollectIntColumn = varadaColumnHandles.get("v-int-data-basic");
        WarmUpElement dataIntWarmUpElement = weHandleToWarmUpElementByType.get(dataIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchCollectIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Domain matchDomain = Domain.multipleValues(matchCollectIntColumn.type(), List.of(1L, 2L));
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(matchCollectIntColumn, matchDomain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(dataIntColumn, matchCollectIntColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns().size()).isZero();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(dataIntWarmUpElement, 0),
                createNativeQueryCollectData(matchCollectIntWarmUpElement, 1, true));
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();

        PredicateData predicateData = PredicateUtil.calcPredicateData(matchDomain, matchIntWarmUpElement.getRecTypeLength(), transformAllowed, matchCollectIntColumn.type());
        BasicBloomQueryMatchData expectedBasicBloomQueryMatchData = BasicBloomQueryMatchData.builder()
                .warmUpElement(matchIntWarmUpElement)
                .type(intType)
                .domain(Optional.of(matchDomain))
                .collectNulls(predicateData.isCollectNulls())
                .nativeExpression(createExpectedNativeExpression(matchDomain))
                .build();
        assertThat(((BasicBloomQueryMatchData) queryMatchData)).isEqualTo(expectedBasicBloomQueryMatchData);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * `select count(V2) from T where V2 in (1, 2)`
     */
    @Test
    public void testVaradaMatchCollectWithNotEnoughMemory()
    {
        TestingConnectorColumnHandle matchCollectIntColumn = varadaColumnHandles.get("v-int-data-basic");
        WarmUpElement collectIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Domain matchDomain = Domain.multipleValues(matchCollectIntColumn.type(), List.of(1L, 2L));
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(matchCollectIntColumn, matchDomain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        when(storageEngineConstants.getMatchCollectBufferSize()).thenReturn(1);
        ClassifierFactory classifierFactory = new ClassifierFactory(storageEngineConstants,
                predicatesCacheService,
                bufferAllocator,
                nativeConfiguration,
                dispatcherProxiedConnectorTransformer,
                matchCollectIdService, globalConfiguration);

        QueryClassifier queryClassifier = new QueryClassifier(
                classifierFactory,
                mock(ConnectorSync.class),
                matchCollectIdService,
                predicateContextFactory);
        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(matchCollectIntColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns().size()).isZero();

        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(collectIntWarmUpElement, 0));

        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();
        PredicateData predicateData = PredicateUtil.calcPredicateData(matchDomain, matchIntWarmUpElement.getRecTypeLength(), transformAllowed, matchCollectIntColumn.type());
        BasicBloomQueryMatchData expectedBasicBloomQueryMatchData = BasicBloomQueryMatchData.builder()
                .warmUpElement(matchIntWarmUpElement)
                .type(intType)
                .domain(Optional.of(matchDomain))
                .collectNulls(predicateData.isCollectNulls())
                .nativeExpression(createExpectedNativeExpression(matchDomain))
                .build();
        assertThat(((BasicBloomQueryMatchData) queryMatchData)).isEqualTo(expectedBasicBloomQueryMatchData);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /*    */

    /**
     * `select count(V2) from T where V2 != 1`
     * This test check the following optimizations:
     * 1. In case of a range predicate on a column which is indexed BASIC + DATA, prefer regular collect over match-collect.
     * 2. Since we don't match-collect, we should choose PREDICATE_TYPE_INVERSE_VALUES over PREDICATE_TYPE_RANGES (see {@code io.varada.presto.dispatcher.query.classifier.PredicateUtil#calcPredicateInfo})
     */
    @Test
    public void testChoseInversePredicateOverMatchCollect()
    {
        TestingConnectorColumnHandle matchCollectIntColumn = varadaColumnHandles.get("v-int-data-basic");
        WarmUpElement collectIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Range rangeLess = Range.lessThan(matchCollectIntColumn.type(), 1L);
        Range rangeGrate = Range.greaterThan(matchCollectIntColumn.type(), 1L);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(matchCollectIntColumn.type(), List.of(rangeLess, rangeGrate));
        Domain matchDomain = Domain.create(sortedRangeSet, false);
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(matchCollectIntColumn, matchDomain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        ClassifierFactory classifierFactory = new ClassifierFactory(storageEngineConstants,
                predicatesCacheService,
                bufferAllocator,
                nativeConfiguration,
                dispatcherProxiedConnectorTransformer,
                matchCollectIdService, globalConfiguration);

        QueryClassifier queryClassifier = new QueryClassifier(
                classifierFactory,
                mock(ConnectorSync.class),
                matchCollectIdService,
                predicateContextFactory);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(matchCollectIntColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(collectIntWarmUpElement, 0));

        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();
        assertThat(queryMatchData.getWarmUpElement()).isEqualTo(matchIntWarmUpElement);
        assertThat(queryMatchData.getType()).isEqualTo(intType);

        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    @Test
    public void testVaradaMatchWithDynamicFilter()
    {
        ColumnHandle dataIntColumn = varadaColumnHandles.get("v-int-data");
        TestingConnectorColumnHandle matchCollectIntColumn = varadaColumnHandles.get("v-int-data-basic");
        WarmUpElement dataIntWarmUpElement = weHandleToWarmUpElementByType.get(dataIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchCollectIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Domain matchDomain = Domain.multipleValues(matchCollectIntColumn.type(), List.of(1L, 2L, 3L));
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(matchCollectIntColumn, matchDomain));
        Domain dynamicFilterDomain = Domain.multipleValues(matchCollectIntColumn.type(), List.of(2L, 3L, 4L));
        TupleDomain<ColumnHandle> dynamicFilter = TupleDomain.withColumnDomains(Map.of(matchCollectIntColumn, dynamicFilterDomain));
        Domain expectedDomain = Domain.multipleValues(matchCollectIntColumn.type(), List.of(2L, 3L));
        DynamicFilter dynamic = new CompletedDynamicFilter(dynamicFilter);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, dynamic, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(dataIntColumn, matchCollectIntColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(dataIntWarmUpElement, 0),
                createNativeQueryCollectData(matchCollectIntWarmUpElement, 1, true));
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();

        PredicateData predicateData = PredicateUtil.calcPredicateData(expectedDomain, matchIntWarmUpElement.getRecTypeLength(), transformAllowed, matchCollectIntColumn.type());
        BasicBloomQueryMatchData expectedBasicBloomQueryMatchData = BasicBloomQueryMatchData.builder()
                .warmUpElement(matchIntWarmUpElement)
                .type(intType)
                .domain(Optional.of(expectedDomain))
                .collectNulls(predicateData.isCollectNulls())
                .nativeExpression(createExpectedNativeExpression(expectedDomain))
                .build();
        assertThat(((BasicBloomQueryMatchData) queryMatchData)).isEqualTo(expectedBasicBloomQueryMatchData);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    @Test
    public void testVaradaMatchWithStringRangeDynamicFilter()
    {
        TestingConnectorColumnHandle matchStringColumn = varadaColumnHandles.get("v-varchar-data-basic");
        WarmUpElement matchStringWarmUpElement = weHandleToWarmUpElementByType.get(matchStringColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchBasicStringWarmUpElement = weHandleToWarmUpElementByType.get(matchStringColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        // Create a dynamic filter with string range on a non-lucene column, so PredicateUtil.canApplyPredicate() will return PushDownSupport.PRESTO_ONLY
        Range range = Range.greaterThanOrEqual(matchStringColumn.type(), Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset())));
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(matchStringColumn.type(), List.of(range));
        Domain dynamicFilterDomain = Domain.create(sortedRangeSet, false);
        TupleDomain<ColumnHandle> dynamicFilter = TupleDomain.withColumnDomains(Map.of(matchStringColumn, dynamicFilterDomain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(TupleDomain.all());
        DynamicFilter completedDynamicFilter = new CompletedDynamicFilter(dynamicFilter);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, completedDynamicFilter, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(matchStringColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(matchStringWarmUpElement, 0));

        PredicateData predicateData = PredicateUtil.calcPredicateData(dynamicFilterDomain, matchBasicStringWarmUpElement.getRecTypeLength(), true, matchStringColumn.type());
        assertThat(predicateData.getPredicateInfo().predicateType()).isEqualTo(PredicateType.PREDICATE_TYPE_STRING_RANGES);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    @Test
    public void testVaradaMatchWithDynamicFilterOnADifferentColumn()
    {
        ColumnHandle dataIntColumn = varadaColumnHandles.get("v-int-data");
        TestingConnectorColumnHandle matchCollectIntColumn = varadaColumnHandles.get("v-int-data-basic");
        TestingConnectorColumnHandle matchOnlyIntColumn = varadaColumnHandles.get("v-int-data-basic-2");
        WarmUpElement dataIntWarmUpElement = weHandleToWarmUpElementByType.get(dataIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchCollectIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        WarmUpElement matchOnlyIntWarmUpElement = weHandleToWarmUpElementByType.get(matchOnlyIntColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Domain matchDomain = Domain.multipleValues(matchCollectIntColumn.type(), List.of(2L, 3L));
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(matchCollectIntColumn, matchDomain));
        Domain dynamicFilterDomain = Domain.multipleValues(matchOnlyIntColumn.type(), List.of(4L, 6L));
        TupleDomain<ColumnHandle> dynamicFilter = TupleDomain.withColumnDomains(Map.of(matchOnlyIntColumn, dynamicFilterDomain));

        TupleDomain<ColumnHandle> tupleDomain = fullPredicate.intersect(dynamicFilter);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(tupleDomain);
        DynamicFilter completedDynamicFilter = new CompletedDynamicFilter(dynamicFilter);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, completedDynamicFilter, dispatcherTableHandle);
        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(dataIntColumn, matchCollectIntColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(dataIntWarmUpElement, 0),
                createNativeQueryCollectData(matchCollectIntWarmUpElement, 1, true));
        int numMatches = 0;
        for (QueryMatchData queryMatchData : queryContext.getMatchLeavesDFS()) {
            if (queryMatchData.getWarmUpElement().equals(matchIntWarmUpElement)) {
                numMatches++;
            }
            else if (queryMatchData.getWarmUpElement().equals(matchOnlyIntWarmUpElement)) {
                numMatches++;
            }
        }
        assertThat(numMatches).isEqualTo(2);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    @Test
    public void testVaradaMatchWithDynamicFilterOnAProxiedColumn()
    {
        TestingConnectorColumnHandle matchCollectIntColumn = varadaColumnHandles.get("v-int-data-basic");
        WarmUpElement matchCollectIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Domain matchDomain = Domain.multipleValues(matchCollectIntColumn.type(), List.of(1L, 2L));
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(matchCollectIntColumn, matchDomain));
        Domain dynamicFilterDomain = Domain.multipleValues(matchCollectIntColumn.type(), List.of(3L, 4L));
        TestingConnectorColumnHandle column1 = testingConnectorColumnHandles.get(0);
        TupleDomain<ColumnHandle> dynamicFilter = TupleDomain.withColumnDomains(Map.of(column1, dynamicFilterDomain));

        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        DynamicFilter completedDynamicFilter = new CompletedDynamicFilter(dynamicFilter);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, completedDynamicFilter, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(column1, matchCollectIntColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(0, column1));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isNotEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(matchCollectIntWarmUpElement, 1, true));
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();

        BasicBloomQueryMatchData expectedBasicBloomQueryMatchData = BasicBloomQueryMatchData.builder()
                .warmUpElement(matchIntWarmUpElement)
                .type(intType)
                .domain(Optional.of(matchDomain))
                .nativeExpression(createExpectedNativeExpression(matchDomain))
                .build();
        assertThat(((BasicBloomQueryMatchData) queryMatchData)).isEqualTo(expectedBasicBloomQueryMatchData);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    @Test
    public void testVaradaMatchWithDynamicFilterOnALuceneColumn()
    {
        TestingConnectorColumnHandle dataIntColumn = varadaColumnHandles.get("v-int-data");
        TestingConnectorColumnHandle matchCollectIntColumn = varadaColumnHandles.get("v-int-data-basic");
        TestingConnectorColumnHandle matchStrWithLuceneColumn = varadaColumnHandles.get("v-varchar-lucene");
        WarmUpElement dataIntWarmUpElement = weHandleToWarmUpElementByType.get(dataIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchCollectIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Domain matchDomain = Domain.multipleValues(matchCollectIntColumn.type(), List.of(1L, 2L));
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(matchCollectIntColumn, matchDomain));
        Domain dynamicFilterDomain = Domain.multipleValues(matchStrWithLuceneColumn.type(), List.of(Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset())),
                Slices.wrappedBuffer("str2".getBytes(Charset.defaultCharset()))));
        TupleDomain<ColumnHandle> dynamicFilter = TupleDomain.withColumnDomains(Map.of(matchStrWithLuceneColumn, dynamicFilterDomain));

        WarmUpElement luceneWarmUpElement = weHandleToWarmUpElementByType.get(matchStrWithLuceneColumn).get(WarmUpType.WARM_UP_TYPE_LUCENE);
        Set<Range> orderedRanges = new HashSet<>(((SortedRangeSet) dynamicFilterDomain.getValues()).getOrderedRanges());
        LuceneQueryMatchData expectedLuceneQueryMatchData = createLuceneQueryMatchData(luceneWarmUpElement, false, orderedRanges, Set.of(), dynamicFilterDomain, false);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        DynamicFilter completedDynamicFilter = new CompletedDynamicFilter(dynamicFilter);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, completedDynamicFilter, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(dataIntColumn, matchCollectIntColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(dataIntWarmUpElement, 0),
                createNativeQueryCollectData(matchCollectIntWarmUpElement, 1, true));
        // Both columns have no cardinality statistics so the match-collect column should be last (See NativeMatchSorter)
        BasicBloomQueryMatchData expectedBasicBloomQueryMatchData = BasicBloomQueryMatchData.builder()
                .warmUpElement(matchIntWarmUpElement)
                .domain(Optional.of(matchDomain))
                .type(intType)
                .nativeExpression(createExpectedNativeExpression(matchDomain))
                .build();
        LogicalMatchData expectedResult = new LogicalMatchData(LogicalMatchData.Operator.AND, List.of(expectedLuceneQueryMatchData, expectedBasicBloomQueryMatchData));
        assertThat(queryContext.getMatchData().orElseThrow()).isEqualTo(expectedResult);
    }

    /**
     * `SELECT count(v-varchar-lucene)
     * FROM T
     * WHERE v-varchar-lucene like 'str%' AND v-varchar-lucene > 'str1'
     */
    @Test
    public void testVaradaMatchOnALuceneColumn()
    {
        TestingConnectorColumnHandle matchOnlyLuceneColumn = varadaColumnHandles.get("v-varchar-lucene");

        Range range = Range.greaterThan(matchOnlyLuceneColumn.type(), Slices.utf8Slice("str1"));
        Domain rangeDomain = Domain.create(ValueSet.ofRanges(range), false);

        Slice likePattern = Slices.utf8Slice("str%");
        VaradaCall likeVaradaExpression = createLikeVaradaExpression(matchOnlyLuceneColumn, likePattern);
        String name = matchOnlyLuceneColumn.name();
        RegularColumn regularColumn = new RegularColumn(name);
        VaradaExpressionData expressionData = new VaradaExpressionData(likeVaradaExpression, varcharType, false, Optional.empty(), regularColumn);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(TupleDomain.withColumnDomains(Map.of(matchOnlyLuceneColumn, rangeDomain)));
        WarpExpression warpExpression = new WarpExpression(expressionData.getExpression(), List.of(expressionData));
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(warpExpression));
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        WarmUpElement warmupElement = weHandleToWarmUpElementByType.get(matchOnlyLuceneColumn).get(WarmUpType.WARM_UP_TYPE_LUCENE);

        LuceneQueryMatchData expectedLuceneQueryMatchData = createLuceneQueryMatchData(warmupElement, false, Set.of(range), Set.of(likePattern), rangeDomain, false);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(matchOnlyLuceneColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(0, matchOnlyLuceneColumn));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        queryContext.getMatchData().orElseThrow().getLeavesDFS().forEach(x -> addInnerToMainQueryBuilder(queryBuilder, ((LuceneQueryMatchData) x).getQuery()));
        assertThat(queryBuilder.build()).isEqualTo(expectedLuceneQueryMatchData.getQuery());
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    @Test
    public void testVaradaMatchOnALuceneColumnWithDateFormat()
    {
        TestingConnectorColumnHandle matchOnlyLuceneColumn = varadaColumnHandles.get("v-varchar-lucene");

        Range range = Range.greaterThan(matchOnlyLuceneColumn.type(), Slices.utf8Slice("2021-07-06"));
        Domain rangeDomain = Domain.create(ValueSet.ofRanges(range), false);

        Slice likePattern = Slices.utf8Slice("2012-07%");
        VaradaCall likeVaradaExpression = createLikeVaradaExpression(matchOnlyLuceneColumn, likePattern);
        RegularColumn regularColumn = new RegularColumn(matchOnlyLuceneColumn.name());

        WarmUpElement warmupElement = weHandleToWarmUpElementByType.get(matchOnlyLuceneColumn).get(WarmUpType.WARM_UP_TYPE_LUCENE);

        LuceneQueryMatchData expectedLuceneQueryMatchData = createLuceneQueryMatchData(warmupElement, false, Set.of(range), Set.of(likePattern), rangeDomain, false);

        VaradaExpressionData expressionData = new VaradaExpressionData(likeVaradaExpression, varcharType, false, Optional.empty(), regularColumn);
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(new WarpExpression(expressionData.getExpression(), List.of(expressionData))));

        when(dispatcherTableHandle.getFullPredicate()).thenReturn(TupleDomain.withColumnDomains(Map.of(matchOnlyLuceneColumn, rangeDomain)));
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(matchOnlyLuceneColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(0, matchOnlyLuceneColumn));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns().size()).isZero();
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(LogicalMatchData.class);
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        queryContext.getMatchData().orElseThrow().getLeavesDFS().forEach(x -> addInnerToMainQueryBuilder(queryBuilder, ((LuceneQueryMatchData) x).getQuery()));
        assertThat(queryBuilder.build()).isEqualTo(expectedLuceneQueryMatchData.getQuery());
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * `SELECT count(v-varchar-all)
     * FROM T
     * WHERE (v-varchar-all is null OR v-varchar-all = 'str') AND (v-varchar-lucene is null OR v-varchar-lucene = 'str')
     */
    @Test
    public void testVaradaMatchAllowNull()
    {
        TestingConnectorColumnHandle matchOnlyLuceneColumn = varadaColumnHandles.get("v-varchar-lucene");
        TestingConnectorColumnHandle matchCollectStrWithLuceneColumn = varadaColumnHandles.get("v-varchar-all");
        WarmUpElement matchCollectWithLuceneDataWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectStrWithLuceneColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchCollectWithLuceneBasicWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectStrWithLuceneColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);

        Domain domain = Domain.onlyNull(varcharType).union(Domain.singleValue(varcharType, Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset()))));
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(
                Map.of(matchCollectStrWithLuceneColumn, domain,
                        matchOnlyLuceneColumn, domain));

        WarmUpElement warmupElement = weHandleToWarmUpElementByType.get(matchOnlyLuceneColumn).get(WarmUpType.WARM_UP_TYPE_LUCENE);
        Set<Range> orderedRanges = new HashSet<>(((SortedRangeSet) domain.getValues()).getOrderedRanges());
        LuceneQueryMatchData expectedLuceneQueryMatchData = createLuceneQueryMatchData(warmupElement, false, orderedRanges, emptySet(), domain, true);

        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(matchCollectStrWithLuceneColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        // match collect is not enabled for lucene so we expect empty
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(matchCollectWithLuceneDataWarmUpElement, 0));
        assertThat(queryContext.getMatchLeavesDFS().size()).isEqualTo(2);
        // matchCollectStrWithLuceneColumn has a basic index while matchOnlyLuceneColumn has only Lucene index
        PredicateData predicateData = PredicateUtil.calcPredicateData(domain, matchCollectWithLuceneBasicWarmUpElement.getRecTypeLength(), transformAllowed, matchCollectStrWithLuceneColumn.type());
        BasicBloomQueryMatchData expectedBasicBloomQueryMatchData = BasicBloomQueryMatchData.builder()
                .warmUpElement(matchCollectWithLuceneBasicWarmUpElement)
                .type(varcharType)
                .domain(Optional.of(domain))
                .collectNulls(predicateData.isCollectNulls())
                .nativeExpression(createExpectedNativeExpression(domain, PredicateType.PREDICATE_TYPE_STRING_VALUES))
                .build();
        LogicalMatchData and = new LogicalMatchData(LogicalMatchData.Operator.AND, List.of(expectedBasicBloomQueryMatchData, expectedLuceneQueryMatchData));
        assertThat(queryContext.getMatchData()).contains(and);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    @Test
    public void testVaradaMatchNotNullOnLuceneColumn()
    {
        TestingConnectorColumnHandle matchOnlyLuceneColumn = varadaColumnHandles.get("v-varchar-lucene");

        Domain notNullDomain = Domain.notNull(matchOnlyLuceneColumn.type());
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(
                Map.of(matchOnlyLuceneColumn, notNullDomain));

        WarmUpElement warmupElement = weHandleToWarmUpElementByType.get(matchOnlyLuceneColumn).get(WarmUpType.WARM_UP_TYPE_LUCENE);

        ImmutableSet<Range> ranges = ImmutableSet.of(Range.all(matchOnlyLuceneColumn.type()));
        LuceneQueryMatchData expectedLuceneQueryMatchData = createLuceneQueryMatchData(warmupElement, false, ranges, emptySet(), notNullDomain, false);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        DynamicFilter completedDynamicFilter = new CompletedDynamicFilter(TupleDomain.all());
        PredicateContextData predicateContextData = predicateContextFactory.create(session, completedDynamicFilter, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(matchOnlyLuceneColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(0, matchOnlyLuceneColumn));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns().size()).isZero();
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();
        assertThat(((LuceneQueryMatchData) queryMatchData)).isEqualTo(expectedLuceneQueryMatchData);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * `SELECT count(v-varchar-data-lucene-failed)
     * FROM T
     * WHERE v-varchar-data-lucene-failed = 'str'`
     */
    @Test
    public void testEqualsOnAFailedLuceneWarmUpElement()
    {
        TestingConnectorColumnHandle dataAndFailedLuceneColumn = varadaColumnHandles.get("v-varchar-data-lucene-failed");
        WarmUpElement dataAndFailedLuceneWarmUpElement = weHandleToWarmUpElementByType.get(dataAndFailedLuceneColumn).get(WarmUpType.WARM_UP_TYPE_DATA);

        Domain domain = Domain.singleValue(dataAndFailedLuceneColumn.type(), Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset())));
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(
                Map.of(dataAndFailedLuceneColumn, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        DynamicFilter completedDynamicFilter = new CompletedDynamicFilter(TupleDomain.all());
        PredicateContextData predicateContextData = predicateContextFactory.create(session, completedDynamicFilter, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(dataAndFailedLuceneColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        // We can't use the failed LUCENE index and the query doesn't contain string matchers so the domain remains at RemainingTupleDomain (see LuceneMatchClassifier)
        // Then, we use data index to match the value
        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();

        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isNotEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(dataAndFailedLuceneWarmUpElement, 0));
        assertThat(queryContext.getMatchData()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * `SELECT count(v-varchar-data-lucene-failed)
     * FROM T
     * WHERE v-varchar-data-lucene-failed like 'str%'`
     */
    @Test
    public void testLikeOnAFailedLuceneWarmUpElement()
    {
        TestingConnectorColumnHandle dataAndFailedLuceneColumn = varadaColumnHandles.get("v-varchar-data-lucene-failed");

        Slice likePattern = Slices.utf8Slice("str%");
        VaradaCall likeVaradaExpression = createLikeVaradaExpression(dataAndFailedLuceneColumn, likePattern);
        VaradaExpressionData varadaExpressionData = new VaradaExpressionData(likeVaradaExpression, varcharType, false, Optional.empty(), new RegularColumn(dataAndFailedLuceneColumn.name()));

        WarmUpElement collectWarmUpElement = weHandleToWarmUpElementByType.get(dataAndFailedLuceneColumn).get(WarmUpType.WARM_UP_TYPE_DATA);

        when(dispatcherTableHandle.getFullPredicate()).thenReturn(TupleDomain.all());
        when(dispatcherTableHandle.getWarpExpression()).thenReturn(Optional.of(new WarpExpression(likeVaradaExpression, List.of(varadaExpressionData))));
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext baseQueryContext = new QueryContext(predicateContextData, ImmutableList.of(dataAndFailedLuceneColumn), 0, true);
        QueryContext queryContext = queryClassifier.classify(baseQueryContext, rowGroupData, dispatcherTableHandle, Optional.of(session));

        // We can't use the failed LUCENE index but we filter the domain out of RemainingTupleDomain (see LuceneMatchClassifier)
        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns().size()).isOne();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(collectWarmUpElement, 0));
        assertThat(queryContext.getMatchData()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * `select count(H1) from T where V1 in (1, 2)`
     */
    @Test
    public void testProxiedCollectMatchVaradaCollect()
    {
        TestingConnectorColumnHandle dataIntColumn = varadaColumnHandles.get("v-int-data");
        Domain domain = Domain.multipleValues(dataIntColumn.type(), List.of(1L, 2L));
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(
                Map.of(dataIntColumn, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        DynamicFilter completedDynamicFilter = new CompletedDynamicFilter(TupleDomain.all());
        PredicateContextData predicateContextData = predicateContextFactory.create(session, completedDynamicFilter, dispatcherTableHandle);

        when(dispatcherProxiedConnectorTransformer.proxyHasPushedDownFilter(any())).thenReturn(true);
        QueryContext baseQueryContext = new QueryContext(predicateContextData, ImmutableList.of(testingConnectorColumnHandles.get(0), dataIntColumn), 0, true);
        QueryContext queryContext = queryClassifier.classify(baseQueryContext,
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));
        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(0, testingConnectorColumnHandles.get(0), 1, dataIntColumn));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns().size()).isEqualTo(1);
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * `
     * select count(H1),count(V1)
     * from T where V2 in(1,2)
     */
    @Test
    public void testProxiedCollectVaradaCollectMatch()
    {
        TestingConnectorColumnHandle dataIntColumn = varadaColumnHandles.get("v-int-data");
        TestingConnectorColumnHandle matchCollectIntColumn = varadaColumnHandles.get("v-int-data-basic");
        WarmUpElement dataIntWarmUpElement = weHandleToWarmUpElementByType.get(dataIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchCollectIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Domain matchDomain = Domain.multipleValues(matchCollectIntColumn.type(), List.of(1L, 2L));
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(matchCollectIntColumn, matchDomain));

        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(testingConnectorColumnHandles.get(0), dataIntColumn, matchCollectIntColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(0, testingConnectorColumnHandles.get(0)));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns().size()).isZero();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(dataIntWarmUpElement, 1),
                createNativeQueryCollectData(matchCollectIntWarmUpElement, 2, true));
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();

        BasicBloomQueryMatchData expectedBasicBloomQueryMatchData = BasicBloomQueryMatchData.builder()
                .warmUpElement(matchIntWarmUpElement)
                .type(intType)
                .domain(Optional.of(matchDomain))
                .nativeExpression(createExpectedNativeExpression(matchDomain))
                .build();
        assertThat(queryMatchData).isEqualTo(expectedBasicBloomQueryMatchData);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * `select count(H1), count(V1) from T where H1 = 1 and V2 in (1, 2)`
     */
    @Test
    public void testProxiedCollectMatchVaradaCollectMatch()
    {
        TestingConnectorColumnHandle dataIntColumn = varadaColumnHandles.get("v-int-data");
        TestingConnectorColumnHandle matchCollectIntColumn = varadaColumnHandles.get("v-int-data-basic");
        WarmUpElement dataIntWarmUpElement = weHandleToWarmUpElementByType.get(dataIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchCollectIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElement matchIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Domain matchDomain = Domain.multipleValues(matchCollectIntColumn.type(), List.of(1L, 2L));
        TestingConnectorColumnHandle column1 = testingConnectorColumnHandles.get(0);
        TupleDomain<ColumnHandle> fullPredicate =
                TupleDomain.withColumnDomains(Map.of(testingConnectorColumnHandles.get(0), Domain.singleValue(testingConnectorColumnHandles.get(0).type(), 1L),
                        matchCollectIntColumn, matchDomain));

        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(column1, testingConnectorColumnHandles.get(1), dataIntColumn, matchCollectIntColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(0, column1,
                1, testingConnectorColumnHandles.get(1)));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isNotEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(dataIntWarmUpElement, 2),
                createNativeQueryCollectData(matchCollectIntWarmUpElement, 3, true));
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();

        BasicBloomQueryMatchData expectedBasicBloomQueryMatchData = BasicBloomQueryMatchData.builder()
                .warmUpElement(matchIntWarmUpElement)
                .type(intType)
                .domain(Optional.of(matchDomain))
                .nativeExpression(createExpectedNativeExpression(matchDomain))
                .build();
        assertThat(queryMatchData).isEqualTo(expectedBasicBloomQueryMatchData);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * select count(H1), count(V3) from T where H1 = 1
     */
    @Test
    public void testProxiedAllPredicatesOnProxied()
    {
        Domain domain = Domain.singleValue(testingConnectorColumnHandles.get(0).type(), 1L);
        TupleDomain<ColumnHandle> fullPredicate =
                TupleDomain.withColumnDomains(Map.of(testingConnectorColumnHandles.get(0), domain));
        ColumnHandle matchStrWithLuceneColumn = varadaColumnHandles.get("v-varchar-all");
        WarmUpElement dataIntWarmUpElement = weHandleToWarmUpElementByType.get(matchStrWithLuceneColumn).get(WarmUpType.WARM_UP_TYPE_DATA);

        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(testingConnectorColumnHandles.get(0), matchStrWithLuceneColumn), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(0, testingConnectorColumnHandles.get(0)));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isNotEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(createNativeQueryCollectData(dataIntWarmUpElement, 1));
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    @Test
    public void testPrefilledCollectVaradaColumn()
    {
        TestingConnectorColumnHandle matchCollectIntColumn = varadaColumnHandles.get("v-int-data-basic");
        TestingConnectorColumnHandle matchCollectIntColumn2 = varadaColumnHandles.get("v-int-data-basic-2");
        WarmUpElement matchIntWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        WarmUpElement matchIntWarmUpElement2 = weHandleToWarmUpElementByType.get(matchCollectIntColumn2).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Domain domain = Domain.singleValue(matchCollectIntColumn.type(), 1L);
        Domain domain2 = Domain.singleValue(matchCollectIntColumn2.type(), 2L);
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(
                matchCollectIntColumn, domain, matchCollectIntColumn2, domain2));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(matchCollectIntColumn, matchCollectIntColumn2), 0, true),
                rowGroupData,
                mockDispatcherTableHandle(schemaTableName),
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex().values()).containsExactly(
                createPrefilledQueryCollectData(matchIntWarmUpElement, 0, domain),
                createPrefilledQueryCollectData(matchIntWarmUpElement2, 1, domain2));
        BasicBloomQueryMatchData expectedMatchData = BasicBloomQueryMatchData
                .builder()
                .warmUpElement(matchIntWarmUpElement)
                .type(matchCollectIntColumn.type())
                .domain(Optional.of(domain))
                .nativeExpression(createExpectedNativeExpression(domain))
                .tightnessRequired(false)
                .build();
        BasicBloomQueryMatchData expectedMatchData2 = BasicBloomQueryMatchData
                .builder()
                .warmUpElement(matchIntWarmUpElement2)
                .type(matchCollectIntColumn2.type())
                .domain(Optional.of(domain2))
                .tightnessRequired(true)
                .nativeExpression(createExpectedNativeExpression(domain2))
                .build();
        assertThat(queryContext.getMatchLeavesDFS()).containsExactlyInAnyOrder(expectedMatchData, expectedMatchData2);
    }

    @Test
    public void testPrefilledCollectProxiedColumn()
    {
        TestingConnectorColumnHandle matchOnlyBasicColumn = varadaColumnHandles.get("v-double-basic");
        WarmUpElement matchWarmUpElement = weHandleToWarmUpElementByType.get(matchOnlyBasicColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Domain matchDomain = Domain.singleValue(DoubleType.DOUBLE, 1.5d);
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(matchOnlyBasicColumn, matchDomain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(matchOnlyBasicColumn), 0, true),
                rowGroupData,
                mockDispatcherTableHandle(schemaTableName),
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex().values()).containsExactly(
                createPrefilledQueryCollectData(matchWarmUpElement, 0, matchDomain));
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();
        BasicBloomQueryMatchData expectedBasicBloomQueryMatchData = BasicBloomQueryMatchData.builder()
                .warmUpElement(matchWarmUpElement)
                .type(doubleType)
                .domain(Optional.of(matchDomain))
                .tightnessRequired(true)
                .nativeExpression(createExpectedNativeExpression(matchDomain))
                .build();
        assertThat(queryMatchData).isEqualTo(expectedBasicBloomQueryMatchData);
    }

    /**
     * select count(*) from T where v-int-data = 1 and v-double-basic = 1.5
     */
    @Test
    public void testPrefilledCollectProxiedColumnWithProxiedDomain()
    {
        TestingConnectorColumnHandle onlyDataColumnHandle = varadaColumnHandles.get("v-int-data");
        WarmUpElement onlyDataWarmUpElement = weHandleToWarmUpElementByType.get(onlyDataColumnHandle).get(WarmUpType.WARM_UP_TYPE_DATA);

        TestingConnectorColumnHandle onlyBasicColumnHandle = varadaColumnHandles.get("v-double-basic");
        WarmUpElement onlyBasicWarmUpElement = weHandleToWarmUpElementByType.get(onlyBasicColumnHandle).get(WarmUpType.WARM_UP_TYPE_BASIC);

        // We can't prefill column which has a proxied domain (a domain which is not at enforcedConstraint and can't be handled by Varada)
        // we assume that the proxied connector is not tight and might return rows with values which are not match the domain

        // It's a proxied domain since V1 is not indexed
        Domain proxiedDomain = Domain.singleValue(onlyDataColumnHandle.type(), 1L);

        // Also create a varada domain so we won't reach "all predicates are on proxied connector, goes to proxied connector"
        Domain varadaDomain = Domain.singleValue(onlyBasicColumnHandle.type(), 1.5d);
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(
                Map.of(onlyDataColumnHandle, proxiedDomain,
                        onlyBasicColumnHandle, varadaDomain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(onlyDataColumnHandle, onlyBasicColumnHandle), 0, true),
                rowGroupData,
                mockDispatcherTableHandle(schemaTableName),
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns().size()).isOne();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex().values()).containsExactly(
                createPrefilledQueryCollectData(onlyBasicWarmUpElement, 1, varadaDomain));
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(createNativeQueryCollectData(onlyDataWarmUpElement, 0));
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
    }

    /**
     * select count(*) from T where v-varchar-lucene = 'str'
     */
    @Test
    public void testPrefilledCollectProxiedLuceneColumn()
    {
        TestingConnectorColumnHandle matchOnlyLuceneColumn = varadaColumnHandles.get("v-varchar-lucene");
        Domain domain = Domain.singleValue(matchOnlyLuceneColumn.type(), Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset())));
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(matchOnlyLuceneColumn, domain));

        WarmUpElement warmupElement = weHandleToWarmUpElementByType.get(matchOnlyLuceneColumn).get(WarmUpType.WARM_UP_TYPE_LUCENE);

        Set<Range> orderedRanges = new HashSet<>(((SortedRangeSet) domain.getValues()).getOrderedRanges());
        LuceneQueryMatchData expectedLuceneQueryMatchData = createLuceneQueryMatchData(warmupElement, true, orderedRanges, emptySet(), domain, false);

        when(dispatcherTableHandle.getFullPredicate()).thenReturn(tupleDomain);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(matchOnlyLuceneColumn), 0, true),
                rowGroupData,
                mockDispatcherTableHandle(schemaTableName),
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex().size()).isEqualTo(1);
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();
        assertThat(queryMatchData).isEqualTo(expectedLuceneQueryMatchData);
    }

    /**
     * select v-varchar-data-basic from T where v-varchar-data-basic in ("str1", "str2")
     */
    @Test
    public void testBasicAndBloomMatchColumns()
    {
        TestingConnectorColumnHandle columnHandle = varadaColumnHandles.get("v-varchar-data-basic");
        Domain matchDomain = Domain.multipleValues(columnHandle.type(), List.of(Slices.utf8Slice("str1"), Slices.utf8Slice("str2")));
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(columnHandle, matchDomain));

        String columnName = columnHandle.name();
        WarmUpElement basicWarmupElement = WarmUpElement.builder()
                .colName(columnName)
                .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                .recTypeCode(RecTypeCode.REC_TYPE_VARCHAR)
                .recTypeLength(10)
                .state(WarmUpElementState.VALID)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();
        WarmUpElement bloomWarmupElement = WarmUpElement.builder()
                .colName(columnName)
                .warmUpType(WarmUpType.WARM_UP_TYPE_BLOOM_HIGH)
                .recTypeCode(RecTypeCode.REC_TYPE_VARCHAR)
                .recTypeLength(10)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .state(WarmUpElementState.VALID)
                .build();

        RowGroupData rowGroupData = RowGroupData.builder(this.rowGroupData).warmUpElements(ImmutableList.of(basicWarmupElement, bloomWarmupElement)).build();

        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(columnHandle), 0, true),
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getMatchLeavesDFS().size()).isEqualTo(2);
        // Bloom should be before Basic (See NativeMatchSorter)
        QueryMatchData queryMatchData = queryContext.getMatchLeavesDFS().get(0);

        BasicBloomQueryMatchData expectedBasicBloomQueryMatchData1 = BasicBloomQueryMatchData.builder()
                .warmUpElement(bloomWarmupElement)
                .type(varcharType)
                .domain(Optional.of(matchDomain))
                .nativeExpression(createExpectedNativeExpression(matchDomain, PredicateType.PREDICATE_TYPE_STRING_VALUES))
                .build();
        assertThat(queryMatchData).isEqualTo(expectedBasicBloomQueryMatchData1);
        QueryMatchData queryMatchData2 = queryContext.getMatchLeavesDFS().get(1);

        BasicBloomQueryMatchData expectedBasicBloomQueryMatchData2 = BasicBloomQueryMatchData.builder()
                .warmUpElement(basicWarmupElement)
                .type(varcharType)
                .domain(Optional.of(matchDomain))
                .nativeExpression(createExpectedNativeExpression(matchDomain, PredicateType.PREDICATE_TYPE_STRING_VALUES))
                .build();
        assertThat(queryMatchData2).isEqualTo(expectedBasicBloomQueryMatchData2);
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
    }

    /**
     * select count(v-double-basic) from T where v-double-basic = 1.5
     * [In addition: v-double-basic is marked as simplified]
     */
    @Test
    public void testNotPrefilledNonTightQueryContext()
    {
        TestingConnectorColumnHandle matchOnlyBasicColumn = varadaColumnHandles.get("v-double-basic");
        WarmUpElement matchWarmUpElement = weHandleToWarmUpElementByType.get(matchOnlyBasicColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);
        Domain matchDomain = Domain.singleValue(DoubleType.DOUBLE, 1.5d);

        ImmutableList<ColumnHandle> remainingCollectColumns = ImmutableList.of(matchOnlyBasicColumn);
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(matchOnlyBasicColumn, matchDomain));

        globalConfiguration.setPredicateSimplifyThreshold(0);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        QueryContext baseQueryContext = new QueryContext(predicateContextData, remainingCollectColumns, 0, true);
        QueryContext queryContext = queryClassifier.classify(baseQueryContext, rowGroupData, mockDispatcherTableHandle(schemaTableName), Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex().values()).isEmpty();
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(matchWarmUpElement, 0, true));
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();
        BasicBloomQueryMatchData expectedBasicBloomQueryMatchData = BasicBloomQueryMatchData.builder()
                .warmUpElement(matchWarmUpElement)
                .type(doubleType)
                .domain(Optional.of(matchDomain))
                .tightnessRequired(false)
                .simplifiedDomain(true)
                .nativeExpression(NativeExpression.builder()
                        .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .collectNulls(matchDomain.isNullAllowed())
                        .domain(matchDomain)
                        .build())
                .build();
        assertThat(queryMatchData).isEqualTo(expectedBasicBloomQueryMatchData);
    }

    /**
     * /*
     * v-int-data and v-varchar-data-basic are warmed partition columns
     * select count(v-int-data), count(v-varchar-data-basic) from T
     */
    @Test
    public void testAllPrefilledColumns()
    {
        ColumnHandle intPartitionColumn = varadaColumnHandles.get("v-int-data");
        ColumnHandle varcharPartitionColumn = varadaColumnHandles.get("v-varchar-data-basic");

        PredicateContextData predicateContextData = new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE);
        when(dispatcherProxiedConnectorTransformer.getConvertedPartitionValue(anyString(), any(), any(Optional.class)))
                .thenReturn(1L)
                .thenReturn(Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset())));
        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(intPartitionColumn, varcharPartitionColumn), 0, true),
                rowGroupDataWithPartitionKeys,
                dispatcherTableHandle,
                Optional.of(session));

        // Since we have only prefilled columns, we convert one column to regular collect in order to create a valid tx.
        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).hasSize(2);
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData()).isEmpty();
    }

    @Test
    public void testPrefilledWhenOnlyPartitionColumnsAndExternal()
    {
        TestingConnectorColumnHandle intPartitionColumn = varadaColumnHandles.get("v-int-data");
        WarmUpElement intPartitionDataWarmUpElement = weHandleToWarmUpElementByType.get(intPartitionColumn).get(WarmUpType.WARM_UP_TYPE_DATA);

        TestingConnectorColumnHandle varcharPartitionColumn = varadaColumnHandles.get("v-varchar-data-basic");
        WarmUpElement varcharPartitionDataWarmUpElement = weHandleToWarmUpElementByType.get(varcharPartitionColumn).get(WarmUpType.WARM_UP_TYPE_DATA);

        TestingConnectorColumnHandle column1 = testingConnectorColumnHandles.get(0);
        TupleDomain<ColumnHandle> fullPredicate =
                TupleDomain.withColumnDomains(Map.of(column1, Domain.singleValue(column1.type(), 1L)));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);

        Optional<SingleValue> expectedSingleValueV1 = Optional.of(SingleValue.create(intPartitionColumn.type(), 1L));
        Optional<SingleValue> expectedSingleValueV2 = Optional.of(SingleValue.create(varcharPartitionColumn.type(), Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset()))));

        when(dispatcherProxiedConnectorTransformer.getConvertedPartitionValue(anyString(), any(), any(Optional.class)))
                .thenReturn(1L)
                .thenReturn(Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset())));
        ImmutableList<ColumnHandle> remainingCollectColumns = ImmutableList.of(intPartitionColumn, varcharPartitionColumn, column1);
        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, remainingCollectColumns, 0, true),
                rowGroupDataWithPartitionKeys,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(Map.of(2, column1));
        assertThat(queryContext.getPredicateContextData().getRemainingColumns().contains(new RegularColumn(column1.name()))).isTrue();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex().values()).containsExactly(
                createPrefilledQueryCollectData(intPartitionDataWarmUpElement, 0, expectedSingleValueV1.orElseThrow()),
                createPrefilledQueryCollectData(varcharPartitionDataWarmUpElement, 1, expectedSingleValueV2.orElseThrow()));
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData()).isEmpty();
    }

    /**
     * v-int-data and v-varchar-data-basic are partition columns
     * select count(v-int-data), count(v-varchar-data-basic), count(v-varchar-all) from T
     */
    @Test
    public void testPrefilledCollectPartitionColumn()
    {
        TestingConnectorColumnHandle intPartitionColumn = varadaColumnHandles.get("v-int-data");
        WarmUpElement intPartitionDataWarmUpElement = weHandleToWarmUpElementByType.get(intPartitionColumn).get(WarmUpType.WARM_UP_TYPE_DATA);

        TestingConnectorColumnHandle varcharPartitionColumn = varadaColumnHandles.get("v-varchar-data-basic");
        WarmUpElement varcharPartitionDataWarmUpElement = weHandleToWarmUpElementByType.get(varcharPartitionColumn).get(WarmUpType.WARM_UP_TYPE_DATA);

        TestingConnectorColumnHandle varcharNonPartitionColumn = varadaColumnHandles.get("v-varchar-all");
        WarmUpElement varcharNonPartitionDataWarmUpElement = weHandleToWarmUpElementByType.get(varcharNonPartitionColumn).get(WarmUpType.WARM_UP_TYPE_DATA);

        // Since we select a non partition column (v-varchar-all) - we expect the partition columns to be prefilled
        Optional<SingleValue> expectedSingleValueV1 = Optional.of(SingleValue.create(intPartitionColumn.type(), 1L));
        Optional<SingleValue> expectedSingleValueV5 = Optional.of(SingleValue.create(varcharPartitionColumn.type(), Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset()))));

        PredicateContextData predicateContextData = new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE);
        when(dispatcherProxiedConnectorTransformer.getConvertedPartitionValue(anyString(), any(), any(Optional.class)))
                .thenReturn(1L)
                .thenReturn(Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset())));
        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(intPartitionColumn, varcharPartitionColumn, varcharNonPartitionColumn), 0, true),
                rowGroupDataWithPartitionKeys,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex().values()).containsExactly(
                createPrefilledQueryCollectData(intPartitionDataWarmUpElement, 0, expectedSingleValueV1.orElseThrow()),
                createPrefilledQueryCollectData(varcharPartitionDataWarmUpElement, 1, expectedSingleValueV5.orElseThrow()));
        assertThat(queryContext.getNativeQueryCollectDataList()).containsExactly(
                createNativeQueryCollectData(varcharNonPartitionDataWarmUpElement, 2));
        assertThat(queryContext.getMatchData()).isEmpty();
    }

    /**
     * v-int-data and v-varchar-data-basic are partition columns
     * select count(v-int-data), count(v-varchar-data-basic) from T WHERE v-int-data-basic-2 = 1
     */
    @Test
    public void testPrefilledCollectPartitionColumnWithPredicate()
    {
        TestingConnectorColumnHandle intPartitionColumn = varadaColumnHandles.get("v-int-data");
        WarmUpElement intPartitionDataWarmUpElement = weHandleToWarmUpElementByType.get(intPartitionColumn).get(WarmUpType.WARM_UP_TYPE_DATA);

        TestingConnectorColumnHandle varcharPartitionColumn = varadaColumnHandles.get("v-varchar-data-basic");
        WarmUpElement varcharPartitionDataWarmUpElement = weHandleToWarmUpElementByType.get(varcharPartitionColumn).get(WarmUpType.WARM_UP_TYPE_DATA);

        TestingConnectorColumnHandle intNonPartitionColumn = varadaColumnHandles.get("v-int-data-basic-2");
        WarmUpElement intNonPartitionBasicWarmUpElement = weHandleToWarmUpElementByType.get(intNonPartitionColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);

        // Since we have predicate on a non partition column (V4) - we expect the partition columns to be prefilled
        Domain domain = Domain.singleValue(intNonPartitionColumn.type(), 1L);
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(intNonPartitionColumn, domain));
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);

        Optional<SingleValue> expectedSingleValueV1 = Optional.of(SingleValue.create(intPartitionColumn.type(), 1L));
        Optional<SingleValue> expectedSingleValueV5 = Optional.of(SingleValue.create(varcharPartitionColumn.type(), Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset()))));
        PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
        when(dispatcherProxiedConnectorTransformer.getConvertedPartitionValue(anyString(), any(), any(Optional.class)))
                .thenReturn(1L)
                .thenReturn(Slices.wrappedBuffer("str".getBytes(Charset.defaultCharset())));
        QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(intPartitionColumn, varcharPartitionColumn), 0, true),
                rowGroupDataWithPartitionKeys,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).isEmpty();
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex().values()).containsExactly(
                createPrefilledQueryCollectData(intPartitionDataWarmUpElement, 0, expectedSingleValueV1.orElseThrow()),
                createPrefilledQueryCollectData(varcharPartitionDataWarmUpElement, 1, expectedSingleValueV5.orElseThrow()));
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        assertThat(queryContext.getMatchData().orElseThrow()).isInstanceOf(QueryMatchData.class);
        QueryMatchData queryMatchData = (QueryMatchData) queryContext.getMatchData().orElseThrow();
        BasicBloomQueryMatchData expectedBasicBloomQueryMatchData = BasicBloomQueryMatchData.builder()
                .warmUpElement(intNonPartitionBasicWarmUpElement)
                .type(intType)
                .domain(Optional.of(domain))
                .nativeExpression(createExpectedNativeExpression(domain))
                .build();
        assertThat(queryMatchData).isEqualTo(expectedBasicBloomQueryMatchData);
    }

    @Test
    public void testBloomUnsupportedPredicateTypes()
    {
        ColumnHandle dataBloomIntColumn = varadaColumnHandles.get("v-int-bloom");
        List<Domain> unsupportedDomains = List.of(Domain.create(ValueSet.ofRanges(Range.greaterThan(IntegerType.INTEGER, 5L)), true),
                Domain.notNull(IntegerType.INTEGER));
        for (Domain unsupportedDomain : unsupportedDomains) {
            TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(dataBloomIntColumn, unsupportedDomain));
            when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
            PredicateContextData predicateContextData = predicateContextFactory.create(session, DynamicFilter.EMPTY, dispatcherTableHandle);
            QueryContext queryContext = queryClassifier.classify(new QueryContext(predicateContextData, ImmutableList.of(dataBloomIntColumn), 0, true),
                    rowGroupData,
                    dispatcherTableHandle,
                    Optional.of(session));

            assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isNotEmpty();
            assertThat(queryContext.getPredicateContextData().getRemainingColumns().size()).isEqualTo(1);
            assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
            assertThat(queryContext.getMatchData()).isEmpty();
            assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
        }
    }

    /**
     * select count(v-int-data), count(v-int-bloom) from T where [DynamicFilter="v-int-data = 2"]
     * [In addition: predicateThreshold = 1, v-int-data is marked as simplified on the table handle]
     */
    @Test
    public void testGetBasicQueryContext()
    {
        int predicateThreshold = 1;
        when(session.getProperty(eq(PREDICATE_SIMPLIFY_THRESHOLD), eq(Integer.class))).thenReturn(predicateThreshold);
        TestingConnectorColumnHandle onlyDataColumnHandle = varadaColumnHandles.get("v-int-data");
        TestingConnectorColumnHandle dataBloomIntColumn = varadaColumnHandles.get("v-int-bloom");
        DispatcherTableHandle dispatcherTableHandle = mockDispatcherTableHandle(schemaTableName);
        RegularColumn regularColumn = new RegularColumn(onlyDataColumnHandle.name());
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of(regularColumn)));
        Domain domain = Domain.singleValue(IntegerType.INTEGER, 2L);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(onlyDataColumnHandle,
                domain));
        DynamicFilter dynamicFilter = new CompletedDynamicFilter(tupleDomain);

        QueryContext basicQueryContext = queryClassifier.getBasicQueryContext(ImmutableList.of(onlyDataColumnHandle, dataBloomIntColumn), dispatcherTableHandle, dynamicFilter, session);

        assertThat(basicQueryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(ImmutableMap.of(0, onlyDataColumnHandle,
                1, dataBloomIntColumn));
        assertThat(basicQueryContext.getPredicateContextData().getRemainingColumns().contains(regularColumn)).isTrue();
    }

    /**
     * select count(v-int-data) from T where [DynamicFilter="v-int-data < 3 OR v-int-data > 9"]
     * [In addition: predicateThreshold = 1]
     */
    @Test
    public void testGetBasicQueryContextSimplifiedDynamicFilter()
    {
        int predicateThreshold = 1;
        when(session.getProperty(eq(PREDICATE_SIMPLIFY_THRESHOLD), eq(Integer.class))).thenReturn(predicateThreshold);
        TestingConnectorColumnHandle onlyDataColumnHandle = varadaColumnHandles.get("v-int-data");
        DispatcherTableHandle dispatcherTableHandle = mockDispatcherTableHandle(schemaTableName);
        Domain domain = Domain.create(SortedRangeSet.copyOf(IntegerType.INTEGER,
                        List.of(Range.lessThan(IntegerType.INTEGER, 3L), Range.greaterThan(IntegerType.INTEGER, 9L))),
                false);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(Map.of(onlyDataColumnHandle, domain));
        DynamicFilter dynamicFilter = new CompletedDynamicFilter(tupleDomain);

        QueryContext basicQueryContext = queryClassifier.getBasicQueryContext(ImmutableList.of(onlyDataColumnHandle), dispatcherTableHandle, dynamicFilter, session);

        assertThat(basicQueryContext.getRemainingCollectColumnByBlockIndex()).isEqualTo(ImmutableMap.of(0, onlyDataColumnHandle));
        PredicateContextData actualRemainingPredicateContext = basicQueryContext.getPredicateContextData();
        assertThat(actualRemainingPredicateContext.getRemainingColumns().size()).isEqualTo(1);
    }

    /**
     * `select count(*) from T where v-int-data-basic = 1 AND [DynamicFilter="v-int-data < 3 OR v-int-data > 9"]`
     * [In addition: dispatcherTableHandle.isSubsumedPredicates() == true]
     */
    @Test
    public void testSubsumedPredicatesWithDynamicFilter()
    {
        TestingConnectorColumnHandle matchCollectIntColumn = varadaColumnHandles.get("v-int-data-basic");
        Domain matchDomain = Domain.singleValue(intType, 1L);
        TupleDomain<ColumnHandle> fullPredicate = TupleDomain.withColumnDomains(Map.of(matchCollectIntColumn, matchDomain));
        DispatcherTableHandle dispatcherTableHandle = mockDispatcherTableHandle(schemaTableName);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(fullPredicate);
        when(dispatcherTableHandle.isSubsumedPredicates()).thenReturn(true);
        WarmUpElement matchWarmUpElement = weHandleToWarmUpElementByType.get(matchCollectIntColumn).get(WarmUpType.WARM_UP_TYPE_BASIC);

        TestingConnectorColumnHandle onlyDataColumnHandle = varadaColumnHandles.get("v-int-data");
        Domain dynamicFilterDomain = Domain.create(SortedRangeSet.copyOf(intType,
                        List.of(Range.lessThan(intType, 3L), Range.greaterThan(intType, 9L))),
                false);
        TupleDomain<ColumnHandle> dynamicFilterTupleDomain = TupleDomain.withColumnDomains(Map.of(onlyDataColumnHandle, dynamicFilterDomain));
        DynamicFilter dynamicFilter = new CompletedDynamicFilter(dynamicFilterTupleDomain);
        RegularColumn dynamicFilterColumn = new RegularColumn(onlyDataColumnHandle.name());

        QueryContext basicQueryContext = queryClassifier.getBasicQueryContext(ImmutableList.of(), dispatcherTableHandle, dynamicFilter, session);

        QueryContext queryContext = queryClassifier.classify(
                basicQueryContext,
                rowGroupData,
                dispatcherTableHandle,
                Optional.of(session));

        assertThat(queryContext.getRemainingCollectColumnByBlockIndex()).isEmpty();
        assertThat(queryContext.getPredicateContextData().getRemainingColumns()).containsExactly(dynamicFilterColumn);
        assertThat(queryContext.getNativeQueryCollectDataList()).isEmpty();
        BasicBloomQueryMatchData expectedMatchData = BasicBloomQueryMatchData.builder()
                .warmUpElement(matchWarmUpElement)
                .type(intType)
                .domain(Optional.of(matchDomain))
                .tightnessRequired(true)
                .simplifiedDomain(false)
                .nativeExpression(NativeExpression.builder()
                        .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .collectNulls(matchDomain.isNullAllowed())
                        .domain(matchDomain)
                        .build())
                .build();
        assertThat(queryContext.getMatchData()).isEqualTo(Optional.of(expectedMatchData));
        assertThat(queryContext.getPrefilledQueryCollectDataByBlockIndex()).isEmpty();
        assertThat(queryContext.isCanBeTight()).isFalse();  // because of the dynamic filter
    }

    static Stream<Arguments> storeIdTest()
    {
        return Stream.of(
                arguments(List.of(List.of("c1")),
                        List.of("c1"), true),
                arguments(List.of(List.of("c1"), List.of("c1", "c2")),
                        List.of("c1"), true),
                arguments(List.of(List.of("c1"), List.of("c1", "c2")),
                        List.of("c1", "c3"), false),
                arguments(List.of(List.of("c1"), List.of("c4")),
                        List.of("c1", "c4", "c5"), false),
                arguments(List.of(List.of("c1"), List.of("c4"), List.of("c1", "c4", "c5")),
                        List.of("c1", "c4", "c5"), true),
                arguments(List.of(List.of("c1")),
                        List.of("c2"), false));
    }

    @ParameterizedTest
    @MethodSource("storeIdTest")
    public void testGetQueryStoreId(List<List<String>> warmedColumns, List<String> requiredColumns, boolean hasStoreId)
    {
        RowGroupData cacheRowGroupData = createCacheRowGroupData(warmedColumns);
        List<CacheColumnId> planSignatureColumns = requiredColumns.stream().map(CacheColumnId::new).toList();
        Optional<UUID> queryStoreId = queryClassifier.getQueryStoreId(cacheRowGroupData, planSignatureColumns);
        assertThat(queryStoreId.isPresent()).isEqualTo(hasStoreId);
    }

    private List<WarmUpElement> createWeWithSameStoreId(List<String> columns)
    {
        UUID storeId = UUID.randomUUID();
        List<WarmUpElement> warmUpElementList = new ArrayList<>();
        for (String column : columns) {
            WarmUpElement we = mock(WarmUpElement.class);
            when(we.getStoreId()).thenReturn(storeId);
            when(we.getVaradaColumn()).thenReturn(new RegularColumn(column));
            warmUpElementList.add(we);
        }
        return warmUpElementList;
    }

    private RowGroupData createCacheRowGroupData(List<List<String>> warmedColumns)
    {
        RowGroupData cachedRowGroupData = mock(RowGroupData.class);
        List<WarmUpElement> warmUpElementList = new ArrayList<>();
        for (var warmedSession : warmedColumns) {
            List<WarmUpElement> weWithSameStoreId = createWeWithSameStoreId(warmedSession);
            warmUpElementList.addAll(weWithSameStoreId);
        }
        when(cachedRowGroupData.getValidWarmUpElements()).thenReturn(warmUpElementList);
        return cachedRowGroupData;
    }

    private DispatcherTableHandle mockDispatcherTableHandle(SchemaTableName schemaTableName)
    {
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        when(dispatcherTableHandle.getSchemaTableName()).thenReturn(schemaTableName);
        when(dispatcherTableHandle.getFullPredicate()).thenReturn(TupleDomain.all());
        when(dispatcherTableHandle.getSimplifiedColumns()).thenReturn(new SimplifiedColumns(Set.of()));
        return dispatcherTableHandle;
    }

    private Type warmupElementToType(WarmUpElement warmUpElement)
    {
        return switch (warmUpElement.getRecTypeCode()) {
            case REC_TYPE_INTEGER -> intType;
            case REC_TYPE_VARCHAR -> varcharType;
            case REC_TYPE_DOUBLE -> doubleType;
            default -> null;
        };
    }

    private VaradaCall createLikeVaradaExpression(TestingConnectorColumnHandle columnHandle, Slice likePattern)
    {
        Type variableType = columnHandle.type();

        return new VaradaCall(LIKE_FUNCTION_NAME.getName(),
                List.of(new VaradaVariable(columnHandle, variableType),
                        new VaradaSliceConstant(likePattern, VarcharType.VARCHAR)), BOOLEAN);
    }

    private NativeQueryCollectData createNativeQueryCollectData(WarmUpElement warmUpElement, int blockIndex)
    {
        return createNativeQueryCollectData(warmUpElement, blockIndex, false);
    }

    private NativeQueryCollectData createNativeQueryCollectData(WarmUpElement warmUpElement,
            int blockIndex,
            boolean isMatchCollect)
    {
        return createNativeQueryCollectData(warmUpElement, blockIndex, isMatchCollect, warmupElementToType(warmUpElement));
    }

    private NativeQueryCollectData createNativeQueryCollectData(WarmUpElement warmUpElement,
            int blockIndex,
            boolean isMatchCollect,
            Type type)
    {
        return NativeQueryCollectData.builder()
                .warmUpElement(warmUpElement)
                .type(type)
                .blockIndex(blockIndex)
                .matchCollectType(isMatchCollect ? MatchCollectType.ORDINARY : MatchCollectType.DISABLED)
                .matchCollectId(isMatchCollect ? 0 : MatchCollectIdService.INVALID_ID)
                .build();
    }

    private PrefilledQueryCollectData createPrefilledQueryCollectData(WarmUpElement warmUpElement, int blockIndex, SingleValue singleValue)
    {
        return PrefilledQueryCollectData.builder()
                .varadaColumn(warmUpElement.getVaradaColumn())
                .type(warmupElementToType(warmUpElement))
                .blockIndex(blockIndex)
                .singleValue(singleValue)
                .build();
    }

    private PrefilledQueryCollectData createPrefilledQueryCollectData(WarmUpElement warmUpElement, int blockIndex, Domain singleValueDomain)
    {
        return createPrefilledQueryCollectData(warmUpElement,
                blockIndex,
                SingleValue.create(warmupElementToType(warmUpElement), singleValueDomain.getSingleValue()));
    }

    private static void addInnerToMainQueryBuilder(BooleanQuery.Builder queryBuilder, BooleanQuery innerQuery)
    {
        for (BooleanClause clause : innerQuery.clauses()) {
            queryBuilder.add(clause);
        }
    }

    private NativeExpression createExpectedNativeExpression(Domain matchDomain)
    {
        return createExpectedNativeExpression(matchDomain, PredicateType.PREDICATE_TYPE_VALUES);
    }

    private NativeExpression createExpectedNativeExpression(Domain matchDomain, PredicateType predicateType)
    {
        return NativeExpression.builder()
                .predicateType(predicateType)
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .collectNulls(matchDomain.isNullAllowed())
                .domain(matchDomain)
                .build();
    }
}
