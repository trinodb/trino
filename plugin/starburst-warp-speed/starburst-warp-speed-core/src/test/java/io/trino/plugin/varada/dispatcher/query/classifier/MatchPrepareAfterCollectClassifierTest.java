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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.varada.dispatcher.query.MatchCollectUtils;
import io.trino.plugin.varada.dispatcher.query.MatchCollectUtils.MatchCollectType;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.MatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

//@RunWith(JUnitParamsRunner.class)
public class MatchPrepareAfterCollectClassifierTest
{
    private static final int MAX_MATCH_COLUMNS = 5;

    MatchCollectIdService matchCollectIdService;
    MatchPrepareAfterCollectClassifier matchPrepareAfterCollectClassifier;
    QueryContext baseContext;
    RowGroupData rowGroupData;
    static final Random random = new Random();

    @BeforeEach
    public void before()
    {
        matchCollectIdService = mock(MatchCollectIdService.class);
        matchPrepareAfterCollectClassifier = new MatchPrepareAfterCollectClassifier(matchCollectIdService, MAX_MATCH_COLUMNS);
        baseContext = new QueryContext(new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE), ImmutableMap.of());
        rowGroupData = mock(RowGroupData.class);
    }

    private static List<NativeQueryCollectData> createCollectColumns(QueryMatchData... collectColumns)
    {
        List<NativeQueryCollectData> result = new ArrayList<>();
        for (int i = 0; i < collectColumns.length; i++) {
            QueryMatchData matchData = collectColumns[i];
            result.add(NativeQueryCollectData
                    .builder()
                    .blockIndex(i)
                    .warmUpElement(matchData.getWarmUpElement())
                    .matchCollectType(MatchCollectType.ORDINARY)
                    .type(matchData.getType())
                    .build());
        }
        return result;
    }

    @Test
    public void testExceededLimitRootIsOr()
    {
        final int maxMatchColumns = 3;
        QueryMatchData data = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_DATA);
        QueryMatchData basicWithCollectLowestPriority = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC, true);
        QueryMatchData onlyMatch = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC);
        QueryMatchData bloom = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BLOOM_LOW);
        List<NativeQueryCollectData> collectColumns = createCollectColumns(data, basicWithCollectLowestPriority);
        ImmutableMap<Integer, ColumnHandle> collectColumnsByBlockIndex = ImmutableMap.of(0, new TestingColumnHandle(data.getVaradaColumn().getName()),
                1, new TestingColumnHandle(basicWithCollectLowestPriority.getVaradaColumn().getName()));
        LogicalMatchData matchData = new LogicalMatchData(LogicalMatchData.Operator.OR,
                List.of(basicWithCollectLowestPriority, onlyMatch, data, bloom));

        matchPrepareAfterCollectClassifier = new MatchPrepareAfterCollectClassifier(matchCollectIdService, maxMatchColumns);
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        WarmedWarmupTypes.Builder warmedWarmupTypes = new WarmedWarmupTypes.Builder();
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE),
                collectColumnsByBlockIndex,
                warmedWarmupTypes.build(),
                false,
                true,
                false);
        QueryContext queryContext = baseContext.asBuilder()
                .matchData(Optional.of(matchData))
                .nativeQueryCollectDataList(collectColumns)
                .remainingCollectColumnByBlockIndex(Map.of())
                .build();
        assertThat(MatchCollectUtils.canBeMatchForMatchCollect(data, queryContext.getNativeQueryCollectDataList())).isTrue();
        assertThat(MatchCollectUtils.canBeMatchForMatchCollect(basicWithCollectLowestPriority, queryContext.getNativeQueryCollectDataList())).isTrue();

        QueryContext result = matchPrepareAfterCollectClassifier.classify(classifyArgs, queryContext);
        assertThat(result.getMatchData()).isEmpty();
    }

    @Test
    public void testExceededLimitWithMatchCollect()
    {
        final int maxMatchColumns = 3;

        QueryMatchData data = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_DATA);
        QueryMatchData basicWithCollectLowestPriority = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC, true);
        QueryMatchData onlyMatch = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC);
        QueryMatchData bloom = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BLOOM_LOW);

        List<NativeQueryCollectData> collectColumns = createCollectColumns(data, basicWithCollectLowestPriority);
        ImmutableMap<Integer, ColumnHandle> collectColumnsByBlockIndex = ImmutableMap.of(
                0, new TestingColumnHandle(data.getVaradaColumn().getName()),
                1, new TestingColumnHandle(basicWithCollectLowestPriority.getVaradaColumn().getName()));
        LogicalMatchData matchData = new LogicalMatchData(LogicalMatchData.Operator.AND,
                List.of(basicWithCollectLowestPriority, onlyMatch, data, bloom));

        matchPrepareAfterCollectClassifier = new MatchPrepareAfterCollectClassifier(matchCollectIdService, maxMatchColumns);
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        WarmedWarmupTypes.Builder warmedWarmupTypes = new WarmedWarmupTypes.Builder();
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE),
                collectColumnsByBlockIndex,
                warmedWarmupTypes.build(),
                false,
                true,
                false);
        QueryContext queryContext = baseContext.asBuilder()
                .matchData(Optional.of(matchData))
                .nativeQueryCollectDataList(collectColumns)
                .remainingCollectColumnByBlockIndex(Map.of())
                .build();
        assertThat(MatchCollectUtils.canBeMatchForMatchCollect(data, queryContext.getNativeQueryCollectDataList())).isTrue();
        assertThat(MatchCollectUtils.canBeMatchForMatchCollect(basicWithCollectLowestPriority, queryContext.getNativeQueryCollectDataList())).isTrue();

        QueryContext result = matchPrepareAfterCollectClassifier.classify(classifyArgs, queryContext);
        LogicalMatchData expectedMatchData = new LogicalMatchData(LogicalMatchData.Operator.AND, List.of(bloom, data, basicWithCollectLowestPriority));
        assertThat(result.getMatchData()).isEqualTo(Optional.of(expectedMatchData));
        assertThat(result.getRemainingCollectColumns()).isEmpty();
        assertThat(result.isCanBeTight()).isFalse();
    }

    @Test
    public void testMoreMatchCollectsThanMatchLimit()
    {
        final int maxMatchColumns = 1;
        int matchCollectId = 15;

        QueryMatchData matchCollect1 = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC, true);
        QueryMatchData matchCollect2 = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC, true);
        QueryMatchData bloom = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BLOOM_LOW);

        List<NativeQueryCollectData> collectColumns = createCollectColumns(matchCollect1, matchCollect2);

        TestingColumnHandle matchCollect1Handle = new TestingColumnHandle(matchCollect1.getVaradaColumn().getName());
        TestingColumnHandle matchCollect2Handle = new TestingColumnHandle(matchCollect2.getVaradaColumn().getName());
        ImmutableMap<Integer, ColumnHandle> collectColumnsByBlockIndex = ImmutableMap.of(
                0, matchCollect1Handle,
                1, matchCollect2Handle);
        LogicalMatchData matchData = new LogicalMatchData(LogicalMatchData.Operator.AND, List.of(matchCollect1, matchCollect2, bloom));

        matchPrepareAfterCollectClassifier = new MatchPrepareAfterCollectClassifier(matchCollectIdService, maxMatchColumns);
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        WarmedWarmupTypes.Builder warmedWarmupTypes = new WarmedWarmupTypes.Builder();
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE),
                collectColumnsByBlockIndex,
                warmedWarmupTypes.build(),
                false,
                true,
                false);
        QueryContext queryContext = baseContext.asBuilder()
                .matchData(Optional.of(matchData))
                .nativeQueryCollectDataList(collectColumns)
                .remainingCollectColumnByBlockIndex(Map.of())
                .build();
        assertThat(MatchCollectUtils.canBeMatchForMatchCollect(matchCollect1, queryContext.getNativeQueryCollectDataList())).isTrue();
        assertThat(MatchCollectUtils.canBeMatchForMatchCollect(matchCollect2, queryContext.getNativeQueryCollectDataList())).isTrue();

        when(matchCollectIdService.allocMatchCollectId()).thenReturn(matchCollectId);

        QueryContext result = matchPrepareAfterCollectClassifier.classify(classifyArgs, queryContext);

        assertThat(result.getMatchData()).isEqualTo(Optional.of(matchCollect1));
        NativeQueryCollectData correspondingCollect = collectColumns.get(0).asBuilder().matchCollectId(matchCollectId).build();
        assertThat(result.getNativeQueryCollectDataList()).containsExactly(correspondingCollect);
        assertThat(result.getRemainingCollectColumns()).containsExactly(matchCollect2Handle);
        assertThat(result.isCanBeTight()).isFalse();
    }

    /**
     * BLOOM , BASIC , BASIC_COLLECT. LUCENE
     */
    @Test
    public void testClassifier_ALLTypes()
    {
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        QueryMatchData bloomMatchDataHigh = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BLOOM_HIGH);
        QueryMatchData basicMatchData = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC);
        QueryMatchData basicCollectIndexMatchData = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC, true);
        QueryMatchData luceneCollectIndexMatchData = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_LUCENE);
        NativeQueryCollectData basicCollectIndexCollectData = generateNativeQueryCollectData(true, 0, basicCollectIndexMatchData.getVaradaColumn());
        WarmedWarmupTypes.Builder warmedWarmupTypes = new WarmedWarmupTypes.Builder();
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(),
                warmedWarmupTypes.build(),
                false,
                true,
                false);
        MatchData matchData = new LogicalMatchData(LogicalMatchData.Operator.AND, List.of(luceneCollectIndexMatchData, bloomMatchDataHigh, basicCollectIndexMatchData, basicMatchData));
        QueryContext queryContext = baseContext.asBuilder()
                .matchData(Optional.of(matchData))
                .nativeQueryCollectDataList(List.of(basicCollectIndexCollectData))
                .build();
        QueryContext classify = matchPrepareAfterCollectClassifier.classify(classifyArgs, queryContext);
        assertThat(((LogicalMatchData) classify.getMatchData().orElseThrow()).getTerms()).containsExactly(bloomMatchDataHigh, basicMatchData, luceneCollectIndexMatchData, basicCollectIndexMatchData);
    }

    /**
     * all column are bloom with different priority
     */
    @Test
    public void testClassifier_bloomPriority()
    {
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        QueryMatchData bloomMatchDataHigh = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BLOOM_HIGH);
        QueryMatchData bloomMatchDataMedium = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BLOOM_MEDIUM);
        QueryMatchData bloomMatchDataLow = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BLOOM_LOW);
        WarmedWarmupTypes.Builder warmedWarmupTypes = new WarmedWarmupTypes.Builder();
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(),
                warmedWarmupTypes.build(),
                false,
                true,
                false);
        MatchData matchData = new LogicalMatchData(LogicalMatchData.Operator.AND, List.of(bloomMatchDataLow, bloomMatchDataHigh, bloomMatchDataMedium));
        QueryContext queryContext = baseContext.asBuilder().matchData(Optional.of(matchData)).build();
        QueryContext classify = matchPrepareAfterCollectClassifier.classify(classifyArgs, queryContext);
        assertThat(((LogicalMatchData) classify.getMatchData().orElseThrow()).getTerms()).containsExactly(bloomMatchDataHigh, bloomMatchDataMedium, bloomMatchDataLow);
    }

    /**
     * AND (OR , LUCENE) -> AND (LUCENE, OR)
     */
    @Test
    public void testAndWithOR()
    {
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        QueryMatchData basicColumn = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC);
        QueryMatchData luceneColumn = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_LUCENE);
        QueryMatchData dataColumn = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_DATA);
        WarmedWarmupTypes.Builder warmedWarmupTypes = new WarmedWarmupTypes.Builder();
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(),
                warmedWarmupTypes.build(),
                false,
                true,
                false);
        LogicalMatchData matchData = new LogicalMatchData(LogicalMatchData.Operator.AND, List.of(
                new LogicalMatchData(LogicalMatchData.Operator.OR, List.of(dataColumn, basicColumn)),
                luceneColumn));
        LogicalMatchData expectedMatchData = new LogicalMatchData(LogicalMatchData.Operator.AND, List.of(
                luceneColumn,
                new LogicalMatchData(LogicalMatchData.Operator.OR, List.of(dataColumn, basicColumn))));

        QueryContext queryContext = baseContext.asBuilder().matchData(Optional.of(matchData)).build();
        QueryContext classify = matchPrepareAfterCollectClassifier.classify(classifyArgs, queryContext);
        assertThat(classify.getMatchData().orElseThrow()).isEqualTo(expectedMatchData);
    }

    /**
     * AND (OR , OR ) - sort should not apply under OR
     */
    @Test
    public void testAndOfOrs()
    {
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        QueryMatchData basicMatchCollectColumn = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC, true);
        QueryMatchData basicColumn = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC);
        QueryMatchData luceneColumn = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_LUCENE);
        QueryMatchData dataColumn = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_DATA);
        WarmedWarmupTypes.Builder warmedWarmupTypes = new WarmedWarmupTypes.Builder();
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(),
                warmedWarmupTypes.build(),
                false,
                true,
                false);
        LogicalMatchData matchData = new LogicalMatchData(LogicalMatchData.Operator.AND, List.of(
                new LogicalMatchData(LogicalMatchData.Operator.OR, List.of(dataColumn, basicMatchCollectColumn)),
                new LogicalMatchData(LogicalMatchData.Operator.OR, List.of(luceneColumn, basicColumn))));
        QueryContext queryContext = baseContext.asBuilder().matchData(Optional.of(matchData)).build();
        QueryContext classify = matchPrepareAfterCollectClassifier.classify(classifyArgs, queryContext);
        assertThat(classify.getMatchData().orElseThrow()).isEqualTo(matchData);
    }

    /**
     * OR (AND , AND ) - sort should not apply under OR
     */
    @Test
    public void testOrOfAnds()
    {
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        QueryMatchData basicMatchCollectColumn = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC, true);
        QueryMatchData basicColumn = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC);
        QueryMatchData luceneColumn = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_LUCENE);
        QueryMatchData dataColumn = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_DATA);
        WarmedWarmupTypes.Builder warmedWarmupTypes = new WarmedWarmupTypes.Builder();
        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(),
                warmedWarmupTypes.build(),
                false,
                true,
                false);
        LogicalMatchData matchData = new LogicalMatchData(LogicalMatchData.Operator.OR, List.of(
                new LogicalMatchData(LogicalMatchData.Operator.AND, List.of(dataColumn, basicMatchCollectColumn)),
                new LogicalMatchData(LogicalMatchData.Operator.AND, List.of(luceneColumn, basicColumn))));
        QueryContext queryContext = baseContext.asBuilder().matchData(Optional.of(matchData)).build();
        QueryContext classify = matchPrepareAfterCollectClassifier.classify(classifyArgs, queryContext);
        assertThat(classify.getMatchData().orElseThrow()).isEqualTo(matchData);
    }

    @Test
    public void testMatchCollectIdAlloc()
    {
        int maxMatchColumns = 1;
        matchPrepareAfterCollectClassifier = new MatchPrepareAfterCollectClassifier(matchCollectIdService, maxMatchColumns);
        DispatcherTableHandle dispatcherTableHandle = mock(DispatcherTableHandle.class);
        QueryMatchData basicMatchData1 = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC);
        QueryMatchData basicMatchData2 = generateQueryMatchData(WarmUpType.WARM_UP_TYPE_BASIC);

        NativeQueryCollectData nativeQueryCollectData1 = createCollectColumns(basicMatchData1).get(0);
        NativeQueryCollectData nativeQueryCollectData2 = createCollectColumns(basicMatchData2).get(0);
        NativeQueryCollectData nativeQueryCollectDataResult1 = nativeQueryCollectData1.asBuilder()
                .matchCollectId(0)
                .build();
        ColumnHandle collectColumnHandle1 = mock(ColumnHandle.class);
        ColumnHandle collectColumnHandle2 = mock(ColumnHandle.class);
        WarmedWarmupTypes.Builder warmedWarmupTypes = new WarmedWarmupTypes.Builder();

        ClassifyArgs classifyArgs = new ClassifyArgs(dispatcherTableHandle,
                rowGroupData,
                mock(PredicateContextData.class),
                ImmutableMap.of(0, collectColumnHandle1, 1, collectColumnHandle2),
                warmedWarmupTypes.build(),
                false,
                true,
                false);
        LogicalMatchData matchData = new LogicalMatchData(LogicalMatchData.Operator.AND, List.of(basicMatchData1, basicMatchData2));
        QueryContext queryContext = baseContext.asBuilder()
                .matchData(Optional.of(matchData))
                .nativeQueryCollectDataList(List.of(nativeQueryCollectData1, nativeQueryCollectData2))
                .remainingCollectColumnByBlockIndex(new HashMap<>())
                .build();
        QueryContext classify = matchPrepareAfterCollectClassifier.classify(classifyArgs, queryContext);

        assertThat(classify.getMatchData()).isEqualTo(Optional.of(basicMatchData1));
        assertThat(classify.getNativeQueryCollectDataList()).containsExactly(nativeQueryCollectDataResult1);
        assertThat(classify.getRemainingCollectColumnByBlockIndex()).containsExactly(Map.entry(0, collectColumnHandle1));
    }

    private static QueryMatchData generateQueryMatchData(WarmUpType warmUpType)
    {
        return generateQueryMatchData(warmUpType, false);
    }

    private static QueryMatchData generateQueryMatchData(WarmUpType warmUpType, boolean collect)
    {
        RegularColumn columnName = new RegularColumn("mock" + random.nextInt());
        WarmUpElement mockElement = mock(WarmUpElement.class, columnName.getName());
        when(mockElement.getVaradaColumn()).thenReturn(columnName);
        when(mockElement.getWarmUpType()).thenReturn(warmUpType);

        QueryMatchData mockQueryMatchData = mock(QueryMatchData.class, warmUpType.name() + " collect:" + collect + ", columnName: " + columnName);
        when(mockQueryMatchData.getType()).thenReturn(IntegerType.INTEGER);
        when(mockQueryMatchData.getWarmUpElement()).thenReturn(mockElement);
        when(mockQueryMatchData.getWarmUpElementOptional()).thenReturn(Optional.of(mockElement));
        when(mockQueryMatchData.getVaradaColumn()).thenReturn(columnName);
        when(mockQueryMatchData.getLeavesDFS()).thenReturn(List.of(mockQueryMatchData));
        when(mockQueryMatchData.canMatchCollect(eq(columnName))).thenReturn(true);
        return mockQueryMatchData;
    }

    private NativeQueryCollectData generateNativeQueryCollectData(boolean isMatchCollect, int blockIndex, VaradaColumn varadaColumn)
    {
        WarmUpElement collectWarmUpElement = mock(WarmUpElement.class);
        when(collectWarmUpElement.getVaradaColumn()).thenReturn(varadaColumn);
        return NativeQueryCollectData.builder()
                .warmUpElement(collectWarmUpElement)
                .matchCollectType(isMatchCollect ? MatchCollectType.ORDINARY : MatchCollectType.DISABLED)
                .matchCollectId(MatchCollectIdService.INVALID_ID)
                .type(IntegerType.INTEGER)
                .blockIndex(blockIndex)
                .build();
    }
}
