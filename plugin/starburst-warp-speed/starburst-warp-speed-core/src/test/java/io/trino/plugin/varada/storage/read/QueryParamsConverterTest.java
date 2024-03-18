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
package io.trino.plugin.varada.storage.read;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.MatchData;
import io.trino.plugin.varada.juffer.PredicateBufferInfo;
import io.trino.plugin.varada.juffer.PredicateCacheData;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.warp.gen.constants.MatchCollectOp;
import io.trino.plugin.warp.gen.constants.MatchNodeType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryParamsConverterTest
{
    @Test
    public void testConvertMatchDataListStructure()
    {
        LuceneQueryMatchData luceneQueryMatchData0 = createLuceneQueryMatchData("lucene0", 0);
        LuceneQueryMatchData luceneQueryMatchData1 = createLuceneQueryMatchData("lucene1", 1);
        LuceneQueryMatchData luceneQueryMatchData2 = createLuceneQueryMatchData("lucene2", 2);

        MatchData matchData =
                new LogicalMatchData(
                        LogicalMatchData.Operator.AND,
                        List.of(
                                luceneQueryMatchData0,
                                new LogicalMatchData(
                                        LogicalMatchData.Operator.OR,
                                        List.of(
                                                luceneQueryMatchData1,
                                                luceneQueryMatchData2))));

        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.getMatchData()).thenReturn(Optional.of(matchData));
        when(queryContext.getNativeQueryCollectDataList()).thenReturn(ImmutableList.of());
        when(queryContext.getTotalRecords()).thenReturn(100);
        QueryParams queryParams = QueryParamsConverter.createQueryParams(queryContext, "filePath");
        List<MatchNode> es = List.of(
                new LogicalMatchNode(MatchNodeType.MATCH_NODE_TYPE_AND,
                        List.of(
                                convertLuceneMatchDataToMatchParams(luceneQueryMatchData0, 0),
                                new LogicalMatchNode(
                                        MatchNodeType.MATCH_NODE_TYPE_OR,
                                        List.of(
                                                convertLuceneMatchDataToMatchParams(luceneQueryMatchData1, 1),
                                                convertLuceneMatchDataToMatchParams(luceneQueryMatchData2, 2))))));
        assertThat(queryParams.getRootMatchNode().get()).isEqualTo(es.get(0));
        assertThat(queryParams.getNumLucene()).isEqualTo(3);
    }

    private WarmupElementMatchParams convertLuceneMatchDataToMatchParams(LuceneQueryMatchData luceneQueryMatchData, int luceneIx)
    {
        return new WarmupElementMatchParams(
                luceneQueryMatchData.getWarmUpElement().getQueryOffset(),
                luceneQueryMatchData.getWarmUpElement().getQueryReadSize(),
                luceneQueryMatchData.getWarmUpElement().getWarmUpType(),
                luceneQueryMatchData.getWarmUpElement().getRecTypeCode(),
                luceneQueryMatchData.getWarmUpElement().getRecTypeLength(),
                luceneQueryMatchData.getWarmUpElement().getWarmEvents(),
                luceneQueryMatchData.getWarmUpElement().isImported(),
                luceneQueryMatchData.getPredicateCacheData().getPredicateBufferInfo().buff(),
                luceneQueryMatchData.getWarmUpElement().getWarmupElementStats().getNullsCount() > 0 && luceneQueryMatchData.isCollectNulls(),
                luceneQueryMatchData.isTightnessRequired(),
                -1,
                MatchCollectOp.MATCH_COLLECT_OP_INVALID,
                Optional.of(new WarmupElementLuceneParams(luceneQueryMatchData, luceneIx)));
    }

    private static LuceneQueryMatchData createLuceneQueryMatchData(String columnName, int weId)
    {
        PredicateCacheData predicateCacheData = mock(PredicateCacheData.class);
        PredicateBufferInfo predicateBufferInfo = mock(PredicateBufferInfo.class);
        when(predicateCacheData.getPredicateBufferInfo()).thenReturn(predicateBufferInfo);
        when(predicateBufferInfo.buff()).thenReturn(MemorySegment.NULL);

        return LuceneQueryMatchData.builder()
                .warmUpElement(WarmUpElement.builder()
                        .warmUpType(WarmUpType.WARM_UP_TYPE_LUCENE)
                        .recTypeCode(RecTypeCode.REC_TYPE_VARCHAR)
                        .recTypeLength(100 + weId)
                        .varadaColumn(new RegularColumn(columnName))
                        .warmupElementStats(new WarmupElementStats(1000 + weId, Long.MIN_VALUE, Long.MAX_VALUE))
                        .build())
                .predicateCacheData(predicateCacheData)
                .collectNulls(weId % 2 == 0)
                .tightnessRequired(weId % 3 == 0)
                .build();
    }
}
