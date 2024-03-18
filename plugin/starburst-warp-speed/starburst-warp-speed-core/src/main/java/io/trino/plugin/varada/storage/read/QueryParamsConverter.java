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
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.MatchCollectUtils;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.MatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.juffer.PredicateCacheData;
import io.trino.plugin.warp.gen.constants.MatchCollectOp;
import io.trino.plugin.warp.gen.constants.MatchNodeType;
import io.trino.spi.block.Block;
import io.varada.tools.util.Pair;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.varada.dictionary.DictionaryCacheService.DICTIONARY_REC_TYPE_CODE;
import static io.trino.plugin.varada.dictionary.DictionaryCacheService.DICTIONARY_REC_TYPE_LENGTH;
import static io.trino.plugin.varada.dispatcher.query.MatchCollectUtils.findMatchForMatchCollect;
import static io.trino.plugin.varada.storage.read.VaradaPageSource.INVALID_COL_IX;

public class QueryParamsConverter
{
    private static final int MATCH = 0;
    private static final int COLLECT = 1;

    private QueryParamsConverter() {}

    public static QueryParams createQueryParams(QueryContext queryContext,
            String filePath)
    {
        Optional<MatchData> matchData = queryContext.getMatchData();
        ImmutableList.Builder<PredicateCacheData> predicateCacheDataBuilder = ImmutableList.builder();

        long lastUsedTimestamp = Instant.now().toEpochMilli();
        int[] minOffsets = new int[2];
        minOffsets[COLLECT] = Integer.MAX_VALUE;
        minOffsets[MATCH] = Integer.MAX_VALUE;

        // collect
        Pair<List<WarmupElementCollectParams>, List<MatchCollectElement>> collectAndMatchCollectParams = getCollectAndMatchCollectParams(matchData, queryContext.getNativeQueryCollectDataList(), lastUsedTimestamp, minOffsets);

        // match
        Pair<List<MatchNode>, Integer> termsToNumLucene;
        termsToNumLucene = matchData.map(data -> convertToMatchNodes(List.of(data),
                collectAndMatchCollectParams.getRight(),
                lastUsedTimestamp,
                0,
                predicateCacheDataBuilder,
                minOffsets)).orElseGet(() -> Pair.of(Collections.emptyList(), 0));
        Optional<MatchNode> rootMatchNode = createRootMatchNode(termsToNumLucene.getLeft());

        return new QueryParams(rootMatchNode,
                termsToNumLucene.getRight(),
                collectAndMatchCollectParams.getLeft(),
                queryContext.getTotalRecords(),
                queryContext.getCatalogSequence(),
                minOffsets[MATCH],
                minOffsets[COLLECT],
                filePath,
                predicateCacheDataBuilder.build());
    }

    private static Optional<MatchNode> createRootMatchNode(List<MatchNode> matchNodes)
    {
        Optional<MatchNode> rootMatchNode;
        if (matchNodes.isEmpty()) {
            rootMatchNode = Optional.empty();
        }
        else if (matchNodes.size() == 1 && matchNodes.get(0) instanceof LogicalMatchNode) {
            // root node must be logical
            rootMatchNode = Optional.of(matchNodes.get(0));
        }
        else {
            rootMatchNode = Optional.of(new LogicalMatchNode(MatchNodeType.MATCH_NODE_TYPE_AND, matchNodes));
        }
        return rootMatchNode;
    }

    private static Pair<List<MatchNode>, Integer> convertToMatchNodes(List<MatchData> matchDataList,
            List<MatchCollectElement> matchCollectElements,
            long lastUsedTimestamp,
            int currentNumLucene,
            ImmutableList.Builder<PredicateCacheData> predicateCacheDataBuilder,
            int[] minOffsets)
    {
        List<MatchNode> convertedList = new ArrayList<>(matchDataList.size());

        for (MatchData matchData : matchDataList) {
            if (matchData instanceof QueryMatchData queryMatchData) {
                WarmUpElement matchDataWarmUpElement = queryMatchData.getWarmUpElement();
                matchDataWarmUpElement.setLastUsedTimestamp(lastUsedTimestamp);
                if (minOffsets[MATCH] > matchDataWarmUpElement.getStartOffset()) {
                    minOffsets[MATCH] = matchDataWarmUpElement.getStartOffset();
                }

                Optional<WarmupElementLuceneParams> luceneParams;
                if (queryMatchData instanceof LuceneQueryMatchData) {
                    luceneParams = Optional.of(new WarmupElementLuceneParams((LuceneQueryMatchData) queryMatchData, currentNumLucene));
                    currentNumLucene++;
                }
                else {
                    luceneParams = Optional.empty();
                }

                Optional<MatchCollectElement> matchCollectElement = matchCollectElements.stream()
                        .filter(match -> match.getQueryMatchData().equals(queryMatchData))
                        .findFirst();
                int matchCollectIndex = matchCollectElement.map(MatchCollectElement::getMatchCollectIndex).orElse(INVALID_COL_IX);
                MatchCollectOp matchCollectOp = matchCollectElement.map(MatchCollectElement::getMatchCollectOp).orElse(MatchCollectOp.MATCH_COLLECT_OP_INVALID);

                convertedList.add(
                        new WarmupElementMatchParams(
                                matchDataWarmUpElement.getQueryOffset(),
                                matchDataWarmUpElement.getQueryReadSize(),
                                matchDataWarmUpElement.getWarmUpType(),
                                matchDataWarmUpElement.getRecTypeCode(),
                                matchDataWarmUpElement.getRecTypeLength(),
                                matchDataWarmUpElement.getWarmEvents(),
                                matchDataWarmUpElement.isImported(),
                                queryMatchData.getPredicateCacheData().getPredicateBufferInfo().buff(),
                                matchDataWarmUpElement.getWarmupElementStats().getNullsCount() > 0 && queryMatchData.isCollectNulls(),
                                queryMatchData.isTightnessRequired(),
                                matchCollectIndex,
                                matchCollectOp,
                                luceneParams));
                predicateCacheDataBuilder.add(queryMatchData.getPredicateCacheData());
            }
            else if (matchData instanceof LogicalMatchData logicalMatchData) {
                Pair<List<MatchNode>, Integer> termsToNumLucene = convertToMatchNodes(logicalMatchData.getTerms(), matchCollectElements, lastUsedTimestamp, currentNumLucene, predicateCacheDataBuilder, minOffsets);
                MatchNodeType nodeType = logicalMatchData.getOperator() == LogicalMatchData.Operator.AND ? MatchNodeType.MATCH_NODE_TYPE_AND : MatchNodeType.MATCH_NODE_TYPE_OR;
                convertedList.add(new LogicalMatchNode(nodeType, termsToNumLucene.getLeft()));
                currentNumLucene = termsToNumLucene.getRight();
            }
            else {
                throw new RuntimeException("Unknown MatchData type: " + matchData);
            }
        }
        return Pair.of(convertedList, currentNumLucene);
    }

    private static Pair<List<WarmupElementCollectParams>, List<MatchCollectElement>> getCollectAndMatchCollectParams(Optional<MatchData> matchData,
            ImmutableList<NativeQueryCollectData> nativeQueryCollectDataList,
            long lastUsedTimestamp,
            int[] minOffsets)
    {
        List<WarmupElementCollectParams> collectParamsList = new ArrayList<>();
        List<MatchCollectElement> matchCollectElements = new ArrayList<>();
        List<QueryMatchData> queryMatchDataLeaves = matchData.stream()
                .flatMap(queryMatchData -> queryMatchData.getLeavesDFS().stream())
                .toList();

        for (NativeQueryCollectData nativeQueryCollectData : nativeQueryCollectDataList) {
            WarmUpElement collectDataWarmUpElement = nativeQueryCollectData.getWarmUpElement();
            collectDataWarmUpElement.setLastUsedTimestamp(lastUsedTimestamp);
            if (minOffsets[COLLECT] > collectDataWarmUpElement.getStartOffset()) {
                minOffsets[COLLECT] = collectDataWarmUpElement.getStartOffset();
            }

            boolean isCollectNulls = collectDataWarmUpElement.getWarmupElementStats().getNullsCount() > 0;
            int blockIndex = nativeQueryCollectDataList.indexOf(nativeQueryCollectData);
            int matchCollectIndex = INVALID_COL_IX;
            int matchCollectId = nativeQueryCollectData.getMatchCollectId();
            Optional<Block> valuesDictBlock = Optional.empty();
            if (nativeQueryCollectData.getMatchCollectType() != MatchCollectUtils.MatchCollectType.DISABLED) {
                QueryMatchData queryMatchData = findMatchForMatchCollect(nativeQueryCollectData, queryMatchDataLeaves)
                        .orElseThrow(() -> new RuntimeException("Expected match-collect was not found"));
                isCollectNulls &= queryMatchData.getWarmUpElement().getWarmupElementStats().getNullsCount() > 0 && queryMatchData.isCollectNulls();
                matchCollectIndex = matchCollectElements.size();
                boolean mappedMatchCollect = nativeQueryCollectData.getMatchCollectType() == MatchCollectUtils.MatchCollectType.MAPPED;
                if (mappedMatchCollect) {
                    valuesDictBlock = queryMatchData.getPredicateCacheData().getValuesDict();
                }
                matchCollectElements.add(new MatchCollectElement(queryMatchData, collectDataWarmUpElement.getWarmUpType(), matchCollectIndex, mappedMatchCollect));
            }

            // prepare the dictionary key if required for retrieving dictionary record type length before tx create and loading
            // the dictionary after tx create
            if (nativeQueryCollectData.getWarmUpElement().isDictionaryUsed()) {
                // data values code and length - code is the same, length might be larger than warm up element
                WarmupElementDictionaryParams dictionaryParams = new WarmupElementDictionaryParams(
                        nativeQueryCollectData.getWarmUpElement().getDictionaryInfo().dictionaryKey(),
                        collectDataWarmUpElement.getUsedDictionarySize(),
                        collectDataWarmUpElement.getRecTypeCode(),
                        collectDataWarmUpElement.getDictionaryInfo().dataValuesRecTypeLength(),
                        collectDataWarmUpElement.getDictionaryInfo().dictionaryOffset());
                collectParamsList.add(
                        new WarmupElementCollectParams(
                                collectDataWarmUpElement.getQueryOffset(),
                                collectDataWarmUpElement.getQueryReadSize(),
                                collectDataWarmUpElement.getWarmUpType(),
                                // native should keep this warm up element with the dictionary code and length
                                DICTIONARY_REC_TYPE_CODE,
                                DICTIONARY_REC_TYPE_LENGTH,
                                // page block should hold the original code and length
                                collectDataWarmUpElement.getRecTypeCode(),
                                Math.min(collectDataWarmUpElement.getRecTypeLength(), dictionaryParams.dataValuesRecTypeLength()),
                                collectDataWarmUpElement.getWarmEvents(),
                                collectDataWarmUpElement.isImported(),
                                Optional.of(dictionaryParams),
                                blockIndex,
                                matchCollectIndex,
                                matchCollectId,
                                isCollectNulls,
                                valuesDictBlock));
            }
            else {
                collectParamsList.add(
                        new WarmupElementCollectParams(
                                collectDataWarmUpElement.getQueryOffset(),
                                collectDataWarmUpElement.getQueryReadSize(),
                                collectDataWarmUpElement.getWarmUpType(),
                                // native will use the original code the length
                                collectDataWarmUpElement.getRecTypeCode(),
                                collectDataWarmUpElement.getRecTypeLength(),
                                // page block should hold the original code and length
                                collectDataWarmUpElement.getRecTypeCode(),
                                collectDataWarmUpElement.getRecTypeLength(),
                                collectDataWarmUpElement.getWarmEvents(),
                                collectDataWarmUpElement.isImported(),
                                Optional.empty(), // no dictionary we put invalid
                                blockIndex,
                                matchCollectIndex,
                                matchCollectId,
                                isCollectNulls,
                                valuesDictBlock));
            }
        }
        return Pair.of(collectParamsList, matchCollectElements);
    }
}
