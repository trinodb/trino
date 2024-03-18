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
import io.airlift.log.Logger;
import io.trino.plugin.varada.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.varada.dispatcher.query.MatchCollectUtils;
import io.trino.plugin.varada.dispatcher.query.MatchCollectUtils.MatchCollectType;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.collect.PrefilledQueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.collect.QueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.MatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.MatchDataSorter;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.connector.ColumnHandle;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyList;

// This classifier is responsible for 3 things (in this order):
// 1. Sort the matches by priority
// 2. Reduce the number of matches to the maximum allowed (currently its 128)
// 3. Allocate and assign a matchCollectId to all match collects if there are after the sort and reduce

class MatchPrepareAfterCollectClassifier
        implements Classifier
{
    private static final Logger logger = Logger.get(MatchPrepareAfterCollectClassifier.class);

    private final MatchCollectIdService matchCollectIdService;
    private final int maxMatchColumns;

    MatchPrepareAfterCollectClassifier(MatchCollectIdService matchCollectIdService, int maxMatchColumns)
    {
        this.matchCollectIdService = matchCollectIdService;
        this.maxMatchColumns = maxMatchColumns;
    }

    @Override
    public QueryContext classify(ClassifyArgs classifyArgs, QueryContext queryContext)
    {
        if (queryContext.getMatchData().isEmpty()) {
            return queryContext;
        }

        QueryContext.Builder contextBuilder = queryContext.asBuilder();
        List<MatchData> matchDatas = queryContext.getMatchData()
                .map(this::extractMatches)
                .orElse(emptyList());
        List<MatchData> sortedMatchDatas = MatchDataSorter.sortMatchDataByWarmupType(matchDatas, queryContext);

        Deque<NativeQueryCollectData> nativeQueryCollectDataQueue = new ArrayDeque<>(queryContext.getNativeQueryCollectDataList());
        Map<Integer, ColumnHandle> remainingCollectColumnByBlockIndex = new HashMap<>(queryContext.getRemainingCollectColumnByBlockIndex());
        int totalMatches = queryContext.getMatchLeavesDFS().size();
        if (totalMatches > maxMatchColumns) {
            // When sorting, match-collects are placed at the end of the list in order to collect as little as possible.
            // But when there are too many matches, we prune from the end of the list, so in order not to lose the match-collects,
            // we extract them here and place them at the end of the list after the pruning.
            Iterator<MatchData> iterator = sortedMatchDatas.iterator();
            List<MatchData> matchCollects = new ArrayList<>();
            while (iterator.hasNext()) {
                MatchData matchData = iterator.next();
                if (matchData instanceof QueryMatchData queryMatchData &&
                        queryMatchData.getWarmUpElement().getWarmUpType() == WarmUpType.WARM_UP_TYPE_BASIC &&
                        MatchCollectUtils.canBeMatchForMatchCollect(queryMatchData, queryContext.getNativeQueryCollectDataList())) {
                    iterator.remove();
                    matchCollects.add(matchData);
                }
            }
            if (matchCollects.size() > maxMatchColumns) {
                // edge case - if there are too many match-collects, place them at the beginning so all the non match-collects will be pruned
                sortedMatchDatas.addAll(0, matchCollects);
                matchCollects = emptyList();
            }

            Map<Integer, PrefilledQueryCollectData> prefilledQueryCollectDataByBlockIndex = new HashMap<>(queryContext.getPrefilledQueryCollectDataByBlockIndex());
            while (totalMatches > maxMatchColumns) {
                // TODO: Return the removed predicate back to queryContext.predicateContextData
                //  if there's no other tight QueryMatchData for the removed columns.
                //  For now, it's ok not to do it because we mark the queryContext as non-tight
                MatchData deletedMatchData = sortedMatchDatas.get(sortedMatchDatas.size() - 1);
                sortedMatchDatas = sortedMatchDatas.subList(0, sortedMatchDatas.size() - 1);

                List<QueryMatchData> removedMatchedData = deletedMatchData.getLeavesDFS();
                totalMatches -= removedMatchedData.size();

                // Remove from prefilled (we might have decided to prefill based on tightness which doesn't exist anymore)
                removedMatchedData.forEach(queryMatchData ->
                        prefilledQueryCollectDataByBlockIndex.values().stream()
                                .filter(prefilled -> queryMatchData.getVaradaColumn().equals(prefilled.getVaradaColumn()))
                                .map(QueryCollectData::getBlockIndex)
                                .toList()
                                .forEach(blockIndex -> {
                                    prefilledQueryCollectDataByBlockIndex.remove(blockIndex);
                                    remainingCollectColumnByBlockIndex.put(blockIndex, classifyArgs.getCollectColumn(blockIndex));
                                }));

                List<NativeQueryCollectData> correspondingMatchCollects = nativeQueryCollectDataQueue.stream()
                        .filter(collectData -> MatchCollectUtils.canStillBeMatchCollected(collectData, removedMatchedData))
                        .toList();
                for (NativeQueryCollectData nativeQueryCollectData : correspondingMatchCollects) {
                    nativeQueryCollectDataQueue.remove(nativeQueryCollectData);
                    handleRemovedMatchCollectElement(nativeQueryCollectData,
                            classifyArgs,
                            nativeQueryCollectDataQueue,
                            remainingCollectColumnByBlockIndex);
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("%s was removed from matchDataList since it exceed the maximum allowed size (%d)%s",
                            deletedMatchData,
                            maxMatchColumns,
                            correspondingMatchCollects.isEmpty() ? "" :
                                    String.format(". In addition, updated / removed the corresponding %s since they were match-collected using this MatchData", correspondingMatchCollects));
                }
            }
            sortedMatchDatas.addAll(matchCollects);

            contextBuilder.canBeTight(false)
                    .prefilledQueryCollectDataByBlockIndex(prefilledQueryCollectDataByBlockIndex);
        }

        Optional<MatchData> sortedMatchData = sortedMatchDatas.isEmpty() ?
                Optional.empty() :
                Optional.of(sortedMatchDatas.size() == 1 ?
                        sortedMatchDatas.get(0) :
                        new LogicalMatchData(LogicalMatchData.Operator.AND, sortedMatchDatas));

        int matchCollectId = updateMatchCollectIds(sortedMatchData, classifyArgs, nativeQueryCollectDataQueue, remainingCollectColumnByBlockIndex);
        return contextBuilder
                .matchData(sortedMatchData)
                .remainingCollectColumnByBlockIndex(remainingCollectColumnByBlockIndex)
                .nativeQueryCollectDataList(nativeQueryCollectDataQueue.stream().toList())
                .matchCollectId(matchCollectId)
                .build();
    }

    private List<MatchData> extractMatches(MatchData matchData)
    {
        ImmutableList.Builder<MatchData> resultBuilder = ImmutableList.builder();
        extractMatches(matchData, resultBuilder);
        return resultBuilder.build();
    }

    private void extractMatches(MatchData matchData, ImmutableList.Builder<MatchData> resultBuilder)
    {
        if (matchData instanceof LogicalMatchData logicalMatchData &&
                logicalMatchData.getOperator() == LogicalMatchData.Operator.AND) {
            for (MatchData term : logicalMatchData.getTerms()) {
                extractMatches(term, resultBuilder);
            }
        }
        else {
            resultBuilder.add(matchData);
        }
    }

    private int updateMatchCollectIds(Optional<MatchData> sortedMatchData,
            ClassifyArgs classifyArgs,
            Deque<NativeQueryCollectData> nativeQueryCollectDataQueue,
            Map<Integer, ColumnHandle> remainingCollectColumnByBlockIndex)
    {
        int matchCollectId = MatchCollectIdService.INVALID_ID;
        boolean failedMatchCollectIdAllocation = false;

        try {
            if (sortedMatchData.isPresent()) {
                List<NativeQueryCollectData> matchCollects = nativeQueryCollectDataQueue.stream()
                        .filter(collectData -> MatchCollectUtils.canStillBeMatchCollected(collectData, sortedMatchData.get().getLeavesDFS()))
                        .toList();
                if (matchCollects.size() > 0) {
                    matchCollectId = matchCollectIdService.allocMatchCollectId();
                    failedMatchCollectIdAllocation = (matchCollectId == MatchCollectIdService.INVALID_ID);
                }
                for (NativeQueryCollectData nativeQueryCollectData : matchCollects) {
                    nativeQueryCollectDataQueue.remove(nativeQueryCollectData);
                    if (failedMatchCollectIdAllocation) {
                        handleRemovedMatchCollectElement(nativeQueryCollectData,
                                classifyArgs,
                                nativeQueryCollectDataQueue,
                                remainingCollectColumnByBlockIndex);
                    }
                    else {
                        nativeQueryCollectDataQueue.add(nativeQueryCollectData.asBuilder()
                                .matchCollectId(matchCollectId)
                                .build());
                    }
                }
            }
        }
        catch (Exception e) {
            matchCollectIdService.freeMatchCollectId(matchCollectId);
            throw e;
        }
        return matchCollectId;
    }

    private void handleRemovedMatchCollectElement(NativeQueryCollectData nativeQueryCollectData,
            ClassifyArgs classifyArgs,
            Deque<NativeQueryCollectData> nativeQueryCollectDataQueue,
            Map<Integer, ColumnHandle> remainingCollectColumnByBlockIndex)
    {
        // Match collected columns have DATA \ BASIC warmUpElement (See NativeCollectClassifier#createMatchCollect)
        if (WarmUpType.WARM_UP_TYPE_DATA.equals(nativeQueryCollectData.getWarmUpElement().getWarmUpType())) {
            nativeQueryCollectDataQueue.add(nativeQueryCollectData.asBuilder()
                    .matchCollectType(MatchCollectType.DISABLED)
                    .build());
        }
        else {
            int blockIndex = nativeQueryCollectData.getBlockIndex();
            remainingCollectColumnByBlockIndex.put(blockIndex, classifyArgs.getCollectColumn(blockIndex));
        }
    }
}
