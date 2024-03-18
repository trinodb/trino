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

import io.airlift.log.Logger;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.MatchCollectUtils.MatchCollectType;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class NativeCollectClassifier
        implements Classifier
{
    private static final Logger logger = Logger.get(NativeCollectClassifier.class);

    private final int matchCollectBufferSize;
    private final int matchCollectRecSizeFactor;
    private final int collectRecordBufferMaxMemory;
    private final int collectTxMaxMemoryConfig;
    private final int matchTxSize;
    private final int maxMatchColumns;
    private final BufferAllocator bufferAllocator;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;

    // collectRecordBufferMaxMemory is the maximal memory we can use from the bundle memory for all collect columns record buffers
    // collectTxMaxMemory is the maximal memory we can use for native tx for all collect colulmns
    NativeCollectClassifier(int matchCollectBufferSize,
            int matchCollectRecSizeFactor,
            int collectRecordBufferMaxMemory,
            int collectTxMaxMemory,
            int matchTxSize,
            int maxMatchColumns,
            BufferAllocator bufferAllocator,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        this.matchCollectBufferSize = matchCollectBufferSize;
        this.matchCollectRecSizeFactor = matchCollectRecSizeFactor;
        this.collectRecordBufferMaxMemory = collectRecordBufferMaxMemory;
        this.collectTxMaxMemoryConfig = collectTxMaxMemory;
        this.matchTxSize = matchTxSize;
        this.maxMatchColumns = maxMatchColumns;
        this.bufferAllocator = bufferAllocator;
        this.dispatcherProxiedConnectorTransformer = dispatcherProxiedConnectorTransformer;
    }

    @Override
    public QueryContext classify(ClassifyArgs classifyArgs, QueryContext queryContext)
    {
        if (queryContext.getRemainingCollectColumnByBlockIndex().isEmpty()) {
            return queryContext;
        }

        final int remainingTxMemoryFromMatch =
                (maxMatchColumns - Math.min(queryContext.getMatchLeavesDFS().size(), maxMatchColumns)) * matchTxSize;
        final int collectTxMaxMemory = this.collectTxMaxMemoryConfig + remainingTxMemoryFromMatch;
        final int matchCollectBufferSize = queryContext.getEnableMatchCollect() ? this.matchCollectBufferSize : 0;
        NativeCollectState state = new NativeCollectState(classifyArgs, collectTxMaxMemory, matchCollectBufferSize);

        Map<Integer, ColumnHandle> remainingCollectColumnByBlockIndex = new HashMap<>(queryContext.getRemainingCollectColumnByBlockIndex());
        Map<CollectCategory, List<NativeQueryCollectData>> nativeQueryCollectDataListsByCategory = new HashMap<>();

        for (Map.Entry<Integer, ColumnHandle> entry : queryContext.getRemainingCollectColumnByBlockIndex().entrySet()) {
            if (!state.isCollectMemoryAvailable()) {
                break;
            }

            state.setCurrentColumn(entry.getValue(), entry.getKey());
            Optional<NativeQueryCollectData> collectOptional = Optional.empty();

            // try match collect first
            Optional<QueryMatchData> potentialMatchForMatchCollect = queryContext.getMatchLeavesDFS().stream()
                    .filter(queryMatchData -> !queryMatchData.isPartOfLogicalOr() && queryMatchData.canMatchCollect(state.getCurrentColumn()))
                    .findFirst();
            if (potentialMatchForMatchCollect.isPresent()) {
                collectOptional = createMatchCollect(classifyArgs, state, potentialMatchForMatchCollect.get());
            }

            // try data warmup
            if (collectOptional.isEmpty() && state.isCurrentColumnCollectWarmedForData()) {
                collectOptional = createCollect(state);
            }

            collectOptional.ifPresent(collectData -> nativeQueryCollectDataListsByCategory.computeIfAbsent(state.getCurrentCollectCategory(), c -> new ArrayList<>()).add(collectData));
        }

        return queryContext.asBuilder()
                .nativeQueryCollectDataList(createMemoryLimitedNativeQueryCollectDataList(queryContext,
                        nativeQueryCollectDataListsByCategory,
                        remainingCollectColumnByBlockIndex,
                        collectTxMaxMemory))
                .remainingCollectColumnByBlockIndex(remainingCollectColumnByBlockIndex)
                .build();
    }

    // assuming collect memory is avaialble - already checked in the calling loop
    private Optional<NativeQueryCollectData> createCollect(NativeCollectState state)
    {
        WarmUpElement warmUpElement = state.getCurrentColumnDataWarmUpElement();
        final int collectBufferSize = getCollectBufferSize(warmUpElement); // collectBufferSize is always positive
        final int collectTxSize = getCollectTxSize(warmUpElement);
        if (state.updateCollectMemoryIfAvailable(collectBufferSize, collectTxSize)) {
            return Optional.of(NativeQueryCollectData.builder()
                    .warmUpElement(warmUpElement)
                    .type(state.getCurrentColumnType())
                    .blockIndex(state.getCurrentBlockIndex())
                    .matchCollectType(MatchCollectType.DISABLED)
                    .build());
        }
        return Optional.empty();
    }

    // note that we have two types of memory to check here:
    // 1. collect memory coming from the bundle that is used to pass the collected records and nulls to java from native
    // 2. match collect memory which is a native intermediate buffer used for collecting the data while traversing the index
    private Optional<NativeQueryCollectData> createMatchCollect(ClassifyArgs classifyArgs,
            NativeCollectState state,
            QueryMatchData queryMatchData)
    {
        if (!state.isMatchCollectMemoryAvailable() || TypeUtils.isStrType(state.getCurrentColumnType())) {
            return Optional.empty();
        }

        boolean canMapMatchCollect = classifyArgs.isMappedMatchCollect() && queryMatchData.canMapMatchCollect();

        // we prefer to take the data element for the match collect if exists for better storage engine performance
        WarmUpElement warmUpElement = state.getCurrentColumnDataWarmUpElement();
        boolean useMatchElement = canMapMatchCollect || (warmUpElement == null);

        if (!useMatchElement) {
            // If we got a DATA element and this is a range predicate, a regular collect is more efficient than a match-collect.
            // At this stage, queryMatchData's PredicateType is still not set (will be calculated later on at PredicateBufferClassifier).
            // There is a delicate relation between choosing PREDICATE_TYPE_INVERSE_VALUES over PREDICATE_TYPE_RANGES and match-collect because if we match-collect we can't inverse.
            // So here we check if the PredicateType could be PREDICATE_TYPE_RANGES, and if so decide not to match-collect. Then, at PredicateBufferClassifier,
            // we might choose PREDICATE_TYPE_INVERSE_VALUES (because transformAllowed will be true).
            Optional<Domain> domain = queryMatchData.getDomain();
            if (domain.isPresent()) {
                PredicateType predicateType = classifyArgs.getPredicateTypeFromCache(domain.get(), queryMatchData.getType());
                if (PredicateType.PREDICATE_TYPE_RANGES.equals(predicateType)) {
                    return Optional.empty();
                }
            }

            // cannot collect using data element in case of a dictionary, because it holds encoded values and not real values, so we use the match elements
            useMatchElement = warmUpElement.isDictionaryUsed();
        }
        // if we decided to use the match element, we take it
        if (useMatchElement) {
            warmUpElement = queryMatchData.getWarmUpElement();
            if (warmUpElement.isDictionaryUsed()) {
                canMapMatchCollect = false;
            }
        }

        // check feasibility in terms of memory
        final int collectBufferSize = getCollectBufferSize(warmUpElement); // collectBufferSize is always positive
        final int collectTxSize = getCollectTxSize(warmUpElement);
        final int matchCollectBufferSize = getMatchCollectBufferSize(warmUpElement, canMapMatchCollect) * matchCollectRecSizeFactor;
        if ((matchCollectBufferSize > 0) && state.updateCollectMemoryIfAvailable(collectBufferSize, collectTxSize, matchCollectBufferSize)) {
            return Optional.of(NativeQueryCollectData.builder()
                    .warmUpElement(warmUpElement)
                    .type(state.getCurrentColumnType())
                    .blockIndex(state.getCurrentBlockIndex())
                    .matchCollectType(canMapMatchCollect ? MatchCollectType.MAPPED : MatchCollectType.ORDINARY)
                    .build());
        }
        return Optional.empty();
    }

    private List<NativeQueryCollectData> createMemoryLimitedNativeQueryCollectDataList(
            QueryContext queryContext,
            Map<CollectCategory, List<NativeQueryCollectData>> nativeQueryCollectDataListsByCategory,
            Map<Integer, ColumnHandle> remainingCollectColumnByBlockIndex,
            int collectTxMaxMemory)
    {
        int collectRecordBufferMemory = 0;
        int collectTxMemory = 0;
        List<NativeQueryCollectData> nativeQueryCollectDataList = new ArrayList<>();

        // start with those who are already in the list of the context
        for (NativeQueryCollectData nativeQueryCollectData : queryContext.getNativeQueryCollectDataList()) {
            collectRecordBufferMemory += getCollectBufferSize(nativeQueryCollectData.getWarmUpElement());
            if (collectRecordBufferMemory > collectRecordBufferMaxMemory) {
                return nativeQueryCollectDataList;
            }
            collectTxMemory += getCollectTxSize(nativeQueryCollectData.getWarmUpElement());
            if (collectTxMemory > collectTxMaxMemory) {
                return nativeQueryCollectDataList;
            }
            nativeQueryCollectDataList.add(nativeQueryCollectData);
            remainingCollectColumnByBlockIndex.remove(nativeQueryCollectData.getBlockIndex());
        }

        // now go from the most important category to the least important one. stop when memory is exhausted
        for (CollectCategory collectCategory : CollectCategory.values()) {
            List<NativeQueryCollectData> nativeQueryCollectDataCategoryList =
                    nativeQueryCollectDataListsByCategory.computeIfAbsent(collectCategory, c -> new ArrayList<>());

            // if we reached the string list we need to sort it since we want to take shorter string first for better performance
            // we delay the sort as much as possible in order to avoid it if not required eventually since no string has been taken
            if ((collectCategory == CollectCategory.STRING) && (nativeQueryCollectDataCategoryList.size() > 0)) {
                Collections.sort(nativeQueryCollectDataCategoryList);
                logger.debug("sorted collect strings list %s", nativeQueryCollectDataCategoryList);
            }

            for (NativeQueryCollectData nativeQueryCollectData : nativeQueryCollectDataCategoryList) {
                collectRecordBufferMemory += getCollectBufferSize(nativeQueryCollectData.getWarmUpElement());
                if (collectRecordBufferMemory > collectRecordBufferMaxMemory) {
                    return nativeQueryCollectDataList;
                }
                collectTxMemory += getCollectTxSize(nativeQueryCollectData.getWarmUpElement());
                if (collectTxMemory > collectTxMaxMemory) {
                    return nativeQueryCollectDataList;
                }
                nativeQueryCollectDataList.add(nativeQueryCollectData);
                remainingCollectColumnByBlockIndex.remove(nativeQueryCollectData.getBlockIndex());
            }
        }

        return nativeQueryCollectDataList;
    }

    private int getMatchCollectBufferSize(WarmUpElement warmUpElement, boolean mappedMatchCollect)
    {
        return mappedMatchCollect ? bufferAllocator.getMappedMatchCollectBufferSize() : bufferAllocator.getMatchCollectRecordBufferSize(warmUpElement.getRecTypeLength());
    }

    private int getCollectBufferSize(WarmUpElement warmUpElement)
    {
        return bufferAllocator.getCollectRecordBufferSize(warmUpElement.getRecTypeCode(), warmUpElement.getRecTypeLength()) +
                bufferAllocator.getQueryNullBufferSize(warmUpElement.getRecTypeCode());
    }

    private int getCollectTxSize(WarmUpElement warmUpElement)
    {
        return bufferAllocator.getCollectTxSize(warmUpElement.getRecTypeCode(), warmUpElement.getRecTypeLength());
    }

    enum CollectCategory
    {
        MATCH_COLLECT,
        FIXED_SIZE, // without chars that are covered by string
        STRING
    }

    private class NativeCollectState
    {
        private final Map<VaradaColumn, WarmUpElement> collectColumnToWarmElement;
        private final int collectTxMaxMemory;
        private final EnumMap<CollectCategory, Integer> collectRecordBufferMemoryPerCategory;
        private final EnumMap<CollectCategory, Integer> collectTxMemoryPerCategory;
        private final int matchCollectBufferSize;
        private RegularColumn currentColumn;
        private Type currentColumnType;
        private int currentBlockIndex;
        private CollectCategory currentCollectCategory;
        private int matchCollectMemory;

        NativeCollectState(ClassifyArgs classifyArgs, int collectTxMaxMemory, int matchCollectBufferSize)
        {
            this.collectColumnToWarmElement = classifyArgs.getWarmedWarmupTypes().dataWarmedElements();
            this.collectTxMaxMemory = collectTxMaxMemory;
            this.currentCollectCategory = CollectCategory.MATCH_COLLECT; // default
            this.matchCollectBufferSize = matchCollectBufferSize;
            this.matchCollectMemory = 0;

            // create map for memory counter per category and initialize it to zero
            this.collectRecordBufferMemoryPerCategory = new EnumMap(CollectCategory.class);
            this.collectTxMemoryPerCategory = new EnumMap(CollectCategory.class);
            for (CollectCategory collectCategory : CollectCategory.values()) {
                collectRecordBufferMemoryPerCategory.put(collectCategory, 0);
                collectTxMemoryPerCategory.put(collectCategory, 0);
            }
        }

        void setCurrentColumn(ColumnHandle columnHandle, int blockIndex)
        {
            currentBlockIndex = blockIndex;
            currentColumn = dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(columnHandle);
            currentColumnType = dispatcherProxiedConnectorTransformer.getColumnType(columnHandle);
        }

        RegularColumn getCurrentColumn()
        {
            return currentColumn;
        }

        WarmUpElement getCurrentColumnDataWarmUpElement()
        {
            return collectColumnToWarmElement.get(currentColumn);
        }

        boolean isCurrentColumnCollectWarmedForData()
        {
            return collectColumnToWarmElement.containsKey(currentColumn);
        }

        Type getCurrentColumnType()
        {
            return currentColumnType;
        }

        int getCurrentBlockIndex()
        {
            return currentBlockIndex;
        }

        CollectCategory getCurrentCollectCategory()
        {
            return currentCollectCategory;
        }

        boolean updateCollectMemoryIfAvailable(int collectBufferUpdate, int collectTxUpdate)
        {
            final boolean isStr = TypeUtils.isStrType(getCurrentColumnType());
            return updateCollectMemoryIfAvailable(collectBufferUpdate,
                    collectTxUpdate,
                    0,
                    isStr ? CollectCategory.STRING : CollectCategory.FIXED_SIZE);
        }

        boolean updateCollectMemoryIfAvailable(int collectBufferUpdate, int collectTxUpdate, int matchCollectBufferUpdate)
        {
            return updateCollectMemoryIfAvailable(collectBufferUpdate,
                    collectTxUpdate,
                    matchCollectBufferUpdate,
                    CollectCategory.MATCH_COLLECT);
        }

        private boolean updateCollectMemoryIfAvailable(int collectBufferUpdate,
                int collectTxUpdate,
                int matchCollectBufferUpdate,
                CollectCategory collectCategory)
        {
            // check if the aggregated memory for this category can take the update as well as the match collect memory
            final int updatedCollectRecordBufferMemory = collectRecordBufferMemoryPerCategory.get(collectCategory) + collectBufferUpdate;
            final int updatedCollectTxMemory = collectTxMemoryPerCategory.get(collectCategory) + collectTxUpdate;
            final int updatedMatchCollectMemory = matchCollectMemory + matchCollectBufferUpdate;
            if ((updatedCollectRecordBufferMemory > collectRecordBufferMaxMemory) ||
                    (updatedMatchCollectMemory > matchCollectBufferSize) ||
                    (updatedCollectTxMemory > collectTxMaxMemory)) {
                return false;
            }

            // update this category and the match collect
            collectRecordBufferMemoryPerCategory.put(collectCategory, updatedCollectRecordBufferMemory);
            collectTxMemoryPerCategory.put(collectCategory, updatedCollectTxMemory);
            matchCollectMemory = updatedMatchCollectMemory;

            // force update the following categories without checking limit
            Arrays.stream(CollectCategory.values())
                    .filter(c -> c.compareTo(collectCategory) > 0)
                    .forEach(c -> collectRecordBufferMemoryPerCategory.put(c, collectRecordBufferMemoryPerCategory.get(c) + collectBufferUpdate));
            Arrays.stream(CollectCategory.values())
                    .filter(c -> c.compareTo(collectCategory) > 0)
                    .forEach(c -> collectTxMemoryPerCategory.put(c, collectTxMemoryPerCategory.get(c) + collectTxUpdate));

            currentCollectCategory = collectCategory;
            return true;
        }

        // if the first category (which is the most important one) still have "room" to take more columns we must continue to look for more
        // collect elements (match collect in this case). we use this method to know when to stop the main loop
        boolean isCollectMemoryAvailable()
        {
            return (collectRecordBufferMemoryPerCategory.get(CollectCategory.MATCH_COLLECT) < collectRecordBufferMaxMemory) &&
                    (collectTxMemoryPerCategory.get(CollectCategory.MATCH_COLLECT) < collectTxMaxMemory);
        }

        boolean isMatchCollectMemoryAvailable()
        {
            return (matchCollectMemory < matchCollectBufferSize);
        }
    }
}
