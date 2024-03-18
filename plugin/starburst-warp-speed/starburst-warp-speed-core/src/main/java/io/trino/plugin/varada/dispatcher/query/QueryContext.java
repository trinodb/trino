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
package io.trino.plugin.varada.dispatcher.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.varada.dispatcher.query.classifier.PredicateContextData;
import io.trino.plugin.varada.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.varada.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.collect.PrefilledQueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.collect.QueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.match.MatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.spi.connector.ColumnHandle;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.varada.dispatcher.query.classifier.QueryClassifier.INVALID_TOTAL_RECORDS;
import static java.lang.Math.toIntExact;

public class QueryContext
{
    private final Optional<MatchData> matchData;
    private final List<QueryMatchData> matchLeaves;
    private final ImmutableList<NativeQueryCollectData> nativeQueryCollectDataList;

    private final ImmutableMap<Integer, PrefilledQueryCollectData> prefilledQueryCollectDataByBlockIndex;
    private final ImmutableMap<Integer, ColumnHandle> remainingCollectColumnByBlockIndex;
    private final ImmutableList<ColumnHandle> remainingCollectColumns;

    private final int totalCollectCount;
    private final boolean enableMatchCollect;
    private final int matchCollectId;
    private final int catalogSequence;

    /**
     * means that predicate calculation result is None, will return EmptyPageSource
     */
    private final boolean isNone;
    private final PredicateContextData predicateContextData;
    private final boolean canBeTight;
    private final int totalRecords;

    public QueryContext(PredicateContextData predicateContextData,
            ImmutableList<ColumnHandle> remainingCollectColumns,
            int catalogSequence,
            boolean enableMatchCollect)
    {
        this(
                Optional.empty(),
                ImmutableList.of(),
                ImmutableMap.of(),
                predicateContextData,
                remainingCollectColumns,
                calculateRemainingCollectColumnByBlockIndex(remainingCollectColumns),
                enableMatchCollect,
                MatchCollectIdService.INVALID_ID,
                catalogSequence,
                false,
                predicateContextData.getRemainingColumns().isEmpty(),
                INVALID_TOTAL_RECORDS);
    }

    // used only for tests
    public QueryContext(PredicateContextData predicateContextData,
            ImmutableMap<Integer, ColumnHandle> remainingCollectColumnByBlockIndex)
    {
        this(
                Optional.empty(),
                ImmutableList.of(),
                ImmutableMap.of(),
                predicateContextData,
                calculateRemainingCollectColumns(remainingCollectColumnByBlockIndex),
                remainingCollectColumnByBlockIndex,
                true,
                MatchCollectIdService.INVALID_ID,
                0,
                false,
                predicateContextData.getRemainingColumns().isEmpty(),
                INVALID_TOTAL_RECORDS);
    }

    private QueryContext(Optional<MatchData> matchData,
            ImmutableList<NativeQueryCollectData> nativeQueryCollectDataList,
            ImmutableMap<Integer, PrefilledQueryCollectData> prefilledQueryCollectDataByBlockIndex,
            PredicateContextData predicateContextData,
            ImmutableList<ColumnHandle> remainingCollectColumns,
            ImmutableMap<Integer, ColumnHandle> remainingCollectColumnByBlockIndex,
            boolean enableMatchCollect,
            int matchCollectId,
            int catalogSequence,
            boolean isNone,
            boolean canBeTight,
            int totalRecords)
    {
        this.matchData = matchData;
        this.matchLeaves = matchData.isPresent() ? matchData.get().getLeavesDFS() : Collections.emptyList();
        this.nativeQueryCollectDataList = nativeQueryCollectDataList;
        this.prefilledQueryCollectDataByBlockIndex = prefilledQueryCollectDataByBlockIndex;
        this.predicateContextData = predicateContextData;
        this.remainingCollectColumns = remainingCollectColumns;
        this.remainingCollectColumnByBlockIndex = remainingCollectColumnByBlockIndex;
        this.totalCollectCount = toIntExact(getQueryCollectDataStream().count()) + remainingCollectColumnByBlockIndex.size();
        this.enableMatchCollect = enableMatchCollect;
        this.matchCollectId = matchCollectId;
        this.catalogSequence = catalogSequence;
        this.isNone = isNone;
        this.totalRecords = totalRecords;

        // TODO: It would be more accurate to check if there's at least one tight queryMatchData per predicate,
        //  but at this point, we don't know if 2 queryMatchDatas on the same column represent
        //  the same predicate (for example BLOOM + BASIC) or not (for example domain + expression)
        //  so until we improve this, a non-tight queryMatchData (BLOOM) causes the entire query to be marked as canBeTight=false
        this.canBeTight = isNone || (canBeTight && matchLeaves.stream().allMatch(QueryMatchData::canBeTight) && predicateContextData.getLeaves().isEmpty());
    }

    private static ImmutableMap<Integer, ColumnHandle> calculateRemainingCollectColumnByBlockIndex(ImmutableList<ColumnHandle> remainingCollectColumns)
    {
        return IntStream.range(0, remainingCollectColumns.size())
                .boxed()
                .collect(toImmutableMap(Function.identity(), remainingCollectColumns::get));
    }

    private static ImmutableList<ColumnHandle> calculateRemainingCollectColumns(Map<Integer, ColumnHandle> remainingCollectColumnByBlockIndex)
    {
        return ImmutableList.copyOf(remainingCollectColumnByBlockIndex.entrySet()
                .stream()
                .sorted(Comparator.comparingInt(Map.Entry::getKey))
                .map(Map.Entry::getValue).toList());
    }

    public Stream<QueryCollectData> getQueryCollectDataStream()
    {
        return Stream.of(nativeQueryCollectDataList.stream(),
                        prefilledQueryCollectDataByBlockIndex.values().stream())
                .flatMap(Function.identity());
    }

    public int getTotalRecords()
    {
        return totalRecords;
    }

    public Optional<MatchData> getMatchData()
    {
        return matchData;
    }

    public List<QueryMatchData> getMatchLeavesDFS()
    {
        return matchLeaves;
    }

    public ImmutableList<NativeQueryCollectData> getNativeQueryCollectDataList()
    {
        return nativeQueryCollectDataList;
    }

    public ImmutableMap<Integer, PrefilledQueryCollectData> getPrefilledQueryCollectDataByBlockIndex()
    {
        return prefilledQueryCollectDataByBlockIndex;
    }

    public ImmutableMap<Integer, ColumnHandle> getRemainingCollectColumnByBlockIndex()
    {
        return remainingCollectColumnByBlockIndex;
    }

    public ImmutableList<ColumnHandle> getRemainingCollectColumns()
    {
        return remainingCollectColumns;
    }

    public PredicateContextData getPredicateContextData()
    {
        return predicateContextData;
    }

    public boolean getEnableMatchCollect()
    {
        return enableMatchCollect;
    }

    public int getTotalCollectCount()
    {
        return totalCollectCount;
    }

    public int getMatchCollectId()
    {
        return matchCollectId;
    }

    public int getCatalogSequence()
    {
        return catalogSequence;
    }

    public boolean isProxyOnly()
    {
        return getTotalRecords() == INVALID_TOTAL_RECORDS ||
                (getNativeQueryCollectDataList().isEmpty() && getMatchData().isEmpty() && getPrefilledQueryCollectDataByBlockIndex().isEmpty());
    }

    public boolean isVaradaOnly()
    {
        return getRemainingCollectColumnByBlockIndex().isEmpty() &&
                (!getNativeQueryCollectDataList().isEmpty() || getMatchData().isPresent());
    }

    public boolean isNoneOnly()
    {
        return isNone;
    }

    public boolean isCanBeTight()
    {
        return canBeTight;
    }

    public boolean isPrefilledOnly()
    {
        return getRemainingCollectColumnByBlockIndex().isEmpty() &&
                getNativeQueryCollectDataList().isEmpty() &&
                getMatchData().isEmpty() &&
                getTotalRecords() != QueryClassifier.INVALID_TOTAL_RECORDS;
    }

    public Builder asBuilder()
    {
        return new Builder().matchData(matchData)
                .nativeQueryCollectDataList(nativeQueryCollectDataList)
                .prefilledQueryCollectDataByBlockIndex(prefilledQueryCollectDataByBlockIndex)
                .predicateContextData(predicateContextData)
                .remainingCollectColumnByBlockIndex(remainingCollectColumnByBlockIndex)
                .enableMatchCollect(enableMatchCollect)
                .matchCollectId(matchCollectId)
                .catalogSequence(catalogSequence)
                .totalRecords(totalRecords)
                .canBeTight(canBeTight);
    }

    @Override
    public String toString()
    {
        return "QueryContext{" +
                "matchData=" + matchData.toString() +
                ", nativeQueryCollectDataList=" + nativeQueryCollectDataList.stream().map(NativeQueryCollectData::toString).collect(Collectors.joining(", ")) +
                ", prefilledQueryCollectDataByBlockIndex=" + prefilledQueryCollectDataByBlockIndex.entrySet().stream().map(entry -> entry.getKey() + " : " + entry.getValue()).collect(Collectors.joining(", ")) +
                ", predicateContextData=" + predicateContextData +
                ", remainingCollectColumnByBlockIndex=" + remainingCollectColumnByBlockIndex.entrySet().stream().map(entry -> entry.getKey() + " : " + entry.getValue()).collect(Collectors.joining(", ")) +
                ", nativeQueryCollectDataList.size=" + nativeQueryCollectDataList.size() +
                ", prefilledQueryCollectDataByBlockIndex.size=" + prefilledQueryCollectDataByBlockIndex.size() +
                ", remainingCollectColumnByBlockIndex.size=" + remainingCollectColumnByBlockIndex.size() +
                ", enableMatchCollect=" + enableMatchCollect +
                ", matchLeaves.size=" + matchData.map(data -> data.getLeavesDFS().size()).orElse(0) +
                ", matchCollectId=" + matchCollectId +
                ", catalogSequence=" + catalogSequence +
                ", totalRecords=" + totalRecords +
                ", canBeTight=" + canBeTight +
                '}';
    }

    public static class Builder
    {
        private Optional<MatchData> matchData = Optional.empty();
        private List<NativeQueryCollectData> nativeQueryCollectDataList = List.of();

        private Map<Integer, PrefilledQueryCollectData> prefilledQueryCollectDataByBlockIndex = Map.of();
        private PredicateContextData predicateContextData = new PredicateContextData(ImmutableMap.of(), VaradaPrimitiveConstant.TRUE);
        private Map<Integer, ColumnHandle> remainingCollectColumnByBlockIndex = Map.of();

        private boolean enableMatchCollect;
        private int matchCollectId;
        private int catalogSequence;
        private boolean isNone;
        private boolean canBeTight;
        private int totalRecords;

        private Builder()
        {
            this.matchCollectId = MatchCollectIdService.INVALID_ID; // initialize as invalid, might be set later
        }

        public QueryContext build()
        {
            return new QueryContext(matchData,
                    ImmutableList.copyOf(nativeQueryCollectDataList),
                    ImmutableMap.copyOf(prefilledQueryCollectDataByBlockIndex),
                    predicateContextData,
                    calculateRemainingCollectColumns(remainingCollectColumnByBlockIndex),
                    ImmutableMap.copyOf(remainingCollectColumnByBlockIndex),
                    enableMatchCollect,
                    matchCollectId,
                    catalogSequence,
                    isNone,
                    canBeTight,
                    totalRecords);
        }

        public Builder matchData(Optional<MatchData> matchData)
        {
            this.matchData = matchData;
            return this;
        }

        public Builder nativeQueryCollectDataList(List<NativeQueryCollectData> nativeQueryCollectDataList)
        {
            this.nativeQueryCollectDataList = nativeQueryCollectDataList;
            return this;
        }

        public Builder prefilledQueryCollectDataByBlockIndex(Map<Integer, PrefilledQueryCollectData> prefilledQueryCollectDataByBlockIndex)
        {
            this.prefilledQueryCollectDataByBlockIndex = prefilledQueryCollectDataByBlockIndex;
            return this;
        }

        public Builder predicateContextData(PredicateContextData predicateContextData)
        {
            this.predicateContextData = predicateContextData;
            return this;
        }

        public Builder remainingCollectColumnByBlockIndex(Map<Integer, ColumnHandle> remainingCollectColumnByBlockIndex)
        {
            this.remainingCollectColumnByBlockIndex = remainingCollectColumnByBlockIndex;
            return this;
        }

        public Builder enableMatchCollect(boolean enableMatchCollect)
        {
            this.enableMatchCollect = enableMatchCollect;
            return this;
        }

        public Builder isNone(boolean isNone)
        {
            this.isNone = isNone;
            return this;
        }

        public Builder canBeTight(boolean canBeTight)
        {
            this.canBeTight = canBeTight;
            return this;
        }

        public Builder matchCollectId(int matchCollectId)
        {
            this.matchCollectId = matchCollectId;
            return this;
        }

        public Builder totalRecords(int totalRecords)
        {
            this.totalRecords = totalRecords;
            return this;
        }

        public Builder catalogSequence(int catalogSequence)
        {
            this.catalogSequence = catalogSequence;
            return this;
        }
    }
}
