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
import io.trino.plugin.varada.dispatcher.query.MatchCollectUtils;
import io.trino.plugin.varada.dispatcher.query.PredicateData;
import io.trino.plugin.varada.dispatcher.query.PredicateInfo;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.data.collect.PrefilledQueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.match.BasicBloomQueryMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.MatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.juffer.PredicateCacheData;
import io.trino.plugin.varada.juffer.PredicatesCacheService;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.varada.tools.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.varada.dispatcher.query.classifier.PredicateUtil.calcPredicateData;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_ALL;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_LUCENE;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_NONE;
import static java.lang.String.format;

class PredicateBufferClassifier
        implements Classifier
{
    private static final Logger logger = Logger.get(PredicateBufferClassifier.class);

    private final PredicatesCacheService predicatesCacheService;
    private final PredefinedPredicate noneWithNulls;
    private final PredefinedPredicate noneWithoutNulls;
    private final PredefinedPredicate allWithNulls;
    private final PredefinedPredicate allWithoutNulls;
    private final PredefinedPredicate luceneWithNulls;
    private final PredefinedPredicate luceneWithoutNulls;

    PredicateBufferClassifier(PredicatesCacheService predicatesCacheService)
    {
        this.predicatesCacheService = predicatesCacheService;
        this.noneWithNulls = buildPredicateDataWithoutBuffer(PREDICATE_TYPE_NONE, true);
        this.noneWithoutNulls = buildPredicateDataWithoutBuffer(PREDICATE_TYPE_NONE, false);
        this.allWithNulls = buildPredicateDataWithoutBuffer(PREDICATE_TYPE_ALL, true);
        this.allWithoutNulls = buildPredicateDataWithoutBuffer(PREDICATE_TYPE_ALL, false);
        this.luceneWithNulls = buildPredicateDataWithoutBuffer(PREDICATE_TYPE_LUCENE, true);
        this.luceneWithoutNulls = buildPredicateDataWithoutBuffer(PREDICATE_TYPE_LUCENE, false);
    }

    @Override
    public QueryContext classify(ClassifyArgs classifyArgs, QueryContext queryContext)
    {
        Map<Integer, PrefilledQueryCollectData> newPrefilledQueryCollectDataByBlockIndex = new HashMap<>(queryContext.getPrefilledQueryCollectDataByBlockIndex());
        Map<Integer, ColumnHandle> newRemainingCollectColumnByBlockIndex = new HashMap<>(queryContext.getRemainingCollectColumnByBlockIndex());
        int columnIx = 0;
        Optional<MatchData> matchDataWithPredicateBuffers = Optional.empty();
        if (queryContext.getMatchData().isPresent()) {
            MatchData newMatchData = calcMatchDataWithLogical(classifyArgs,
                    queryContext.getMatchData().get(),
                    queryContext,
                    columnIx,
                    newRemainingCollectColumnByBlockIndex,
                    newPrefilledQueryCollectDataByBlockIndex);
            matchDataWithPredicateBuffers = Optional.of(newMatchData);
        }

        return queryContext.asBuilder()
                .matchData(matchDataWithPredicateBuffers)
                .prefilledQueryCollectDataByBlockIndex(newPrefilledQueryCollectDataByBlockIndex)
                .remainingCollectColumnByBlockIndex(newRemainingCollectColumnByBlockIndex)
                .build();
    }

    private QueryMatchData createPredicateBuffer(QueryContext queryContext,
            ClassifyArgs classifyArgs,
            QueryMatchData queryMatchData,
            Map<Integer, ColumnHandle> newRemainingCollectColumnByBlockIndex, Map<Integer, PrefilledQueryCollectData> newPrefilledQueryCollectDataByBlockIndex)
    {
        boolean simplifiedDomain = queryMatchData.isSimplifiedDomain();
        boolean tightnessRequired = queryMatchData.isTightnessRequired();
        boolean collectNulls;
        Pair<PredicateCacheData, Boolean> queryMatch;
        Type columnType = queryMatchData.getType();
        Optional<Domain> optionalDomain;
        PredicateCacheData predicateCacheData;
        Domain domain = queryMatchData.getDomain().orElse(Domain.all(queryMatchData.getType()));
        Optional<NativeExpression> nativeExpressionOptional = queryMatchData instanceof BasicBloomQueryMatchData ? Optional.of(((BasicBloomQueryMatchData) queryMatchData).getNativeExpression()) : Optional.empty();
        if (queryMatchData instanceof LuceneQueryMatchData) {
            PredefinedPredicate predefinedPredicate = queryMatchData.isCollectNulls() ? luceneWithNulls : luceneWithoutNulls;
            predicateCacheData = predefinedPredicate.predicateCacheData;
            collectNulls = predefinedPredicate.collectNulls;
        }
        else if (domain.getValues().isAll()) {
            PredefinedPredicate predefinedPredicate = domain.isNullAllowed() ? allWithNulls : allWithoutNulls;
            predicateCacheData = predefinedPredicate.predicateCacheData;
            collectNulls = predefinedPredicate.collectNulls;
        }
        else if (domain.getValues().isNone()) {
            PredefinedPredicate predefinedPredicate = domain.isNullAllowed() ? noneWithNulls : noneWithoutNulls;
            predicateCacheData = predefinedPredicate.predicateCacheData;
            collectNulls = predefinedPredicate.collectNulls;
        }
        else {
            WarmUpType warmUpType = queryMatchData.getWarmUpElement().getWarmUpType();
            boolean transformAllowed = warmUpType == WarmUpType.WARM_UP_TYPE_BASIC &&
                    !MatchCollectUtils.canBeMatchForMatchCollect(queryMatchData, queryContext.getNativeQueryCollectDataList());
            PredicateData predicateData;
            int recTypeLength = queryMatchData.getWarmUpElement().getRecTypeLength();
            if (nativeExpressionOptional.isPresent()) {
                domain = nativeExpressionOptional.get().domain();
                predicateData = calcPredicateData(nativeExpressionOptional.get(),
                        recTypeLength,
                        transformAllowed,
                        columnType);
            }
            else {
                predicateData = calcPredicateData(domain,
                        recTypeLength,
                        transformAllowed,
                        columnType);
            }
            collectNulls = predicateData.isCollectNulls();
            queryMatch = calculatePredicateBuffer(predicateData, queryMatchData, newPrefilledQueryCollectDataByBlockIndex, newRemainingCollectColumnByBlockIndex, classifyArgs, domain, queryContext, tightnessRequired, domain.isNullAllowed());
            if (!queryMatch.getRight()) {
                tightnessRequired = false;
                simplifiedDomain = true;  // A simplified domain will cause the entire queryContext to be marked with canBeTight = false
                collectNulls = domain.isNullAllowed();
            }
            predicateCacheData = queryMatch.getLeft();
        }
        optionalDomain = Optional.of(domain);
        return queryMatchData.asBuilder()
                .collectNulls(collectNulls)
                .predicateCacheData(predicateCacheData)
                .tightnessRequired(tightnessRequired)
                .simplifiedDomain(simplifiedDomain)
                .domain(optionalDomain)
                .build();
    }

    private Pair<PredicateCacheData, Boolean> calculatePredicateBuffer(
            PredicateData predicateData,
            QueryMatchData queryMatchData,
            Map<Integer, PrefilledQueryCollectData> newPrefilledQueryCollectDataByBlockIndex,
            Map<Integer, ColumnHandle> newRemainingCollectColumnByBlockIndex,
            ClassifyArgs classifyArgs,
            Object value,
            QueryContext queryContext,
            boolean tightnessRequired,
            boolean isNullAllowed)
    {
        PredicateCacheData predicateCacheData;
        boolean allocatedBuffer;
        Optional<PredicateCacheData> predicateCacheDataOpt = predicatesCacheService.getOrCreatePredicateBufferId(predicateData, value);
        if (predicateCacheDataOpt.isPresent()) {
            predicateCacheData = predicateCacheDataOpt.get();
            allocatedBuffer = true;
        }
        else {
            // no buffer is available. we fall back to predicate all and return a result to be filtered by upper layers
            PredefinedPredicate predefinedPredicate = isNullAllowed ? allWithNulls : allWithoutNulls;
            predicateCacheData = predefinedPredicate.predicateCacheData;
            if (tightnessRequired) {
                logger.debug("Tightness requirement can't be met because no predicate buffer is available. " +
                        "predicateData=%s, queryMatchData=%s, queryContext=%s",
                        predicateData, queryMatchData, queryContext);
                // Not tight anymore - remove prefill.
                // Theoretically, we can try to find another nativeQueryMatchData of the same column with canBeTight() = true,
                // but since we currently choose only one nativeQueryMatchData of type lucene \ basic per column, we'll actually need to create it.
                // Since this is an edge case and a better solution is not simple, just rollback the prefill creation - to avoid bugs.
                queryContext.getPrefilledQueryCollectDataByBlockIndex()
                        .entrySet().stream()
                        .filter(entry -> entry.getValue().getVaradaColumn().equals(queryMatchData.getVaradaColumn()))
                        .forEach(entry -> {
                            int blockIndex = entry.getKey();
                            newPrefilledQueryCollectDataByBlockIndex.remove(blockIndex);
                            newRemainingCollectColumnByBlockIndex.put(blockIndex, classifyArgs.getCollectColumn(blockIndex));
                        });
            }
            allocatedBuffer = false;
        }
        return Pair.of(predicateCacheData, allocatedBuffer);
    }

    private MatchData calcMatchDataWithLogical(ClassifyArgs classifyArgs,
            MatchData matchData,
            QueryContext queryContext,
            int columnIx,
            Map<Integer, ColumnHandle> newRemainingCollectColumnByBlockIndex,
            Map<Integer, PrefilledQueryCollectData> newPrefilledQueryCollectDataByBlockIndex)
    {
        MatchData res;
        if (matchData instanceof LogicalMatchData logicalMatchData) {
            List<MatchData> terms = logicalMatchData.getTerms();
            List<MatchData> newTerms = new ArrayList<>();
            for (MatchData term : terms) {
                MatchData newTerm = calcMatchDataWithLogical(classifyArgs,
                        term,
                        queryContext,
                        columnIx,
                        newRemainingCollectColumnByBlockIndex,
                        newPrefilledQueryCollectDataByBlockIndex);
                newTerms.add(newTerm);
                columnIx += newTerm.getLeavesDFS().size();
            }
            res = new LogicalMatchData(logicalMatchData.getOperator(), newTerms);
        }
        else if (matchData instanceof QueryMatchData queryMatchData) {
            res = createPredicateBuffer(queryContext,
                    classifyArgs,
                    queryMatchData,
                    newRemainingCollectColumnByBlockIndex,
                    newPrefilledQueryCollectDataByBlockIndex);
        }
        else {
            throw new UnsupportedOperationException(format("unsupported MatchData type %s", matchData));
        }
        return res;
    }

    private PredefinedPredicate buildPredicateDataWithoutBuffer(PredicateType predicateType, boolean isCollectNulls)
    {
        int numMatchElements = 0;
        PredicateInfo predicateInfo = new PredicateInfo(predicateType, FunctionType.FUNCTION_TYPE_NONE, numMatchElements, Collections.emptyList(), 0);

        PredicateData predicateData = PredicateData
                .builder()
                .isCollectNulls(isCollectNulls)
                .predicateInfo(predicateInfo)
                .predicateHashCode(0)
                .predicateSize(PredicateUtil.PREDICATE_HEADER_SIZE)
                .columnType(IntegerType.INTEGER) //fake - unused
                .build();
        PredicateCacheData predicateCacheData = predicatesCacheService.predicateDataToBuffer(predicateData, null).get();
        return new PredefinedPredicate(isCollectNulls, predicateCacheData);
    }

    private record PredefinedPredicate(
            @SuppressWarnings("unused") boolean collectNulls,
            @SuppressWarnings("unused") PredicateCacheData predicateCacheData) {}
}
