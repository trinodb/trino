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
import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.SingleValue;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.PredicateContext;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.data.collect.PrefilledQueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.MatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.expression.DomainExpression;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import io.varada.log.ShapingLogger;
import io.varada.tools.util.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

class PrefilledCollectClassifier
        implements Classifier
{
    private static final Logger logger = Logger.get(PrefilledCollectClassifier.class);
    private final ShapingLogger shapingLogger;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;

    public PrefilledCollectClassifier(
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            GlobalConfiguration globalConfiguration)
    {
        this.dispatcherProxiedConnectorTransformer = dispatcherProxiedConnectorTransformer;
        shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfiguration.getShapingLoggerThreshold(),
                globalConfiguration.getShapingLoggerDuration(),
                globalConfiguration.getShapingLoggerNumberOfSamples());
    }

    @Override
    public QueryContext classify(ClassifyArgs classifyArgs, QueryContext queryContext)
    {
        if (queryContext.getRemainingCollectColumnByBlockIndex().isEmpty()) {
            return queryContext;
        }

        try {
            Map<VaradaColumn, Pair<Optional<QueryMatchData>, SingleValue>> prefilledCollectColumnValues =
                    getPrefilledCollectColumnValues(classifyArgs, queryContext);
            Map<VaradaColumn, String> partitionKeys = classifyArgs.getRowGroupData().getPartitionKeys();

            Map<Integer, PrefilledQueryCollectData> prefilledQueryCollectDataByBlockIndex = new HashMap<>(queryContext.getPrefilledQueryCollectDataByBlockIndex());
            Map<Integer, ColumnHandle> remainingCollectColumnByBlockIndex = new HashMap<>(queryContext.getRemainingCollectColumnByBlockIndex());
            ImmutableMap<VaradaColumn, WarmUpElement> dataElementsMap = classifyArgs.getWarmedWarmupTypes().dataWarmedElements();
            PredicateContextData predicateContextData = queryContext.getPredicateContextData();
            Optional<MatchData> matchData = queryContext.getMatchData();

            for (Map.Entry<Integer, ColumnHandle> entry : queryContext.getRemainingCollectColumnByBlockIndex().entrySet()) {
                int blockIndex = entry.getKey();
                RegularColumn regularColumn = dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(entry.getValue());
                Type type = dispatcherProxiedConnectorTransformer.getColumnType(entry.getValue());
                if (partitionKeys.containsKey(regularColumn)) {
                    Object convertedPartitionValue = dispatcherProxiedConnectorTransformer.getConvertedPartitionValue(partitionKeys.get(regularColumn), entry.getValue(), DispatcherTableHandle.getNullPartitionValue());
                    SingleValue singleValue = SingleValue.create(type, convertedPartitionValue);
                    PrefilledQueryCollectData prefilledQueryCollectData = PrefilledQueryCollectData.builder()
                            .varadaColumn(regularColumn)
                            .type(type)
                            .blockIndex(blockIndex)
                            .singleValue(singleValue)
                            .build();
                    if (matchData.isPresent()) {
                        matchData = removePrefillColumnFromMatchTree(regularColumn, matchData.get());
                        List<PredicateContext> remainingPredicates = classifyArgs.getPredicateContextData().getRemainingPredicatesByColumn(regularColumn);
                        if (!remainingPredicates.isEmpty()) {
                            Map<VaradaExpression, PredicateContext> newLeaves = new HashMap<>(predicateContextData.getLeaves());
                            remainingPredicates.forEach(predicateContext -> newLeaves.put(predicateContext.getExpression(), predicateContext));
                            // TODO: copied from MatchClassifier but it's a bad practice - leaves are changed while root remains as-is
                            predicateContextData = new PredicateContextData(ImmutableMap.copyOf(newLeaves), predicateContextData.getRootExpression());
                        }
                    }
                    prefilledQueryCollectDataByBlockIndex.put(blockIndex, prefilledQueryCollectData);
                    remainingCollectColumnByBlockIndex.remove(blockIndex);
                }
                else if (prefilledCollectColumnValues.containsKey(regularColumn)) {
                    // Create a prefilled collect
                    Pair<Optional<QueryMatchData>, SingleValue> prefilledMatchToValue = prefilledCollectColumnValues.get(regularColumn);
                    PrefilledQueryCollectData prefilledQueryCollectdata = PrefilledQueryCollectData.builder()
                            .varadaColumn(regularColumn)
                            .type(type)
                            .blockIndex(blockIndex)
                            .singleValue(prefilledMatchToValue.getValue())
                            .build();
                    if (prefilledMatchToValue.getKey().isPresent() && queryContext.getMatchData().isPresent()) {
                        QueryMatchData elementToUpdate = prefilledMatchToValue.getKey().get();
                        MatchData currentMatchData = queryContext.getMatchData().get();
                        matchData = Optional.of(updateMatchElementWithTightness(currentMatchData, elementToUpdate));
                    }
                    prefilledQueryCollectDataByBlockIndex.put(blockIndex, prefilledQueryCollectdata);
                    remainingCollectColumnByBlockIndex.remove(blockIndex);
                }
                else {
                    // handle all null warmup-element as prefilled
                    WarmUpElement dataWarmupElement = dataElementsMap.get(regularColumn);
                    if ((dataWarmupElement != null) &&
                            (dataWarmupElement.getWarmupElementStats().getNullsCount() > 0) &&
                            dataWarmupElement.getWarmupElementStats().getNullsCount() == dataWarmupElement.getTotalRecords()) {
                        SingleValue nullSingleValue = SingleValue.create(type, null);
                        PrefilledQueryCollectData prefilledQueryCollectdata = PrefilledQueryCollectData.builder()
                                .varadaColumn(regularColumn)
                                .type(type)
                                .blockIndex(blockIndex)
                                .singleValue(nullSingleValue)
                                .build();
                        prefilledQueryCollectDataByBlockIndex.put(blockIndex, prefilledQueryCollectdata);
                        remainingCollectColumnByBlockIndex.remove(blockIndex);
                    }
                }
            }

            return queryContext.asBuilder()
                    .prefilledQueryCollectDataByBlockIndex(prefilledQueryCollectDataByBlockIndex)
                    .predicateContextData(predicateContextData)
                    .matchData(matchData)
                    .remainingCollectColumnByBlockIndex(remainingCollectColumnByBlockIndex)
                    .build();
        }
        catch (Throwable e) {
            shapingLogger.warn(e, "failed classifying prefilled");
        }
        return queryContext;
    }

    private Optional<MatchData> removePrefillColumnFromMatchTree(RegularColumn regularColumn, MatchData matchData)
    {
        Optional<MatchData> result;
        if (matchData instanceof LogicalMatchData logicalMatchData &&
                logicalMatchData.getOperator() == LogicalMatchData.Operator.AND) {
            ImmutableList.Builder<MatchData> updatedTerms = ImmutableList.builder();
            for (MatchData term : logicalMatchData.getTerms()) {
                removePrefillColumnFromMatchTree(regularColumn, term).ifPresent(updatedTerms::add);
            }
            ImmutableList<MatchData> newTerms = updatedTerms.build();
            if (newTerms.isEmpty()) {
                result = Optional.empty();
            }
            else if (newTerms.size() == 1) {
                result = Optional.of(newTerms.get(0));
            }
            else {
                result = Optional.of(new LogicalMatchData(logicalMatchData.getOperator(), newTerms));
            }
        }
        else if (matchData instanceof QueryMatchData queryMatchData &&
                queryMatchData.getVaradaColumn().equals(regularColumn)) {
            //remove column from Match tree
            result = Optional.empty();
        }
        else {
            result = Optional.of(matchData);
        }
        return result;
    }

    /**
     * update MatchData tree, search and find @elementToUpdate and set it with tightnessRequired true
     */
    private MatchData updateMatchElementWithTightness(MatchData currentMatchData, QueryMatchData elementToUpdate)
    {
        if (currentMatchData.equals(elementToUpdate)) {
            return elementToUpdate.asBuilder().tightnessRequired(true).build();
        }
        if (currentMatchData instanceof LogicalMatchData logicalMatchData) {
            ImmutableList.Builder<MatchData> updatedTerms = ImmutableList.builder();
            for (MatchData term : logicalMatchData.getTerms()) {
                updatedTerms.add(updateMatchElementWithTightness(term, elementToUpdate));
            }
            return new LogicalMatchData(logicalMatchData.getOperator(), updatedTerms.build());
        }
        else {
            return currentMatchData;
        }
    }

    private Map<VaradaColumn, Pair<Optional<QueryMatchData>, SingleValue>> getPrefilledCollectColumnValues(ClassifyArgs classifyArgs, QueryContext queryContext)
    {
        Map<VaradaColumn, SingleValue> columnsWithSingleValueDomain = getColumnsWithSingleValueDomain(classifyArgs.getPredicateContextData());
        Map<VaradaColumn, Pair<Optional<QueryMatchData>, SingleValue>> res =
                new HashMap<>(columnsWithSingleValueDomain.entrySet()
                        .stream()
                        .map(entry -> Pair.of(getQueryMatchDataForPrefill(queryContext, entry.getKey()), entry.getValue()))
                        .filter(pair -> pair.getKey().isPresent())
                        .collect(Collectors.toMap(pair -> pair.getKey().get().getVaradaColumn(), Function.identity())));

        // Add partition columns (tightness is irrelevant since for each partition, there is only one value in the entire split)
        Map<VaradaColumn, SingleValue> partitionColumnSingleValues = getPartitionColumnSingleValues(queryContext, classifyArgs.getRowGroupData());
        res.putAll(partitionColumnSingleValues.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Pair.of(Optional.empty(), entry.getValue()))));

        return res;
    }

    // Note: We can't add these values in coordinator because tupleDomain contains dynamic filter
    private Map<VaradaColumn, SingleValue> getColumnsWithSingleValueDomain(PredicateContextData predicateContextData)
    {
        Map<VaradaColumn, SingleValue> res = new HashMap<>();
        predicateContextData.getLeaves()
                .entrySet()
                .stream()
                .filter(entry -> entry.getKey() instanceof DomainExpression)
                .forEach(entry -> {
                    PredicateContext predicateContext = entry.getValue();
                    Domain domain = predicateContext.getDomain();
                    if (domain.isSingleValue()) {
                        res.put(predicateContext.getVaradaColumn(),
                                SingleValue.create(domain.getType(), domain.getValues().getSingleValue()));
                    }
                    // Special case - the only possible value is null
                    if (domain.isOnlyNull()) {
                        res.put(predicateContext.getVaradaColumn(),
                                SingleValue.create(domain.getType(), null));
                    }
                });

        return res;
    }

    private Optional<QueryMatchData> getQueryMatchDataForPrefill(QueryContext queryContext, VaradaColumn column)
    {
        // It doesn't matter if there are other predicates on this column (in Proxy \ Trino) -
        // If there is a tight single-value match that is not part of a logical OR, then this value would be the only output.
        // A column that only appears within a logical OR can't be prefilled. For example, on [A=2 OR (B=3 AND C=4)]
        // we can't set prefilled values of B=3, C=4 because the row (A=2,B=5,C=6) also matches the predicate.
        return queryContext.getMatchLeavesDFS().stream()
                .filter(queryMatchData -> !queryMatchData.isPartOfLogicalOr())
                .filter(queryMatchData -> queryMatchData.getVaradaColumn().equals(column) && queryMatchData.canBeTight())
                .findAny();
    }

    private Map<VaradaColumn, SingleValue> getPartitionColumnSingleValues(QueryContext queryContext, RowGroupData rowGroupData)
    {
        return queryContext
                .getRemainingCollectColumnByBlockIndex()
                .values()
                .stream()
                .map(column -> Pair.of(column, dispatcherProxiedConnectorTransformer.getConvertedPartitionValue(rowGroupData, column, DispatcherTableHandle.getNullPartitionValue())))
                .filter(pair -> pair.getValue().isPresent()) // skip non-partition columns
                .collect(Collectors.toMap(
                        pair -> dispatcherProxiedConnectorTransformer.getVaradaRegularColumn(pair.getKey()),
                        pair -> SingleValue.create(dispatcherProxiedConnectorTransformer.getColumnType(pair.getKey()), pair.getValue().get())));
    }
}
