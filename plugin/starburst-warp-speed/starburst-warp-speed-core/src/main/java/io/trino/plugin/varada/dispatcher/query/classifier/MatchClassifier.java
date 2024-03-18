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
import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.query.PredicateContext;
import io.trino.plugin.varada.dispatcher.query.QueryContext;
import io.trino.plugin.varada.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.MatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.NoneMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaExpressionData;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.type.BooleanType;
import io.varada.log.ShapingLogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;

class MatchClassifier
        implements Classifier
{
    private static final Logger logger = Logger.get(MatchClassifier.class);
    private final ShapingLogger shapingLogger;

    private final List<Matcher> matchers;

    MatchClassifier(List<Matcher> matchers, GlobalConfiguration globalConfiguration)
    {
        this.matchers = matchers;
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfiguration.getShapingLoggerThreshold(),
                globalConfiguration.getShapingLoggerDuration(),
                globalConfiguration.getShapingLoggerNumberOfSamples());
    }

    @Override
    public QueryContext classify(ClassifyArgs classifyArgs, QueryContext queryContext)
    {
        PredicateContextData predicateContextData = queryContext.getPredicateContextData();
        ImmutableMap<VaradaExpression, PredicateContext> leaves = predicateContextData.getLeaves();
        VaradaExpression rootExpression = queryContext.getPredicateContextData().getRootExpression();
        if (rootExpression == VaradaPrimitiveConstant.FALSE) {
            return queryContext.asBuilder()
                    .isNone(true)
                    .build();
        }
        if (leaves.isEmpty()) {
            return queryContext;
        }
        MatchResult matchResult = handleLogicalFunction(classifyArgs, leaves, rootExpression);
        boolean isNone = false;
        Set<VaradaColumn> matchColumns = new HashSet<>();
        if (matchResult.matchData().isPresent()) {
            MatchData matchData = matchResult.matchData().get();
            if (matchData instanceof NoneMatchData) {
                isNone = true;
            }
            else {
                for (QueryMatchData queryMatchData : matchData.getLeavesDFS()) {
                    if (queryMatchData.getWarmUpElement().getWarmUpType() == WarmUpType.WARM_UP_TYPE_DATA) {
                        shapingLogger.error("calculated queryMatchData with WARM_UP_TYPE_DATA type, skip matching. matchData=%s. classifyArgs=%s", matchData, classifyArgs);
                        matchColumns.clear();
                        break;
                    }
                    VaradaColumn varadaColumn = queryMatchData.getVaradaColumn();
                    matchColumns.add(varadaColumn);
                }
            }
        }

        Map<VaradaExpression, PredicateContext> remainingPredicateExpressions = isNone ?
                Collections.emptyMap() :
                leaves.entrySet()
                        .stream()
                        // TODO: This is not accurate, one match is not enough to determine that there are no remaining matches (for example in the case of domain + expression).
                        // TODO: This is ok for now since we mark canBeTight = false in the query context
                        .filter(entry -> !matchColumns.contains(entry.getValue().getVaradaColumn()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // TODO: The predicate is not accurate anymore, leaves are changed while root remains as-is
        PredicateContextData remainingPredicateContextData = new PredicateContextData(ImmutableMap.copyOf(remainingPredicateExpressions), predicateContextData.getRootExpression());

        return queryContext.asBuilder()
                .matchData(matchResult.matchData())
                .predicateContextData(remainingPredicateContextData)
                .canBeTight(matchResult.canBeTight()) // Overwrite (instead of && with existing value) because beforehand, there might have been remaining predicates that caused queryContext to be non-tight
                .isNone(isNone)
                .build();
    }

    private MatchResult handleLogicalFunction(ClassifyArgs classifyArgs,
            Map<VaradaExpression, PredicateContext> leaves,
            VaradaExpression expression)
    {
        MatchResult result;

        try {
            if (expression instanceof VaradaCall varadaCall) {
                if (varadaCall.getFunctionName().equals(AND_FUNCTION_NAME.getName())) {
                    result = handleAndExpression(classifyArgs, varadaCall, leaves);
                }
                else if (varadaCall.getFunctionName().equals(OR_FUNCTION_NAME.getName())) {
                    result = handleOrExpression(classifyArgs, varadaCall, leaves);
                }
                else {
                    Optional<MatchData> matchData = handleFlatExpression(classifyArgs, leaves, expression);
                    result = new MatchResult(matchData, matchData.isPresent());
                }
            }
            else {
                Optional<MatchData> matchData = handleFlatExpression(classifyArgs, leaves, expression);
                result = new MatchResult(matchData, matchData.isPresent());
            }
        }
        catch (Exception e) {
            shapingLogger.warn(e, "failed on file %s", classifyArgs.getRowGroupData().getRowGroupKey());
            result = new MatchResult(Optional.empty(), false);
        }
        return result;
    }

    private Optional<MatchData> handleFlatExpression(ClassifyArgs classifyArgs,
            Map<VaradaExpression, PredicateContext> leaves,
            VaradaExpression expression)
    {
        Optional<MatchData> res;
        PredicateContext leaf = leaves.get(expression);
        if (leaf.getDomain().isNone()) {
            return Optional.of(new NoneMatchData());
        }
        if (!classifyArgs.isEnableInverseWithNulls() && leaf.isInverseWithNulls()) {
            return Optional.empty();
        }

        MatchContext matchContext = runMatchers(classifyArgs, Map.of(leaf.getVaradaColumn(), leaf));
        if (!matchContext.validRange()) {
            // if there are any existing matches, they are dropped and replaced with none
            return Optional.of(new NoneMatchData());
        }
        List<MatchData> terms = new ArrayList<>(matchContext.matchDataList());
        if (terms.isEmpty()) {
            res = Optional.empty();
        }
        else if (terms.size() == 1) {
            res = Optional.of(terms.get(0));
        }
        else {
            res = Optional.of(new LogicalMatchData(LogicalMatchData.Operator.AND, terms));
        }
        return res;
    }

    private MatchResult handleAndExpression(ClassifyArgs classifyArgs, VaradaCall andExpression,
            Map<VaradaExpression, PredicateContext> leaves)
    {
        List<MatchData> terms = new ArrayList<>();
        Map<VaradaColumn, PredicateContext> remainingPredicateContext = new HashMap<>();
        boolean canBeTight = true;
        for (VaradaExpression varadaExpression : andExpression.getChildren()) {
            if (varadaExpression instanceof VaradaCall varadaCall &&
                    (varadaCall.getFunctionName().equals(OR_FUNCTION_NAME.getName()) ||
                            varadaCall.getFunctionName().equals(AND_FUNCTION_NAME.getName()))) {
                MatchResult matchResult = handleLogicalFunction(classifyArgs, leaves, varadaExpression);
                if (matchResult.matchData().isPresent()) {
                    MatchData matchData = matchResult.matchData().get();
                    if (matchData instanceof NoneMatchData) {
                        return new MatchResult(Optional.of(new NoneMatchData()), true);
                    }
                    terms.add(matchData);
                }
                else {
                    canBeTight = false;
                }
            }
            else {
                PredicateContext predicateContext = leaves.get(varadaExpression);
                if (predicateContext.getDomain().isNone()) {
                    return new MatchResult(Optional.of(new NoneMatchData()), true);
                }
                VaradaColumn varadaColumn = predicateContext.getVaradaColumn();
                PredicateContext existingPredicateContext = remainingPredicateContext.get(varadaColumn);
                if (existingPredicateContext == null) {
                    remainingPredicateContext.put(varadaColumn, predicateContext);
                }
                else {
                    Optional<PredicateContext> predicateContextBase = tryMergeAndPredicates(existingPredicateContext, predicateContext);
                    if (predicateContextBase.isPresent()) {
                        if (predicateContextBase.get().getDomain().isNone()) {
                            //no need to continue - will return emptyPageSource
                            return new MatchResult(Optional.of(new NoneMatchData()), true);
                        }
                        remainingPredicateContext.put(varadaColumn, predicateContextBase.get());
                    }
                    else {
                        Optional<MatchData> matchData = handleFlatExpression(classifyArgs, leaves, varadaExpression);
                        matchData.ifPresent(terms::add);
                    }
                }
            }
        }
        MatchContext matchContext = runMatchers(classifyArgs, remainingPredicateContext);
        if (!matchContext.validRange()) {
            return new MatchResult(Optional.of(new NoneMatchData()), true);
        }
        canBeTight = canBeTight && matchContext.remainingPredicateContext().isEmpty();
        terms.addAll(matchContext.matchDataList());
        Optional<MatchData> result;
        if (terms.isEmpty()) {
            result = Optional.empty();
        }
        else if (terms.size() == 1) {
            return new MatchResult(Optional.of(terms.get(0)), canBeTight);
        }
        else {
            result = Optional.of(new LogicalMatchData(LogicalMatchData.Operator.AND, terms));
        }
        return new MatchResult(result, canBeTight);
    }

    /**
     * merge 2 predicate with AND between them, if they are the same we can intersect domains
     */
    private Optional<PredicateContext> tryMergeAndPredicates(PredicateContext existingPredicateContext, PredicateContext newPredicateContext)
    {
        Optional<PredicateContext> res = Optional.empty();
        if (existingPredicateContext.canMergeExpressions(newPredicateContext)) {
            NativeExpression expression1 = existingPredicateContext.getNativeExpression().get();
            NativeExpression expression2 = newPredicateContext.getNativeExpression().get();
            NativeExpression mergedNativeExpression = expression1.mergeAnd(expression2);
            VaradaExpression varadaExpression = new VaradaCall(AND_FUNCTION_NAME.getName(),
                    List.of(existingPredicateContext.getExpression(), newPredicateContext.getExpression()),
                    BooleanType.BOOLEAN);
            res = Optional.of(new PredicateContext(
                    new VaradaExpressionData(varadaExpression,
                            existingPredicateContext.getColumnType(),
                            mergedNativeExpression.collectNulls(),
                            Optional.of(mergedNativeExpression),
                            existingPredicateContext.getVaradaColumn())));
        }
        return res;
    }

    private MatchResult handleOrExpression(ClassifyArgs classifyArgs, VaradaCall orExpression, Map<VaradaExpression, PredicateContext> leaves)
    {
        List<MatchData> terms = new ArrayList<>();
        boolean canBeTight = true;
        for (VaradaExpression varadaExpression : orExpression.getArguments()) {
            checkArgument(varadaExpression instanceof VaradaCall, "varadaExpression is not instance of VaradaCall");
            String functionName = ((VaradaCall) varadaExpression).getFunctionName();
            if (functionName.equals(OR_FUNCTION_NAME.getName()) || functionName.equals(AND_FUNCTION_NAME.getName())) {
                MatchResult matchResult = handleLogicalFunction(classifyArgs, leaves, varadaExpression);
                if (matchResult.matchData().isPresent()) {
                    terms.add(matchResult.matchData().get());
                    canBeTight = canBeTight && matchResult.canBeTight();
                }
                else {
                    terms = Collections.emptyList();
                    canBeTight = false;
                    break;
                }
            }
            else {
                PredicateContext predicateContext = leaves.get(varadaExpression);
                if (predicateContext.getDomain().isNone()) {
                    terms.add(new NoneMatchData());
                    canBeTight = true;
                    continue;
                }
                VaradaColumn varadaColumn = predicateContext.getVaradaColumn();
                MatchContext matchContext = runMatchers(classifyArgs, Map.of(varadaColumn, predicateContext));

                if (matchContext.matchDataList().isEmpty()) {
                    terms = Collections.emptyList();
                    canBeTight = false;
                    break;
                }
                else if (matchContext.matchDataList().size() == 1) {
                    terms.add(matchContext.matchDataList().get(0));
                }
                else {
                    //in case of basic + bloom we will get 2 matchDatas, we need to set AND between them
                    Optional<QueryMatchData> basicMatch = matchContext.matchDataList().stream().filter(x -> x.getWarmUpElement().getWarmUpType() == WarmUpType.WARM_UP_TYPE_BASIC).findFirst();
                    if (basicMatch.isPresent()) {
                        terms.add(basicMatch.get());
                    }
                }
            }
        }
        Optional<MatchData> result;
        checkArgument(terms.size() != 1, "can't contain 1 matchColumn under OR expression");
        if (terms.isEmpty()) {
            result = Optional.empty();
        }
        else {
            terms = terms.stream().filter(x -> !(x instanceof NoneMatchData)).collect(Collectors.toList());
            if (terms.isEmpty()) {
                //means all where none
                result = Optional.of(new NoneMatchData());
            }
            else if (terms.size() == 1) {
                result = Optional.of(terms.get(0));
            }
            else {
                result = Optional.of(new LogicalMatchData(LogicalMatchData.Operator.OR, terms));
            }
        }
        return new MatchResult(result, canBeTight);
    }

    /**
     * @param remainingPredicateContext - A map of predicates with AND relation between them
     */
    private MatchContext runMatchers(ClassifyArgs classifyArgs, Map<VaradaColumn, PredicateContext> remainingPredicateContext)
    {
        MatchContext matchContext = new MatchContext(Collections.emptyList(), remainingPredicateContext, true);
        if (!remainingPredicateContext.isEmpty()) {
            for (Matcher matcher : matchers) {
                matchContext = matcher.match(classifyArgs, matchContext);
                if (!matchContext.validRange()) {
                    break;
                }
            }
        }
        return matchContext;
    }

    @SuppressWarnings("unused")
    private record MatchResult(Optional<MatchData> matchData, boolean canBeTight) {}
}
