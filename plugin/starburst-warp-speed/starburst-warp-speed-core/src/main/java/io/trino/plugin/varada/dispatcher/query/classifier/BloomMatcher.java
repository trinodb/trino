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

import com.google.common.collect.TreeMultimap;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.PredicateContext;
import io.trino.plugin.varada.dispatcher.query.data.match.BasicBloomQueryMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.predicate.Domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class BloomMatcher
        implements Matcher
{
    private static final Set<PredicateType> supportedPredicateTypes = Set.of(
            PredicateType.PREDICATE_TYPE_VALUES,
            PredicateType.PREDICATE_TYPE_STRING_VALUES,
            PredicateType.PREDICATE_TYPE_NONE);

    BloomMatcher()
    {
    }

    @Override
    public MatchContext match(ClassifyArgs classifyArgs, MatchContext matchContext)
    {
        if (matchContext.remainingPredicateContext().isEmpty()) {
            return matchContext;
        }
        TreeMultimap<VaradaColumn, WarmUpElement> bloomColNameToWarmupElement = classifyArgs.getWarmedWarmupTypes().bloomWarmedElements();
        if (bloomColNameToWarmupElement.isEmpty()) {
            return matchContext;
        }
        List<QueryMatchData> matchDataList = new ArrayList<>(matchContext.matchDataList());

        for (Map.Entry<VaradaColumn, PredicateContext> predicateColumn : matchContext.remainingPredicateContext().entrySet()) {
            Optional<WarmUpElement> warmUpElement = bloomColNameToWarmupElement.get(predicateColumn.getKey()).stream().filter(x -> !x.getVaradaColumn().isTransformedColumn()).findAny();
            PredicateContext predicateContext = predicateColumn.getValue();
            Domain domain = predicateContext.getDomain();
            boolean supportedByNative = !(domain.isAll() || domain.isNone());

            // Since bloom is not tight, we leave the domain in remainingTupleDomain, so maybe another classifier will use it
            if (warmUpElement.isPresent() &&
                    predicateContext.getNativeExpression().filter(expression -> expression.functionType() == FunctionType.FUNCTION_TYPE_NONE).isPresent() &&
                    supportedByNative &&
                    PredicateUtil.canApplyPredicate(warmUpElement, predicateContext.getColumnType()) &&
                    supportedPredicateTypes.contains(classifyArgs.getPredicateTypeFromCache(domain, predicateContext.getColumnType()))) {
                matchDataList.add(BasicBloomQueryMatchData.builder()
                        .warmUpElement(warmUpElement.get())
                        .type(predicateContext.getColumnType())
                        .domain(Optional.of(domain))
                        .nativeExpression(predicateContext.getNativeExpression().get())
                        .simplifiedDomain(predicateContext.isSimplified())
                        .tightnessRequired(classifyArgs.getDispatcherTableHandle().isSubsumedPredicates())
                        .build());
            }
        }

        return new MatchContext(matchDataList, matchContext.remainingPredicateContext(), true);
    }
}
