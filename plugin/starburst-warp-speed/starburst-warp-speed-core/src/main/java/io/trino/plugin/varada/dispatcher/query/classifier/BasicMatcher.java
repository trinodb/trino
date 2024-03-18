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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.varada.dispatcher.model.TransformedColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.PredicateContext;
import io.trino.plugin.varada.dispatcher.query.data.match.BasicBloomQueryMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.spi.predicate.Domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.plugin.varada.dispatcher.query.classifier.PredicateUtil.canApplyPredicate;

class BasicMatcher
        implements Matcher
{
    BasicMatcher()
    {
    }

    @Override
    public MatchContext match(ClassifyArgs classifyArgs,
            MatchContext matchContext)
    {
        ImmutableListMultimap<VaradaColumn, WarmUpElement> basicColNameToWarmupElement = classifyArgs.getWarmedWarmupTypes().basicWarmedElements();
        if (basicColNameToWarmupElement.isEmpty()) {
            return matchContext;
        }
        List<QueryMatchData> matchDataList = new ArrayList<>(matchContext.matchDataList());
        ImmutableMap.Builder<VaradaColumn, PredicateContext> remainingPredicateContext = ImmutableMap.builder();

        for (Map.Entry<VaradaColumn, PredicateContext> predicateColumn : matchContext.remainingPredicateContext().entrySet()) {
            PredicateContext predicateContext = predicateColumn.getValue();
            Optional<NativeExpression> nativeExpression = predicateContext.getNativeExpression();
            Optional<WarmUpElement> warmUpElement;
            Domain domain = predicateContext.getDomain();

            if (nativeExpression.isPresent() && !domain.isAll()) {
                List<WarmUpElement> existingWarmupElements = basicColNameToWarmupElement.get(predicateColumn.getKey());
                warmUpElement = existingWarmupElements.stream()
                        .filter(element ->
                                ((element.getVaradaColumn() instanceof TransformedColumn transformedColumn) &&
                                        Objects.equals(transformedColumn.getTransformFunction(), nativeExpression.get().transformFunction())) ||
                                (!element.getVaradaColumn().isTransformedColumn() &&
                                        Objects.equals(nativeExpression.get().transformFunction(), TransformFunction.NONE)))
                        .findFirst();

                if (warmUpElement.isPresent() && canApplyPredicate(warmUpElement, predicateContext.getColumnType())) {
                    matchDataList.add(BasicBloomQueryMatchData.builder()
                            .warmUpElement(warmUpElement.get())
                            .type(predicateContext.getColumnType())
                            .domain(Optional.of(domain))
                            .simplifiedDomain(predicateContext.isSimplified())
                            .nativeExpression(nativeExpression.get())
                            .tightnessRequired(classifyArgs.getDispatcherTableHandle().isSubsumedPredicates())
                            .build());
                }
                else {
                    remainingPredicateContext.put(predicateColumn.getKey(), predicateContext);
                }
            }
            else {
                remainingPredicateContext.put(predicateColumn.getKey(), predicateContext);
            }
        }
        return new MatchContext(matchDataList, remainingPredicateContext.buildOrThrow(), true);
    }
}
