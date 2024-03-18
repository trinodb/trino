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
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.PredicateContext;
import io.trino.plugin.varada.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.varada.expression.DomainExpression;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaConstant;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.rewrite.worker.varadatolucene.LuceneRewriteContext;
import io.trino.plugin.varada.expression.rewrite.worker.varadatolucene.LuceneRulesHandler;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.varada.storage.lucene.LuceneQueryUtils.createRangeQuery;

final class LuceneElementsMatcher
        implements Matcher
{
    private final LuceneRulesHandler luceneRulesHandler;

    public LuceneElementsMatcher(DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        this.luceneRulesHandler = new LuceneRulesHandler();
        luceneRulesHandler.init(dispatcherProxiedConnectorTransformer);
    }

    @Override
    public MatchContext match(ClassifyArgs classifyArgs, MatchContext matchContext)
    {
        ImmutableMap<VaradaColumn, WarmUpElement> luceneColumnToWarmupElement = classifyArgs.getWarmedWarmupTypes().luceneWarmedElements();
        if (luceneColumnToWarmupElement.isEmpty()) {
            return matchContext;
        }

        List<QueryMatchData> matchDataList = new ArrayList<>(matchContext.matchDataList());

        ImmutableMap.Builder<VaradaColumn, PredicateContext> remainingPredicateContext = ImmutableMap.builder();
        ImmutableListMultimap<VaradaColumn, WarmUpElement> basicWarmupElements = classifyArgs.getWarmedWarmupTypes().basicWarmedElements();
        for (Map.Entry<VaradaColumn, PredicateContext> predicateColumn : matchContext.remainingPredicateContext().entrySet()) {
            VaradaColumn varadaColumn = predicateColumn.getKey();
            WarmUpElement warmUpElement = luceneColumnToWarmupElement.get(varadaColumn);
            PredicateContext predicateContext = predicateColumn.getValue();

            if (warmUpElement == null) {
                remainingPredicateContext.put(predicateColumn.getKey(), predicateContext);
                continue;
            }
            Optional<WarmUpElement> basicWarmUpElement = basicWarmupElements.get(varadaColumn).stream().filter(basicElement -> !basicElement.getVaradaColumn().isTransformedColumn()).findFirst();
            if (preferBasicWarm(predicateContext, basicWarmUpElement)) {
                remainingPredicateContext.put(predicateColumn.getKey(), predicateContext);
                continue;
            }
            Optional<LuceneQueryMatchData> lucenePredicateDataOptional = convert(
                    classifyArgs,
                    warmUpElement,
                    predicateContext,
                    basicWarmUpElement);
            if (lucenePredicateDataOptional.isPresent()) {
                matchDataList.add(lucenePredicateDataOptional.get());
            }
            else {
                remainingPredicateContext.put(predicateColumn.getKey(), predicateContext);
            }
        }
        return new MatchContext(matchDataList, remainingPredicateContext.buildOrThrow(), true);
    }

    /**
     * in case the have basic element and its type of PREDICATE_TYPE_STRING_VALUES, PREDICATE_TYPE_INVERSE_STRING we prefer native over lucene
     */
    private static boolean preferBasicWarm(PredicateContext predicateContext, Optional<WarmUpElement> basicWarmUpElement)
    {
        return basicWarmUpElement.isPresent() &&
                predicateContext.getVaradaExpressionData().getNativeExpressionOptional().isPresent() &&
                (predicateContext.getVaradaExpressionData().getNativeExpressionOptional().get().predicateType() == PredicateType.PREDICATE_TYPE_STRING_VALUES ||
                        predicateContext.getVaradaExpressionData().getNativeExpressionOptional().get().predicateType() == PredicateType.PREDICATE_TYPE_INVERSE_STRING);
    }

    private Optional<LuceneQueryMatchData> convert(
            ClassifyArgs classifyArgs,
            WarmUpElement warmUpElement,
            PredicateContext predicateContext,
            Optional<WarmUpElement> basicWarmUpElement)
    {
        boolean simplifiedDomain = predicateContext.isSimplified();
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        Type type = predicateContext.getColumnType();
        boolean isValid = buildLuceneQuery(queryBuilder, predicateContext, basicWarmUpElement, classifyArgs);
        if (!isValid) {
            return Optional.empty();
        }
        boolean isCollectNulls = predicateContext.isCollectNulls();
        Domain domain = predicateContext.getDomain();
        BooleanQuery query = queryBuilder.build();
        LuceneQueryMatchData luceneQueryMatchData = LuceneQueryMatchData
                .builder()
                .warmUpElement(warmUpElement)
                .type(type)
                .query(query)
                .domain(Optional.of(domain))
                .simplifiedDomain(simplifiedDomain)
                .collectNulls(isCollectNulls)
                .tightnessRequired(classifyArgs.getDispatcherTableHandle().isSubsumedPredicates())
                .build();
        return Optional.of(luceneQueryMatchData);
    }

    private boolean buildLuceneQuery(BooleanQuery.Builder innerQueryBuilder, PredicateContext predicateContext, Optional<WarmUpElement> basicWarmUpElement, ClassifyArgs classifyArgs)
    {
        boolean res;
        if (predicateContext.getExpression() instanceof DomainExpression) {
            res = buildLuceneDomainQuery(innerQueryBuilder, predicateContext, basicWarmUpElement, classifyArgs);
        }
        else {
            res = buildLuceneExpressionQuery(innerQueryBuilder, predicateContext);
        }
        return res;
    }

    private boolean buildLuceneExpressionQuery(BooleanQuery.Builder queryBuilder, PredicateContext expressionPredicateContext)
    {
        boolean isValid = false;
        VaradaExpression varadaExpression = expressionPredicateContext.getExpression();
        if (varadaExpression instanceof VaradaConstant varadaConstant) {
            if (varadaConstant.getType() == BooleanType.BOOLEAN && !Boolean.parseBoolean(String.valueOf(varadaConstant.getValue()))) {
                isValid = true;
            }
        }
        else if (varadaExpression instanceof VaradaCall) {
            LuceneRewriteContext context = new LuceneRewriteContext(queryBuilder, BooleanClause.Occur.MUST, expressionPredicateContext.isCollectNulls());
            isValid = luceneRulesHandler.rewrite(varadaExpression, context);
        }
        return isValid;
    }

    private boolean buildLuceneDomainQuery(BooleanQuery.Builder queryBuilder,
            PredicateContext domainPredicateContext,
            Optional<WarmUpElement> basicWarmUpElement,
            ClassifyArgs classifyArgs)
    {
        boolean isValid = false;
        Domain domain = domainPredicateContext.getDomain();
        if (!domain.isAll() && !domain.isNone()) {
            // check if we better search this predicate on BASIC
            if (basicWarmUpElement.isPresent()) {
                // we don't know at this stage if transform will be allowed or not, we assume the worse case
                // because BASIC is only slightly better than LUCENE on single values, but match worse on ranges.
                PredicateType predicateType = classifyArgs.getPredicateTypeFromCache(domain, domainPredicateContext.getColumnType());
                if (PredicateType.PREDICATE_TYPE_STRING_VALUES.equals(predicateType)) {
                    return false;
                }
            }

            SortedRangeSet sortedRangeSet = (SortedRangeSet) domain.getValues();
            if (sortedRangeSet.getRangeCount() > 0) {
                BooleanQuery.Builder innerQueryBuilder = new BooleanQuery.Builder();
                sortedRangeSet.getOrderedRanges().forEach(range -> innerQueryBuilder.add(createRangeQuery(range), BooleanClause.Occur.SHOULD));
                queryBuilder.add(innerQueryBuilder.build(), BooleanClause.Occur.MUST);
            }
            isValid = true;
        }
        return isValid;
    }
}
