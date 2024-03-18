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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.query.PredicateContext;
import io.trino.plugin.varada.expression.DomainExpression;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.predicate.SortedRangeSet;

import java.util.List;
import java.util.Set;

import static io.trino.plugin.varada.dispatcher.query.classifier.PredicateUtil.isAllSingleValue;
import static io.trino.plugin.varada.dispatcher.query.classifier.PredicateUtil.isInversePredicate;

public class PredicateContextData
{
    private final ImmutableMap<VaradaExpression, PredicateContext> leaves;
    /**
     * root expression which is a combination of domain and expression with AND between them
     */
    private final VaradaExpression rootExpression;

    private final ListMultimap<RegularColumn, PredicateContext> remainingPredicatesByColumn;

    public PredicateContextData(ImmutableMap<VaradaExpression, PredicateContext> leaves,
            VaradaExpression rootExpression)
    {
        this.leaves = leaves;
        this.rootExpression = rootExpression;
        ListMultimap<RegularColumn, PredicateContext> remainingPredicates = ArrayListMultimap.create();
        for (PredicateContext leaf : leaves.values()) {
            remainingPredicates.put(leaf.getVaradaColumn(), leaf);
        }
        this.remainingPredicatesByColumn = ImmutableListMultimap.copyOf(remainingPredicates);
    }

    VaradaExpression getRootExpression()
    {
        return rootExpression;
    }

    public Set<RegularColumn> getRemainingColumns()
    {
        return remainingPredicatesByColumn.keySet();
    }

    public ImmutableMap<VaradaExpression, PredicateContext> getLeaves()
    {
        return leaves;
    }

    public List<PredicateContext> getRemainingPredicatesByColumn(RegularColumn regularColumn)
    {
        return remainingPredicatesByColumn.get(regularColumn);
    }

    public boolean isLuceneColumn(VaradaColumn varadaColumn)
    {
        boolean res = false;
        for (PredicateContext predicateContext : leaves.values()) {
            if (!predicateContext.getVaradaColumn().equals(varadaColumn)) {
                continue;
            }
            if (isLuceneExpression(predicateContext)) {
                res = true;
                break;
            }
            else if (predicateContext.getExpression() instanceof VaradaCall &&
                    (predicateContext.getVaradaExpressionData().getNativeExpressionOptional().isEmpty() ||
                            predicateContext.getVaradaExpressionData().getNativeExpressionOptional().get().predicateType() == PredicateType.PREDICATE_TYPE_STRING_RANGES)) {
                res = true;
                break;
            }
        }
        return res;
    }

    /**
     * for string values - prefer basic over Lucene
     * for string ranges - prefer Lucene over Basic
     */
    private static boolean isLuceneExpression(PredicateContext predicateContext)
    {
        return predicateContext.getExpression() instanceof DomainExpression domainExpression &&
                TypeUtils.isVarcharType(domainExpression.getType()) &&
                domainExpression.getDomain().getValues() instanceof SortedRangeSet sortedRangeSet &&
                    /* we choose to warm Lucene in case of PREDICATE_TYPE_STRING_RANGES, cause native
                     only can say if all the chunk is in or out and then trino needs to filter after us, means we are not tight */
                !isAllSingleValue(sortedRangeSet.getInclusive(), sortedRangeSet.getSortedRanges(), domainExpression.getType()) &&
                !isInversePredicate(sortedRangeSet, domainExpression.getType());
    }

    @Override
    public String toString()
    {
        return "PredicateContextData{" +
                "remainingPredicateExpressions=" + leaves +
                ", rootExpression=" + rootExpression +
                '}';
    }
}
