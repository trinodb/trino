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

import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.expression.DomainExpression;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaExpressionData;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

import static io.trino.plugin.varada.dispatcher.query.classifier.PredicateUtil.isInversePredicate;
import static java.util.Objects.requireNonNull;

public class PredicateContext
{
    private final VaradaExpressionData varadaExpressionData;
    private final boolean isSimplified;

    public PredicateContext(VaradaExpressionData varadaExpressionData)
    {
        this(requireNonNull(varadaExpressionData), false);
    }

    public PredicateContext(VaradaExpressionData varadaExpressionData, boolean isSimplified)
    {
        this.varadaExpressionData = varadaExpressionData;
        this.isSimplified = isSimplified;
    }

    public RegularColumn getVaradaColumn()
    {
        return varadaExpressionData.getVaradaColumn();
    }

    public Type getColumnType()
    {
        return varadaExpressionData.getColumnType();
    }

    public Domain getDomain()
    {
        return getNativeExpression().map(NativeExpression::domain)
                .orElse(Domain.all(getColumnType()));
    }

    public VaradaExpressionData getVaradaExpressionData()
    {
        return varadaExpressionData;
    }

    public Optional<NativeExpression> getNativeExpression()
    {
        return varadaExpressionData.getNativeExpressionOptional();
    }

    public boolean isCollectNulls()
    {
        return varadaExpressionData.isCollectNulls();
    }

    public boolean isInverseWithNulls()
    {
        return varadaExpressionData.isCollectNulls() &&
                getExpression() instanceof DomainExpression domainExpression &&
                domainExpression.getDomain().getValues() instanceof SortedRangeSet sortedRangeSet &&
                isInversePredicate(sortedRangeSet, domainExpression.getType());
    }

    public boolean isSimplified()
    {
        return isSimplified;
    }

    public VaradaExpression getExpression()
    {
        return varadaExpressionData.getExpression();
    }

    public boolean canMergeExpressions(PredicateContext other)
    {
        boolean res = false;
        if (getNativeExpression().isPresent() &&
                other.getNativeExpression().isPresent()) {
            NativeExpression expression1 = getNativeExpression().get();
            NativeExpression expression2 = other.getNativeExpression().get();
            if (expression1.functionType() == expression2.functionType() &&
                    Objects.equals(expression1.transformFunction(), expression2.transformFunction()) &&
                    expression1.functionParams().equals(expression2.functionParams())) {
                res = true;
            }
        }
        return res;
    }

    public TransformFunction getTransformedColumn()
    {
        return getNativeExpression().isPresent() ? getNativeExpression().get().transformFunction() : TransformFunction.NONE;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PredicateContext that = (PredicateContext) o;
        return isSimplified == that.isSimplified() &&
                Objects.equals(varadaExpressionData, that.varadaExpressionData);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(varadaExpressionData, isSimplified);
    }

    @Override
    public String toString()
    {
        return "PredicateContext{" +
                "isSimplified=" + isSimplified +
                ", varadaExpressionData=" + varadaExpressionData +
                '}';
    }
}
