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
package io.trino.plugin.varada.dispatcher.query.data.match;

import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.classifier.PredicateUtil;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.juffer.PredicateCacheData;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BasicBloomQueryMatchData
        extends QueryMatchData
{
    protected final NativeExpression nativeExpression;

    private BasicBloomQueryMatchData(WarmUpElement warmUpElement,
            Type type,
            PredicateCacheData predicateCacheData,
            boolean collectNulls,
            boolean tightnessRequired,
            Optional<Domain> domain,
            boolean simplifiedDomain,
            NativeExpression nativeExpression,
            boolean isPartOfLogicalOr)
    {
        super(warmUpElement, collectNulls, predicateCacheData, type, tightnessRequired, domain, simplifiedDomain, isPartOfLogicalOr);

        this.nativeExpression = requireNonNull(nativeExpression);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public boolean canMatchCollect(VaradaColumn varadaColumn)
    {
        return this.varadaColumn.equals(varadaColumn) && !getWarmUpElement().getWarmUpType().bloom();
    }

    @Override
    public boolean canMapMatchCollect()
    {
        return domain.filter(value -> PredicateUtil.canMapMatchCollect(type, getNativeExpression().predicateType(),
                getNativeExpression().functionType(), value.getValues().getRanges().getRangeCount())).isPresent();
    }

    @Override
    public boolean isFunction()
    {
        return nativeExpression.functionType() != FunctionType.FUNCTION_TYPE_NONE;
    }

    @Override
    public boolean canBeTight()
    {
        return super.canBeTight() &&
                !getWarmUpElement().getWarmUpType().bloom() &&
                // in BASIC we support ranges on strings only via the min/max of each chunk, so only single-value ranges can be tight
                (!TypeUtils.isStrType(type) || nativeExpression.allSingleValue());
    }

    @Override
    public Builder asBuilder()
    {
        return new Builder(this);
    }

    public NativeExpression getNativeExpression()
    {
        return nativeExpression;
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
        BasicBloomQueryMatchData that = (BasicBloomQueryMatchData) o;
        return super.equals(that) &&
                nativeExpression.equals(that.nativeExpression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), nativeExpression);
    }

    @Override
    public String toString()
    {
        return "BasicBloomQueryMatchData{" +
                "nativeExpression=" + nativeExpression +
                '}' + super.toString();
    }

    public static class Builder
            extends QueryMatchData.Builder
    {
        protected NativeExpression nativeExpression;

        public Builder()
        {
            super();
        }

        public Builder(BasicBloomQueryMatchData basicBloomQueryMatchData)
        {
            super(basicBloomQueryMatchData);

            this.nativeExpression = basicBloomQueryMatchData.nativeExpression;
        }

        @Override
        public BasicBloomQueryMatchData build()
        {
            return new BasicBloomQueryMatchData(warmUpElementOptional.orElseThrow(),
                    type,
                    predicateCacheData,
                    collectNulls,
                    tightnessRequired,
                    domain,
                    simplifiedDomain,
                    nativeExpression,
                    isPartOfLogicalOr);
        }

        public Builder nativeExpression(NativeExpression nativeExpression)
        {
            this.nativeExpression = nativeExpression;
            return this;
        }

        @Override
        public Builder tightnessRequired(boolean tightnessRequired)
        {
            super.tightnessRequired(tightnessRequired);
            return this;
        }

        @Override
        public Builder simplifiedDomain(boolean simplifiedDomain)
        {
            super.simplifiedDomain(simplifiedDomain);
            return this;
        }

        @Override
        public Builder domain(Optional<Domain> domain)
        {
            super.domain(domain);
            return this;
        }

        @Override
        public Builder warmUpElement(WarmUpElement warmUpElement)
        {
            super.warmUpElement(warmUpElement);
            return this;
        }

        @Override
        public Builder varadaColumn(VaradaColumn varadaColumn)
        {
            super.varadaColumn(varadaColumn);
            return this;
        }

        @Override
        public Builder type(Type type)
        {
            super.type(type);
            return this;
        }

        @Override
        public Builder collectNulls(boolean collectNulls)
        {
            super.collectNulls(collectNulls);
            return this;
        }

        @Override
        public Builder predicateCacheData(PredicateCacheData predicateCacheData)
        {
            super.predicateCacheData(predicateCacheData);
            return this;
        }
    }
}
