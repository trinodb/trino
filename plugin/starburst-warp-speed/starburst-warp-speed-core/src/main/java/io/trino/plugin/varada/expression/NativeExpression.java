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
package io.trino.plugin.varada.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.varada.dispatcher.query.classifier.PredicateUtil;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record NativeExpression(
        PredicateType predicateType,
        FunctionType functionType,
        Domain domain,
        boolean collectNulls,
        boolean allSingleValue,
        List<Object> functionParams,
        TransformFunction transformFunction)
{
    @JsonCreator
    public NativeExpression(@JsonProperty("predicateType") PredicateType predicateType,
            @JsonProperty("functionType") FunctionType functionType,
            @JsonProperty("domain") Domain domain,
            @JsonProperty("collectNulls") boolean collectNulls,
            @JsonProperty("allSingleValue") boolean allSingleValue,
            @JsonProperty("functionParams") List<Object> functionParams,
            @JsonProperty("transformedColumn") TransformFunction transformFunction)
    {
        this.predicateType = requireNonNull(predicateType);
        this.functionType = requireNonNull(functionType);
        this.domain = requireNonNull(domain);
        this.collectNulls = collectNulls;
        this.allSingleValue = allSingleValue;
        this.functionParams = requireNonNull(functionParams);
        this.transformFunction = requireNonNull(transformFunction);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(NativeExpression nativeExpression)
    {
        return new Builder()
                .predicateType(nativeExpression.predicateType)
                .collectNulls(nativeExpression.collectNulls)
                .domain(nativeExpression.domain)
                .functionParams(nativeExpression.functionParams)
                .transformedColumn(nativeExpression.transformFunction)
                .functionType(nativeExpression.functionType);
    }

    @Override
    @JsonProperty("domain")
    public Domain domain()
    {
        return domain;
    }

    @Override
    @JsonProperty("predicateType")
    public PredicateType predicateType()
    {
        return predicateType;
    }

    @Override
    @JsonProperty("functionType")
    public FunctionType functionType()
    {
        return functionType;
    }

    @Override
    @JsonProperty("collectNulls")
    public boolean collectNulls()
    {
        return collectNulls;
    }

    @Override
    @JsonProperty("allSingleValue")
    public boolean allSingleValue()
    {
        return allSingleValue;
    }

    @Override
    @JsonProperty("functionParams")
    public List<Object> functionParams()
    {
        return functionParams;
    }

    @JsonProperty("transformedColumn")
    public TransformFunction transformFunction()
    {
        return transformFunction;
    }

    public NativeExpression mergeAnd(NativeExpression other)
    {
        Domain intersectDomain = domain().intersect(other.domain());
        return NativeExpression.builder(this)
                .domain(intersectDomain)
                .collectNulls(intersectDomain.isNullAllowed())
                .build();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NativeExpression that)) {
            return false;
        }
        return predicateType == that.predicateType &&
                domain.equals(that.domain) &&
                transformFunction.equals(that.transformFunction) &&
                collectNulls == that.collectNulls &&
                functionParams.equals(that.functionParams) &&
                functionType == that.functionType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(predicateType, functionType, domain, collectNulls, functionParams, transformFunction);
    }

    public static class Builder
    {
        private PredicateType predicateType;
        private FunctionType functionType;
        private Domain domain;
        private Boolean collectNulls;
        private List<Object> functionParams = Collections.emptyList();

        private TransformFunction transformFunction = TransformFunction.NONE;

        private Builder()
        {}

        public Builder predicateType(PredicateType predicateType)
        {
            this.predicateType = predicateType;
            return this;
        }

        public Builder functionType(FunctionType functionType)
        {
            this.functionType = functionType;
            return this;
        }

        public FunctionType getFunctionType()
        {
            return functionType;
        }

        public PredicateType getPredicateType()
        {
            return predicateType;
        }

        public Builder domain(Domain domain)
        {
            this.domain = domain;
            return this;
        }

        public Builder collectNulls(boolean collectNulls)
        {
            this.collectNulls = collectNulls;
            return this;
        }

        public boolean getCollectNulls()
        {
            return collectNulls;
        }

        public Builder functionParams(List<Object> functionParams)
        {
            this.functionParams = functionParams;
            return this;
        }

        public Builder transformedColumn(TransformFunction transformFunction)
        {
            this.transformFunction = transformFunction;
            return this;
        }

        public List<Object> getFunctionParams()
        {
            return functionParams;
        }

        public Domain getDomain()
        {
            return domain;
        }

        public NativeExpression build()
        {
            NativeExpression res;
            if (domain.isAll() || domain.isNone()) {
                res = new NativeExpression(predicateType,
                        requireNonNull(functionType),
                        domain,
                        domain.isAll(),
                        true,
                        Collections.emptyList(), TransformFunction.NONE);
            }
            else {
                checkArgument(domain.getValues() instanceof SortedRangeSet);
                SortedRangeSet sortedRangeSet = (SortedRangeSet) domain.getValues();
                Block sortedRanges = sortedRangeSet.getSortedRanges();
                boolean[] inclusive = sortedRangeSet.getInclusive();
                boolean allSingleValue = PredicateUtil.isAllSingleValue(inclusive, sortedRanges, domain.getType());
                res = new NativeExpression(requireNonNull(predicateType),
                        requireNonNull(functionType),
                        domain,
                        requireNonNull(collectNulls),
                        allSingleValue,
                        functionParams,
                        transformFunction);
            }
            return res;
        }
    }
}
