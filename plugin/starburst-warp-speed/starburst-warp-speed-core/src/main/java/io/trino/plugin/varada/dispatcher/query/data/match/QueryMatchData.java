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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.data.QueryColumn;
import io.trino.plugin.varada.juffer.PredicateCacheData;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public abstract class QueryMatchData
        extends QueryColumn
        implements MatchData
{
    private final boolean collectNulls;
    private final PredicateCacheData predicateCacheData;
    private final List<QueryMatchData> leaves;
    protected final boolean tightnessRequired;
    protected final Optional<Domain> domain;
    protected final boolean simplifiedDomain;
    protected final boolean isPartOfLogicalOr;

    protected QueryMatchData(WarmUpElement warmUpElement,
            boolean collectNulls,
            PredicateCacheData predicateCacheData,
            Type type,
            boolean tightnessRequired,
            Optional<Domain> domain,
            boolean simplifiedDomain,
            boolean isPartOfLogicalOr)
    {
        super(warmUpElement, type);
        this.collectNulls = collectNulls;
        this.predicateCacheData = predicateCacheData;
        this.tightnessRequired = tightnessRequired;
        this.domain = domain;
        this.simplifiedDomain = simplifiedDomain;
        this.isPartOfLogicalOr = isPartOfLogicalOr;

        this.leaves = ImmutableList.of(this);
    }

    public abstract boolean canMatchCollect(VaradaColumn varadaColumn);

    public abstract boolean canMapMatchCollect();

    public abstract boolean isFunction();

    @Override
    public List<QueryMatchData> getLeavesDFS()
    {
        return leaves;
    }

    public boolean canBeTight()
    {
        return !simplifiedDomain;
    }

    public WarmUpElement getWarmUpElement()
    {
        // must present
        return getWarmUpElementOptional().get();
    }

    public boolean isCollectNulls()
    {
        return collectNulls;
    }

    public PredicateCacheData getPredicateCacheData()
    {
        return predicateCacheData;
    }

    public boolean isTightnessRequired()
    {
        return tightnessRequired;
    }

    public Optional<Domain> getDomain()
    {
        return domain;
    }

    public boolean isSimplifiedDomain()
    {
        return simplifiedDomain;
    }

    @Override
    public boolean isPartOfLogicalOr()
    {
        return isPartOfLogicalOr;
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
        QueryMatchData that = (QueryMatchData) o;
        return super.equals(that) &&
                collectNulls == that.collectNulls &&
                tightnessRequired == that.tightnessRequired &&
                domain.equals(that.domain) &&
                simplifiedDomain == that.simplifiedDomain &&
                isPartOfLogicalOr == that.isPartOfLogicalOr;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), collectNulls, tightnessRequired, domain, simplifiedDomain, isPartOfLogicalOr);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" +
                "collectNulls=" + collectNulls +
                ", tightnessRequired=" + tightnessRequired +
                ", domain=" + domain +
                ", simplifiedDomain=" + simplifiedDomain +
                ", isPartOfLogicalOr=" + isPartOfLogicalOr +
                "} " + super.toString();
    }

    @Override
    public abstract Builder asBuilder();

    public abstract static class Builder
            extends QueryColumn.Builder
    {
        protected PredicateCacheData predicateCacheData;
        protected boolean collectNulls;
        protected boolean tightnessRequired;
        protected Optional<Domain> domain = Optional.empty();
        protected boolean simplifiedDomain;
        protected boolean isPartOfLogicalOr;

        public Builder()
        {
            super();
        }

        public Builder(QueryMatchData queryMatchData)
        {
            super(queryMatchData);
            this.collectNulls = queryMatchData.collectNulls;
            this.predicateCacheData = queryMatchData.predicateCacheData;
            this.tightnessRequired = queryMatchData.tightnessRequired;
            this.domain = queryMatchData.domain;
            this.simplifiedDomain = queryMatchData.simplifiedDomain;
            this.isPartOfLogicalOr = queryMatchData.isPartOfLogicalOr;
        }

        public Builder collectNulls(boolean collectNulls)
        {
            this.collectNulls = collectNulls;
            return this;
        }

        public Builder predicateCacheData(PredicateCacheData predicateCacheData)
        {
            this.predicateCacheData = predicateCacheData;
            return this;
        }

        public Builder tightnessRequired(boolean tightnessRequired)
        {
            this.tightnessRequired = tightnessRequired;
            return this;
        }

        public Builder domain(Optional<Domain> domain)
        {
            this.domain = domain;
            return this;
        }

        public Builder simplifiedDomain(boolean simplifiedDomain)
        {
            this.simplifiedDomain = simplifiedDomain;
            return this;
        }

        public Builder warmUpElement(WarmUpElement warmUpElement)
        {
            super.warmUpElementOptional(Optional.of(warmUpElement));
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
        public abstract QueryMatchData build();
    }
}
