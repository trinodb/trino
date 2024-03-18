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
import io.trino.plugin.varada.juffer.PredicateCacheData;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import org.apache.lucene.search.BooleanQuery;

import java.util.Objects;
import java.util.Optional;

public class LuceneQueryMatchData
        extends QueryMatchData
{
    private final BooleanQuery query;

    private LuceneQueryMatchData(WarmUpElement warmUpElement,
            Type type,
            BooleanQuery query,
            PredicateCacheData predicateCacheData,
            boolean collectNulls,
            boolean tightnessRequired,
            Optional<Domain> domain,
            boolean simplifiedDomain,
            boolean isPartOfLogicalOr)
    {
        super(warmUpElement, collectNulls, predicateCacheData, type, tightnessRequired, domain, simplifiedDomain, isPartOfLogicalOr);
        this.query = query;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public boolean canMatchCollect(VaradaColumn varadaColumn)
    {
        return false;
    }

    @Override
    public boolean canMapMatchCollect()
    {
        return false;
    }

    @Override
    public boolean isFunction()
    {
        return true;
    }

    @Override
    public Builder asBuilder()
    {
        return new Builder(this);
    }

    public BooleanQuery getQuery()
    {
        return query;
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
        LuceneQueryMatchData that = (LuceneQueryMatchData) o;
        return super.equals(that) &&
                Objects.equals(query, that.query);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), query);
    }

    @Override
    public String toString()
    {
        return "LuceneQueryMatchData{" +
                "query=" + query +
                '}' + super.toString();
    }

    public static class Builder
            extends QueryMatchData.Builder
    {
        private BooleanQuery query;

        public Builder()
        {
            super();
        }

        public Builder(LuceneQueryMatchData luceneQueryMatchData)
        {
            super(luceneQueryMatchData);
            this.query = luceneQueryMatchData.query;
        }

        @Override
        public LuceneQueryMatchData build()
        {
            return new LuceneQueryMatchData(warmUpElementOptional.orElseThrow(),
                    type,
                    query,
                    predicateCacheData,
                    collectNulls,
                    tightnessRequired,
                    domain,
                    simplifiedDomain,
                    isPartOfLogicalOr);
        }

        public Builder query(BooleanQuery query)
        {
            this.query = query;
            return this;
        }

        @Override
        public Builder tightnessRequired(boolean tightnessRequired)
        {
            super.tightnessRequired(tightnessRequired);
            return this;
        }

        @Override
        public Builder domain(Optional<Domain> domain)
        {
            super.domain(domain);
            return this;
        }

        @Override
        public Builder simplifiedDomain(boolean simplifiedDomain)
        {
            super.simplifiedDomain(simplifiedDomain);
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
