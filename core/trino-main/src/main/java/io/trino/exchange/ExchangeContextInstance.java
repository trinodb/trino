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
package io.trino.exchange;

import io.opentelemetry.api.trace.Span;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeId;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ExchangeContextInstance
        implements ExchangeContext
{
    private final QueryId queryId;
    private final ExchangeId exchangeId;
    private final Span parentSpan;

    public ExchangeContextInstance(QueryId queryId, ExchangeId exchangeId, Span parentSpan)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.parentSpan = requireNonNull(parentSpan, "parentSpan is null");
    }

    @Override
    public QueryId getQueryId()
    {
        return queryId;
    }

    @Override
    public ExchangeId getExchangeId()
    {
        return exchangeId;
    }

    @Override
    public Span getParentSpan()
    {
        return parentSpan;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("exchangeId", exchangeId)
                .add("parentSpan", parentSpan)
                .toString();
    }
}
