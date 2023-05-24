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
package io.trino.spi.exchange;

import io.trino.spi.Experimental;
import io.trino.spi.QueryId;

import static java.util.Objects.requireNonNull;

@Experimental(eta = "2023-09-01")
public class ExchangeContext
{
    private final QueryId queryId;
    private final ExchangeId exchangeId;

    public ExchangeContext(QueryId queryId, ExchangeId exchangeId)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public ExchangeId getExchangeId()
    {
        return exchangeId;
    }
}
