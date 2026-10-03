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
package io.trino.server.protocol;

import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.trino.Session;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.QueryManager;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.server.ForStatementResource;
import io.trino.spi.block.BlockEncodingSerde;

import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;

/**
 * Assembles the machinery a {@link ProtocolQuery} needs to pull results out of a dispatched query,
 * so that callers only have to supply the session and slug that identify it.
 */
public class ProtocolQueryFactory
{
    private final QueryManager queryManager;
    private final QueryInfoUrlFactory queryInfoUrlFactory;
    private final DirectExchangeClientSupplier directExchangeClientSupplier;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final BoundedExecutor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final BlockEncodingSerde blockEncodingSerde;

    @Inject
    public ProtocolQueryFactory(
            QueryManager queryManager,
            QueryInfoUrlFactory queryInfoUrlFactory,
            DirectExchangeClientSupplier directExchangeClientSupplier,
            ExchangeManagerRegistry exchangeManagerRegistry,
            @ForStatementResource BoundedExecutor responseExecutor,
            @ForStatementResource ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.queryInfoUrlFactory = requireNonNull(queryInfoUrlFactory, "queryInfoUrlFactory is null");
        this.directExchangeClientSupplier = requireNonNull(directExchangeClientSupplier, "directExchangeClientSupplier is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
    }

    public ProtocolQuery create(Session session, Slug slug, QueryDataProducerFactory queryDataProducerFactory)
    {
        return createQuery(session, slug, queryDataProducerFactory);
    }

    /**
     * Same as {@link #create}, exposing the implementation type to the REST resource, which also
     * drives per-request concerns (slug validation, stage-level cancellation) that are not part of
     * the {@link ProtocolQuery} contract.
     */
    Query createQuery(Session session, Slug slug, QueryDataProducerFactory queryDataProducerFactory)
    {
        return Query.create(
                session,
                slug,
                queryManager,
                queryInfoUrlFactory.getQueryInfoUrl(session.getQueryId()),
                directExchangeClientSupplier,
                exchangeManagerRegistry,
                responseExecutor,
                timeoutExecutor,
                blockEncodingSerde,
                queryDataProducerFactory);
    }
}
