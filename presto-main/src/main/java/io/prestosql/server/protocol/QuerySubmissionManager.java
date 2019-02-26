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
package io.prestosql.server.protocol;

import com.google.common.collect.ImmutableList;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.event.QueryMonitor;
import io.prestosql.execution.ForQueryExecution;
import io.prestosql.execution.LocationFactory;
import io.prestosql.execution.QueryExecution;
import io.prestosql.execution.QueryExecution.QueryExecutionFactory;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.warnings.WarningCollectorFactory;
import io.prestosql.memory.context.SimpleLocalMemoryContext;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.ExchangeClient;
import io.prestosql.operator.ExchangeClientSupplier;
import io.prestosql.security.AccessControl;
import io.prestosql.server.ForStatementResource;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.sql.tree.Statement;
import io.prestosql.transaction.TransactionManager;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.server.protocol.Query.createQuery;
import static java.util.Objects.requireNonNull;

public class QuerySubmissionManager
{
    private static final Logger log = Logger.get(QuerySubmissionManager.class);

    private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();

    private final QueryManager queryManager;

    private final ExchangeClientSupplier exchangeClientSupplier;
    private final BlockEncodingSerde blockEncodingSerde;

    private final ExecutorService queryCreationExecutor;
    private final BoundedExecutor resultsProcessorExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    private final ScheduledExecutorService queryManagementExecutor;

    @GuardedBy("this")
    private ScheduledFuture<?> backgroundTask;

    @Inject
    public QuerySubmissionManager(
            QueryManager queryManager,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Metadata metadata,
            QueryMonitor queryMonitor,
            LocationFactory locationFactory,
            Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories,
            WarningCollectorFactory warningCollectorFactory,
            ExchangeClientSupplier exchangeClientSupplier,
            BlockEncodingSerde blockEncodingSerde,
            @ForQueryExecution ExecutorService queryCreationExecutor,
            @ForStatementResource BoundedExecutor resultsProcessorExecutor,
            @ForStatementResource ScheduledExecutorService timeoutExecutor)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.queryCreationExecutor = requireNonNull(queryCreationExecutor, "queryCreationExecutor is null");
        this.resultsProcessorExecutor = requireNonNull(resultsProcessorExecutor, "resultsProcessorExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");

        this.queryManagementExecutor = Executors.newSingleThreadScheduledExecutor(threadsNamed("query-submission-management-%s"));
    }

    public void submitQuery(
            QueryExecution queryExecution,
            String slug,
            Duration queryElapsedTime,
            Duration queryQueueDuration)
    {
        queries.computeIfAbsent(queryExecution.getQueryId(), id -> {
            ExchangeClient exchangeClient = exchangeClientSupplier.get(new SimpleLocalMemoryContext(
                    newSimpleAggregatedMemoryContext(),
                    ExecutingStatementResource.class.getSimpleName()));
            return createQuery(
                    queryExecution.getQueryId(),
                    slug,
                    queryElapsedTime,
                    queryQueueDuration,
                    queryManager,
                    () -> submitQueryExecution(queryExecution),
                    throwable -> {
                        queryExecution.fail(throwable);
                        queryManager.createQuery(queryExecution);
                        return queryExecution;
                    },
                    exchangeClient,
                    queryCreationExecutor,
                    resultsProcessorExecutor,
                    timeoutExecutor,
                    blockEncodingSerde);
        });
    }

    private QueryExecution submitQueryExecution(QueryExecution queryExecution)
    {
        queryManager.createQuery(queryExecution);
        return queryExecution;
    }

    public Optional<Query> getQuery(QueryId queryId)
    {
        return Optional.ofNullable(queries.get(queryId));
    }

    @PostConstruct
    public synchronized void start()
    {
        checkState(backgroundTask == null, "%s already started", getClass().getSimpleName());
        backgroundTask = queryManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                failAbandonedQueries();
            }
            catch (Throwable e) {
                log.error(e, "Error failing abandoned queries");
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void failAbandonedQueries()
    {
        for (Query query : ImmutableList.copyOf(queries.values())) {
            try {
                if (query.isQueryCreated()) {
                    // track query until it is no longer being track by the query manager
                    if (!queryManager.isQueryRegistered(query.getQueryId())) {
                        queries.remove(query.getQueryId());
                    }
                }
                else {
                    query.failIfAbandoned();
                }
            }
            catch (RuntimeException e) {
                log.error(e, "Exception checking client query status %s", query.getQueryId());
            }
        }
    }

    @PreDestroy
    public synchronized void destroy()
    {
        if (backgroundTask == null) {
            return;
        }
        backgroundTask.cancel(true);
    }
}
