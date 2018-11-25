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
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.event.QueryMonitor;
import io.prestosql.execution.FailedQueryExecution;
import io.prestosql.execution.ForQueryExecution;
import io.prestosql.execution.LocationFactory;
import io.prestosql.execution.QueryExecution;
import io.prestosql.execution.QueryExecution.QueryExecutionFactory;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.execution.QueryStateMachine;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.execution.warnings.WarningCollectorFactory;
import io.prestosql.memory.context.SimpleLocalMemoryContext;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.ExchangeClient;
import io.prestosql.operator.ExchangeClientSupplier;
import io.prestosql.security.AccessControl;
import io.prestosql.server.ForStatementResource;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.sql.analyzer.Analyzer;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Statement;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionManager;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.net.URI;
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
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.server.protocol.Query.createQuery;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.util.StatementUtils.isTransactionControlStatement;
import static java.util.Objects.requireNonNull;

public class QuerySubmissionManager
{
    private static final Logger log = Logger.get(QuerySubmissionManager.class);

    private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();

    private final QueryManager queryManager;

    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;
    private final LocationFactory locationFactory;
    private final SqlParser sqlParser;
    private final QueryExplainer queryExplainer;
    private final Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories;
    private final WarningCollectorFactory warningCollectorFactory;

    private final ExchangeClientSupplier exchangeClientSupplier;
    private final BlockEncodingSerde blockEncodingSerde;

    private final ListeningExecutorService queryCreationExecutor;
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
            SqlParser sqlParser,
            QueryExplainer queryExplainer,
            Map<Class<? extends Statement>, QueryExecutionFactory<?>> executionFactories,
            WarningCollectorFactory warningCollectorFactory,
            ExchangeClientSupplier exchangeClientSupplier,
            BlockEncodingSerde blockEncodingSerde,
            @ForQueryExecution ExecutorService queryCreationExecutor,
            @ForStatementResource BoundedExecutor resultsProcessorExecutor,
            @ForStatementResource ScheduledExecutorService timeoutExecutor)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
        this.executionFactories = requireNonNull(executionFactories, "executionFactories is null");
        this.warningCollectorFactory = requireNonNull(warningCollectorFactory, "warningCollectorFactory is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.queryCreationExecutor = listeningDecorator(requireNonNull(queryCreationExecutor, "queryCreationExecutor is null"));
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

    public void submitQuery(
            Session session,
            String sql,
            String slug,
            Duration queryElapsedTime,
            Duration queryQueueDuration,
            PreparedQuery preparedQuery,
            ResourceGroupId resourceGroup)
    {
        queries.computeIfAbsent(session.getQueryId(), id -> {
            ExchangeClient exchangeClient = exchangeClientSupplier.get(new SimpleLocalMemoryContext(
                    newSimpleAggregatedMemoryContext(),
                    ExecutingStatementResource.class.getSimpleName()));
            return createQuery(
                    session.getQueryId(),
                    slug,
                    queryElapsedTime,
                    queryQueueDuration,
                    queryManager,
                    () -> createAndSubmitQueryExecution(session, sql, preparedQuery, resourceGroup),
                    throwable -> {
                        QueryExecution queryExecution = createFailedQueryExecution(session, sql, resourceGroup, throwable);
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

    private QueryExecution createAndSubmitQueryExecution(
            Session session,
            String query,
            PreparedQuery preparedQuery,
            ResourceGroupId resourceGroup)
    {
        try {
            WarningCollector warningCollector = warningCollectorFactory.create();
            QueryStateMachine stateMachine = QueryStateMachine.begin(
                    query,
                    session,
                    locationFactory.createQueryLocation(session.getQueryId()),
                    resourceGroup,
                    isTransactionControlStatement(preparedQuery.getStatement()),
                    transactionManager,
                    accessControl,
                    queryCreationExecutor,
                    metadata,
                    warningCollector);

            QueryExecutionFactory<?> queryExecutionFactory = executionFactories.get(preparedQuery.getStatement().getClass());
            if (queryExecutionFactory == null) {
                throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type: " + preparedQuery.getStatement().getClass().getSimpleName());
            }

            QueryExecution queryExecution = queryExecutionFactory.createQueryExecution(preparedQuery, stateMachine, warningCollector);
            return submitQueryExecution(queryExecution);
        }
        catch (Throwable e) {
            QueryExecution failedQueryExecution = createFailedQueryExecution(session, query, resourceGroup, e);
            return submitQueryExecution(failedQueryExecution);
        }
    }

    private QueryExecution createFailedQueryExecution(Session session, String query, ResourceGroupId resourceGroup, Throwable throwable)
    {
        // this must never fail
        URI self;
        try {
            self = locationFactory.createQueryLocation(session.getQueryId());
        }
        catch (Throwable ignored) {
            self = URI.create("failed://" + session.getQueryId());
        }
        return new FailedQueryExecution(session, query, self, Optional.of(resourceGroup), queryCreationExecutor, throwable);
    }

    public void analyzeQuery(Session session, PreparedQuery preparedQuery)
    {
        // skip transaction control statements
        boolean transactionControl = isTransactionControlStatement(preparedQuery.getStatement());
        if (transactionControl) {
            return;
        }

        // If there is an existing transaction, activate it, otherwise, begin an auto commit transaction
        boolean autoCommit;
        TransactionId transactionId;
        if (session.getTransactionId().isPresent()) {
            transactionId = session.getTransactionId().get();
            transactionManager.activateTransaction(session, false, accessControl);
            autoCommit = false;
        }
        else {
            transactionId = transactionManager.beginTransaction(true);
            session = session.beginTransactionId(transactionId, transactionManager, accessControl);
            autoCommit = true;
        }

        try {
            Analyzer analyzer = new Analyzer(
                    session,
                    metadata,
                    sqlParser,
                    accessControl,
                    Optional.of(queryExplainer),
                    preparedQuery.getParameters(),
                    WarningCollector.NOOP);
            analyzer.analyze(preparedQuery.getStatement());
        }
        finally {
            if (autoCommit) {
                transactionManager.asyncAbort(transactionId);
            }
            else {
                transactionManager.trySetInactive(transactionId);
            }
        }
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
