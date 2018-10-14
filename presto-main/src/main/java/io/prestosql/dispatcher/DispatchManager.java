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
package io.prestosql.dispatcher;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.prestosql.Session;
import io.prestosql.execution.QueryIdGenerator;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.QueryPreparer;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.execution.QueryTracker;
import io.prestosql.execution.SqlQueryManagerStats;
import io.prestosql.execution.resourcegroups.ResourceGroupManager;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.security.AccessControl;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.SessionContext;
import io.prestosql.server.SessionPropertyDefaults;
import io.prestosql.server.SessionSupplier;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.SelectionContext;
import io.prestosql.spi.resourcegroups.SelectionCriteria;
import io.prestosql.sql.SqlPath;
import io.prestosql.transaction.TransactionManager;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.prestosql.spi.StandardErrorCode.QUERY_TEXT_TOO_LARGE;
import static io.prestosql.util.StatementUtils.getQueryType;
import static io.prestosql.util.StatementUtils.isTransactionControlStatement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class DispatchManager
{
    private final QueryIdGenerator queryIdGenerator;
    private final QueryPreparer queryPreparer;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final DispatchQueryFactory dispatchQueryFactory;
    private final FailedDispatchQueryFactory failedDispatchQueryFactory;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final SessionSupplier sessionSupplier;
    private final SessionPropertyDefaults sessionPropertyDefaults;

    private final int maxQueryLength;

    private final ListeningScheduledExecutorService queryManagementExecutor;
    private final ThreadPoolExecutorMBean queryManagementExecutorMBean;

    private final QueryTracker<DispatchQuery> queryTracker;

    private final SqlQueryManagerStats stats = new SqlQueryManagerStats();

    @Inject
    public DispatchManager(
            QueryIdGenerator queryIdGenerator,
            QueryPreparer queryPreparer,
            @SuppressWarnings("rawtypes") ResourceGroupManager resourceGroupManager,
            DispatchQueryFactory dispatchQueryFactory,
            FailedDispatchQueryFactory failedDispatchQueryFactory,
            TransactionManager transactionManager,
            AccessControl accessControl,
            SessionSupplier sessionSupplier,
            SessionPropertyDefaults sessionPropertyDefaults,
            QueryManagerConfig queryManagerConfig)
    {
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.dispatchQueryFactory = requireNonNull(dispatchQueryFactory, "dispatchQueryFactory is null");
        this.failedDispatchQueryFactory = requireNonNull(failedDispatchQueryFactory, "failedDispatchQueryFactory is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.sessionPropertyDefaults = requireNonNull(sessionPropertyDefaults, "sessionPropertyDefaults is null");

        this.maxQueryLength = queryManagerConfig.getMaxQueryLength();

        ScheduledExecutorService scheduledExecutorService = newScheduledThreadPool(queryManagerConfig.getQueryManagerExecutorPoolSize(), threadsNamed("query-dispatch-%s"));
        queryManagementExecutor = listeningDecorator(scheduledExecutorService);
        queryManagementExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) scheduledExecutorService);

        this.queryTracker = new QueryTracker<>(queryManagerConfig, queryManagementExecutor);
    }

    @Managed(description = "Query dispatch executor")
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return queryManagementExecutorMBean;
    }

    @Managed
    @Flatten
    public SqlQueryManagerStats getStats()
    {
        return stats;
    }

    public QueryId createQueryId()
    {
        return queryIdGenerator.createNextQueryId();
    }

    public ListenableFuture<?> createQuery(QueryId queryId, String slug, SessionContext sessionContext, String query)
    {
        DispatchQueryCreationFuture queryCreationFuture = new DispatchQueryCreationFuture();
        queryManagementExecutor.submit(() -> {
            try {
                createQueryInternal(queryId, slug, sessionContext, query, resourceGroupManager);
                queryCreationFuture.set(null);
            }
            catch (Throwable e) {
                queryCreationFuture.setException(e);
            }
        });
        return queryCreationFuture;
    }

    private <C> void createQueryInternal(QueryId queryId, String slug, SessionContext sessionContext, String query, ResourceGroupManager<C> resourceGroupManager)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(sessionContext, "sessionFactory is null");
        requireNonNull(query, "query is null");
        checkArgument(!query.isEmpty(), "query must not be empty string");
        checkArgument(!queryTracker.tryGetQuery(queryId).isPresent(), "query %s already exists", queryId);

        Session session = null;
        SelectionContext<C> selectionContext = null;
        DispatchQuery dispatchQuery;
        try {
            if (query.length() > maxQueryLength) {
                int queryLength = query.length();
                query = query.substring(0, maxQueryLength);
                throw new PrestoException(QUERY_TEXT_TOO_LARGE, format("Query text length (%s) exceeds the maximum length (%s)", queryLength, maxQueryLength));
            }

            // decode session
            session = sessionSupplier.createSession(queryId, sessionContext);

            // prepare query
            PreparedQuery preparedQuery = queryPreparer.prepareQuery(session, query);

            // select resource group
            Optional<String> queryType = getQueryType(preparedQuery.getStatement().getClass()).map(Enum::name);
            selectionContext = resourceGroupManager.selectGroup(new SelectionCriteria(
                    sessionContext.getIdentity().getPrincipal().isPresent(),
                    sessionContext.getIdentity().getUser(),
                    Optional.ofNullable(sessionContext.getSource()),
                    sessionContext.getClientTags(),
                    sessionContext.getResourceEstimates(),
                    queryType));

            // apply system default session properties (does not override user set properties)
            session = sessionPropertyDefaults.newSessionWithDefaultProperties(session, queryType, selectionContext.getResourceGroupId());

            // mark existing transaction as active
            transactionManager.activateTransaction(session, isTransactionControlStatement(preparedQuery.getStatement()), accessControl);

            dispatchQuery = dispatchQueryFactory.createDispatchQuery(session, query, preparedQuery, slug, selectionContext.getResourceGroupId(), queryManagementExecutor);
        }
        catch (RuntimeException e) {
            // This is intentionally not a method, since after the state change listener is registered
            // it's not safe to do any of this, and we had bugs before where people reused this code in a method
            // if session creation failed, create a minimal session object
            if (session == null) {
                session = Session.builder(new SessionPropertyManager())
                        .setQueryId(queryId)
                        .setIdentity(sessionContext.getIdentity())
                        .setSource(sessionContext.getSource())
                        .setPath(new SqlPath(Optional.empty()))
                        .build();
            }

            // create and immediately fail the query
            DispatchQuery failedDispatchQuery = failedDispatchQueryFactory.createFailedDispatchQuery(
                    session,
                    query,
                    Optional.ofNullable(selectionContext).map(SelectionContext::getResourceGroupId),
                    e);

            try {
                queryCreated(failedDispatchQuery);
            }
            finally {
                handleQueryFailure(failedDispatchQuery);
            }
            return;
        }

        try {
            queryCreated(dispatchQuery);
            dispatchQuery.addStateChangeListener(newState -> {
                if (newState.isDone()) {
                    stats.queryFinished(dispatchQuery.getBasicQueryInfo());
                }
            });

            resourceGroupManager.submit(dispatchQuery, selectionContext, queryManagementExecutor);
        }
        catch (RuntimeException e) {
            dispatchQuery.fail(e);
        }
    }

    private void queryCreated(DispatchQuery dispatchQuery)
    {
        queryTracker.addQuery(dispatchQuery);
        stats.queryQueued();
    }

    private void handleQueryFailure(DispatchQuery dispatchQuery)
    {
        try {
            stats.queryStarted();
            stats.queryStopped();
            BasicQueryInfo queryInfo = dispatchQuery.getBasicQueryInfo();
            stats.queuedQueryFailed(queryInfo.getQueryStats().getQueuedTime(), Optional.ofNullable(queryInfo.getErrorCode()));
        }
        finally {
            // execution MUST be added to the expiration queue or there will be a leak
            queryTracker.expireQuery(dispatchQuery.getQueryId());
        }
    }

    public ListenableFuture<?> waitForDispatched(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(dispatchQuery -> {
                    dispatchQuery.recordHeartbeat();
                    return dispatchQuery.getDispatchedFuture();
                })
                .orElseGet(() -> immediateFuture(null));
    }

    public List<BasicQueryInfo> getQueries()
    {
        return queryTracker.getAllQueries().stream()
                .map(DispatchQuery::getBasicQueryInfo)
                .collect(toImmutableList());
    }

    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        DispatchQuery dispatchQuery = queryTracker.getQuery(queryId);
        dispatchQuery.recordHeartbeat();
        return dispatchQuery.getBasicQueryInfo();
    }

    public Optional<DispatchInfo> getDispatchInfo(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(dispatchQuery -> {
                    dispatchQuery.recordHeartbeat();
                    return dispatchQuery.getDispatchInfo();
                });
    }

    public void cancelQuery(QueryId queryId)
    {
        queryTracker.tryGetQuery(queryId)
                .ifPresent(DispatchQuery::cancel);
    }

    private static class DispatchQueryCreationFuture
            extends AbstractFuture<QueryInfo>
    {
        @Override
        protected boolean set(QueryInfo value)
        {
            return super.set(value);
        }

        @Override
        protected boolean setException(Throwable throwable)
        {
            return super.setException(throwable);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            // query submission can not be canceled
            return false;
        }
    }
}
