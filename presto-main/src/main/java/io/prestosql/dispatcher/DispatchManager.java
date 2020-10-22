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
import io.prestosql.Session;
import io.prestosql.execution.QueryIdGenerator;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.QueryManagerStats;
import io.prestosql.execution.QueryPreparer;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.execution.QueryTracker;
import io.prestosql.execution.resourcegroups.ResourceGroupManager;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.security.AccessControl;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.SessionContext;
import io.prestosql.server.SessionPropertyDefaults;
import io.prestosql.server.SessionSupplier;
import io.prestosql.server.protocol.Slug;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.SelectionContext;
import io.prestosql.spi.resourcegroups.SelectionCriteria;
import io.prestosql.transaction.TransactionManager;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.execution.QueryState.QUEUED;
import static io.prestosql.execution.QueryState.RUNNING;
import static io.prestosql.spi.StandardErrorCode.QUERY_TEXT_TOO_LARGE;
import static io.prestosql.util.StatementUtils.getQueryType;
import static io.prestosql.util.StatementUtils.isTransactionControlStatement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

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

    private final Executor queryExecutor;

    private final QueryTracker<DispatchQuery> queryTracker;

    private final QueryManagerStats stats = new QueryManagerStats();

    @Inject
    public DispatchManager(
            QueryIdGenerator queryIdGenerator,
            QueryPreparer queryPreparer,
            ResourceGroupManager<?> resourceGroupManager,
            DispatchQueryFactory dispatchQueryFactory,
            FailedDispatchQueryFactory failedDispatchQueryFactory,
            TransactionManager transactionManager,
            AccessControl accessControl,
            SessionSupplier sessionSupplier,
            SessionPropertyDefaults sessionPropertyDefaults,
            QueryManagerConfig queryManagerConfig,
            DispatchExecutor dispatchExecutor)
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

        requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.maxQueryLength = queryManagerConfig.getMaxQueryLength();

        this.queryExecutor = requireNonNull(dispatchExecutor, "dispatchExecutor is null").getExecutor();

        this.queryTracker = new QueryTracker<>(queryManagerConfig, dispatchExecutor.getScheduledExecutor());
    }

    @PostConstruct
    public void start()
    {
        queryTracker.start();
    }

    @PreDestroy
    public void stop()
    {
        queryTracker.stop();
    }

    @Managed
    @Flatten
    public QueryManagerStats getStats()
    {
        return stats;
    }

    public QueryId createQueryId()
    {
        return queryIdGenerator.createNextQueryId();
    }

    public ListenableFuture<?> createQuery(QueryId queryId, Slug slug, SessionContext sessionContext, String query)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(sessionContext, "sessionFactory is null");
        requireNonNull(query, "query is null");
        checkArgument(!query.isEmpty(), "query must not be empty string");
        checkArgument(queryTracker.tryGetQuery(queryId).isEmpty(), "query %s already exists", queryId);

        DispatchQueryCreationFuture queryCreationFuture = new DispatchQueryCreationFuture();
        queryExecutor.execute(() -> {
            try {
                createQueryInternal(queryId, slug, sessionContext, query, resourceGroupManager);
            }
            finally {
                queryCreationFuture.set(null);
            }
        });
        return queryCreationFuture;
    }

    /**
     * Creates and registers a dispatch query with the query tracker.  This method will never fail to register a query with the query
     * tracker.  If an error occurs while creating a dispatch query, a failed dispatch will be created and registered.
     */
    private <C> void createQueryInternal(QueryId queryId, Slug slug, SessionContext sessionContext, String query, ResourceGroupManager<C> resourceGroupManager)
    {
        Session session = null;
        PreparedQuery preparedQuery = null;
        try {
            if (query.length() > maxQueryLength) {
                int queryLength = query.length();
                query = query.substring(0, maxQueryLength);
                throw new PrestoException(QUERY_TEXT_TOO_LARGE, format("Query text length (%s) exceeds the maximum length (%s)", queryLength, maxQueryLength));
            }

            // decode session
            session = sessionSupplier.createSession(queryId, sessionContext);

            // check query execute permissions
            accessControl.checkCanExecuteQuery(sessionContext.getIdentity());

            // prepare query
            preparedQuery = queryPreparer.prepareQuery(session, query);

            // select resource group
            Optional<String> queryType = getQueryType(preparedQuery.getStatement().getClass()).map(Enum::name);
            SelectionContext<C> selectionContext = resourceGroupManager.selectGroup(new SelectionCriteria(
                    sessionContext.getIdentity().getPrincipal().isPresent(),
                    sessionContext.getIdentity().getUser(),
                    sessionContext.getIdentity().getGroups(),
                    Optional.ofNullable(sessionContext.getSource()),
                    sessionContext.getClientTags(),
                    sessionContext.getResourceEstimates(),
                    queryType));

            // apply system default session properties (does not override user set properties)
            session = sessionPropertyDefaults.newSessionWithDefaultProperties(session, queryType, selectionContext.getResourceGroupId());

            // mark existing transaction as active
            transactionManager.activateTransaction(session, isTransactionControlStatement(preparedQuery.getStatement()), accessControl);

            DispatchQuery dispatchQuery = dispatchQueryFactory.createDispatchQuery(
                    session,
                    query,
                    preparedQuery,
                    slug,
                    selectionContext.getResourceGroupId());

            boolean queryAdded = queryCreated(dispatchQuery);
            if (queryAdded && !dispatchQuery.isDone()) {
                try {
                    resourceGroupManager.submit(dispatchQuery, selectionContext, queryExecutor);
                }
                catch (Throwable e) {
                    // dispatch query has already been registered, so just fail it directly
                    dispatchQuery.fail(e);
                }
            }
        }
        catch (Throwable throwable) {
            // creation must never fail, so register a failed query in this case
            if (session == null) {
                session = Session.builder(new SessionPropertyManager())
                        .setQueryId(queryId)
                        .setIdentity(sessionContext.getIdentity())
                        .setSource(sessionContext.getSource())
                        .build();
            }
            Optional<String> preparedSql = Optional.ofNullable(preparedQuery).flatMap(PreparedQuery::getPrepareSql);
            DispatchQuery failedDispatchQuery = failedDispatchQueryFactory.createFailedDispatchQuery(session, query, preparedSql, Optional.empty(), throwable);
            queryCreated(failedDispatchQuery);
        }
    }

    private boolean queryCreated(DispatchQuery dispatchQuery)
    {
        boolean queryAdded = queryTracker.addQuery(dispatchQuery);

        // only add state tracking if this query instance will actually be used for the execution
        if (queryAdded) {
            dispatchQuery.addStateChangeListener(newState -> {
                if (newState.isDone()) {
                    // execution MUST be added to the expiration queue or there will be a leak
                    queryTracker.expireQuery(dispatchQuery.getQueryId());
                }
            });
            stats.trackQueryStats(dispatchQuery);
        }

        return queryAdded;
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

    @Managed
    public long getQueuedQueries()
    {
        return queryTracker.getAllQueries().stream()
                .filter(query -> query.getState() == QUEUED)
                .count();
    }

    @Managed
    public long getRunningQueries()
    {
        return queryTracker.getAllQueries().stream()
                .filter(query -> query.getState() == RUNNING && !query.getBasicQueryInfo().getQueryStats().isFullyBlocked())
                .count();
    }

    public boolean isQueryRegistered(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId).isPresent();
    }

    public DispatchQuery getQuery(QueryId queryId)
    {
        return queryTracker.getQuery(queryId);
    }

    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getBasicQueryInfo();
    }

    public Optional<QueryInfo> getFullQueryInfo(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId).map(DispatchQuery::getFullQueryInfo);
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

    public void failQuery(QueryId queryId, Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        queryTracker.tryGetQuery(queryId)
                .ifPresent(query -> query.fail(cause));
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
            // query submission cannot be canceled
            return false;
        }
    }
}
