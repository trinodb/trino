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
package io.trino.dispatcher;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.Session;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.QueryManagerStats;
import io.trino.execution.QueryPreparer;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.QueryTracker;
import io.trino.execution.resourcegroups.ResourceGroupManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.server.BasicQueryInfo;
import io.trino.server.SessionContext;
import io.trino.server.SessionPropertyDefaults;
import io.trino.server.SessionSupplier;
import io.trino.server.protocol.Slug;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;
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
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.spi.StandardErrorCode.QUERY_TEXT_TOO_LARGE;
import static io.trino.util.StatementUtils.getQueryType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DispatchManager
{
    private final QueryIdGenerator queryIdGenerator;
    private final QueryPreparer queryPreparer;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final DispatchQueryFactory dispatchQueryFactory;
    private final FailedDispatchQueryFactory failedDispatchQueryFactory;
    private final AccessControl accessControl;
    private final SessionSupplier sessionSupplier;
    private final SessionPropertyDefaults sessionPropertyDefaults;
    private final SessionPropertyManager sessionPropertyManager;

    private final int maxQueryLength;

    private final Executor dispatchExecutor;

    private final QueryTracker<DispatchQuery> queryTracker;

    private final QueryManagerStats stats = new QueryManagerStats();

    @Inject
    public DispatchManager(
            QueryIdGenerator queryIdGenerator,
            QueryPreparer queryPreparer,
            ResourceGroupManager<?> resourceGroupManager,
            DispatchQueryFactory dispatchQueryFactory,
            FailedDispatchQueryFactory failedDispatchQueryFactory,
            AccessControl accessControl,
            SessionSupplier sessionSupplier,
            SessionPropertyDefaults sessionPropertyDefaults,
            SessionPropertyManager sessionPropertyManager,
            QueryManagerConfig queryManagerConfig,
            DispatchExecutor dispatchExecutor)
    {
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.dispatchQueryFactory = requireNonNull(dispatchQueryFactory, "dispatchQueryFactory is null");
        this.failedDispatchQueryFactory = requireNonNull(failedDispatchQueryFactory, "failedDispatchQueryFactory is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.sessionPropertyDefaults = requireNonNull(sessionPropertyDefaults, "sessionPropertyDefaults is null");
        this.sessionPropertyManager = sessionPropertyManager;

        requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.maxQueryLength = queryManagerConfig.getMaxQueryLength();

        this.dispatchExecutor = requireNonNull(dispatchExecutor, "dispatchExecutor is null").getExecutor();

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

    public ListenableFuture<Void> createQuery(QueryId queryId, Slug slug, SessionContext sessionContext, String query)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(sessionContext, "sessionContext is null");
        requireNonNull(query, "query is null");
        checkArgument(!query.isEmpty(), "query must not be empty string");
        checkArgument(queryTracker.tryGetQuery(queryId).isEmpty(), "query %s already exists", queryId);

        // It is important to return a future implementation which ignores cancellation request.
        // Using NonCancellationPropagatingFuture is not enough; it does not propagate cancel to wrapped future
        // but it would still return true on call to isCancelled() after cancel() is called on it.
        DispatchQueryCreationFuture queryCreationFuture = new DispatchQueryCreationFuture();
        dispatchExecutor.execute(() -> {
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
                throw new TrinoException(QUERY_TEXT_TOO_LARGE, format("Query text length (%s) exceeds the maximum length (%s)", queryLength, maxQueryLength));
            }

            // decode session
            session = sessionSupplier.createSession(queryId, sessionContext);

            // check query execute permissions
            accessControl.checkCanExecuteQuery(sessionContext.getIdentity());

            // prepare query
            preparedQuery = queryPreparer.prepareQuery(session, query);

            // select resource group
            Optional<String> queryType = getQueryType(preparedQuery.getStatement()).map(Enum::name);
            SelectionContext<C> selectionContext = resourceGroupManager.selectGroup(new SelectionCriteria(
                    sessionContext.getIdentity().getPrincipal().isPresent(),
                    sessionContext.getIdentity().getUser(),
                    sessionContext.getIdentity().getGroups(),
                    sessionContext.getSource(),
                    sessionContext.getClientTags(),
                    sessionContext.getResourceEstimates(),
                    queryType));

            // apply system default session properties (does not override user set properties)
            session = sessionPropertyDefaults.newSessionWithDefaultProperties(session, queryType, selectionContext.getResourceGroupId());

            DispatchQuery dispatchQuery = dispatchQueryFactory.createDispatchQuery(
                    session,
                    sessionContext.getTransactionId(),
                    query,
                    preparedQuery,
                    slug,
                    selectionContext.getResourceGroupId());

            boolean queryAdded = queryCreated(dispatchQuery);
            if (queryAdded && !dispatchQuery.isDone()) {
                try {
                    resourceGroupManager.submit(dispatchQuery, selectionContext, dispatchExecutor);
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
                session = Session.builder(sessionPropertyManager)
                        .setQueryId(queryId)
                        .setIdentity(sessionContext.getIdentity())
                        .setSource(sessionContext.getSource().orElse(null))
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

    public ListenableFuture<Void> waitForDispatched(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(dispatchQuery -> {
                    dispatchQuery.recordHeartbeat();
                    return dispatchQuery.getDispatchedFuture();
                })
                .orElseGet(Futures::immediateVoidFuture);
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
            extends AbstractFuture<Void>
    {
        @Override
        protected boolean set(Void value)
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
