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
package io.trino.execution;

import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.ExceededCpuLimitException;
import io.trino.ExceededScanLimitException;
import io.trino.Session;
import io.trino.execution.QueryExecution.QueryOutputInfo;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.memory.ClusterMemoryManager;
import io.trino.server.BasicQueryInfo;
import io.trino.server.protocol.Slug;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.sql.planner.Plan;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.SystemSessionProperties.getQueryMaxCpuTime;
import static io.trino.SystemSessionProperties.getQueryMaxScanPhysicalBytes;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@ThreadSafe
public class SqlQueryManager
        implements QueryManager
{
    private static final Logger log = Logger.get(SqlQueryManager.class);

    private final ClusterMemoryManager memoryManager;
    private final QueryTracker<QueryExecution> queryTracker;

    private final Duration maxQueryCpuTime;
    private final Optional<DataSize> maxQueryScanPhysicalBytes;

    private final ExecutorService queryExecutor;
    private final ThreadPoolExecutorMBean queryExecutorMBean;

    private final ScheduledExecutorService queryManagementExecutor;
    private final ThreadPoolExecutorMBean queryManagementExecutorMBean;

    @Inject
    public SqlQueryManager(ClusterMemoryManager memoryManager, QueryManagerConfig queryManagerConfig)
    {
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");

        this.maxQueryCpuTime = queryManagerConfig.getQueryMaxCpuTime();
        this.maxQueryScanPhysicalBytes = queryManagerConfig.getQueryMaxScanPhysicalBytes();

        this.queryExecutor = newCachedThreadPool(threadsNamed("query-scheduler-%s"));
        this.queryExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) queryExecutor);

        this.queryManagementExecutor = newScheduledThreadPool(queryManagerConfig.getQueryManagerExecutorPoolSize(), threadsNamed("query-management-%s"));
        this.queryManagementExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) queryManagementExecutor);

        this.queryTracker = new QueryTracker<>(queryManagerConfig, queryManagementExecutor);
    }

    @PostConstruct
    public void start()
    {
        queryTracker.start();
        queryManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                enforceMemoryLimits();
            }
            catch (Throwable e) {
                log.error(e, "Error enforcing memory limits");
            }

            try {
                enforceCpuLimits();
            }
            catch (Throwable e) {
                log.error(e, "Error enforcing query CPU time limits");
            }

            try {
                enforceScanLimits();
            }
            catch (Throwable e) {
                log.error(e, "Error enforcing query scan bytes limits");
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        queryTracker.stop();
        queryManagementExecutor.shutdownNow();
        queryExecutor.shutdownNow();
    }

    @Override
    public List<BasicQueryInfo> getQueries()
    {
        return queryTracker.getAllQueries().stream()
                .map(queryExecution -> {
                    try {
                        return queryExecution.getBasicQueryInfo();
                    }
                    catch (RuntimeException ignored) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    @Override
    public void setOutputInfoListener(QueryId queryId, Consumer<QueryOutputInfo> listener)
    {
        requireNonNull(listener, "listener is null");

        queryTracker.getQuery(queryId).setOutputInfoListener(listener);
    }

    @Override
    public void outputTaskFailed(TaskId taskId, Throwable failure)
    {
        queryTracker.getQuery(taskId.getQueryId()).outputTaskFailed(taskId, failure);
    }

    @Override
    public void resultsConsumed(QueryId queryId)
    {
        queryTracker.getQuery(queryId).resultsConsumed();
    }

    @Override
    public void addStateChangeListener(QueryId queryId, StateChangeListener<QueryState> listener)
    {
        requireNonNull(listener, "listener is null");

        queryTracker.getQuery(queryId).addStateChangeListener(listener);
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryId queryId, QueryState currentState)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(query -> query.getStateChange(currentState))
                .orElseGet(() -> immediateFailedFuture(new NoSuchElementException()));
    }

    @Override
    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getBasicQueryInfo();
    }

    @Override
    public QueryInfo getFullQueryInfo(QueryId queryId)
            throws NoSuchElementException
    {
        return queryTracker.getQuery(queryId).getQueryInfo();
    }

    @Override
    public Session getQuerySession(QueryId queryId)
            throws NoSuchElementException
    {
        return queryTracker.getQuery(queryId).getSession();
    }

    @Override
    public Slug getQuerySlug(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getSlug();
    }

    public Plan getQueryPlan(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getQueryPlan();
    }

    public void addFinalQueryInfoListener(QueryId queryId, StateChangeListener<QueryInfo> stateChangeListener)
    {
        queryTracker.getQuery(queryId).addFinalQueryInfoListener(stateChangeListener);
    }

    @Override
    public QueryState getQueryState(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getState();
    }

    @Override
    public void recordHeartbeat(QueryId queryId)
    {
        queryTracker.tryGetQuery(queryId)
                .ifPresent(QueryExecution::recordHeartbeat);
    }

    @Override
    public void createQuery(QueryExecution queryExecution)
    {
        requireNonNull(queryExecution, "queryExecution is null");

        if (!queryTracker.addQuery(queryExecution)) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Query %s already registered", queryExecution.getQueryId()));
        }

        queryExecution.addFinalQueryInfoListener(finalQueryInfo -> {
            // execution MUST be added to the expiration queue or there will be a leak
            queryTracker.expireQuery(queryExecution.getQueryId());
        });

        queryExecution.start();
    }

    @Override
    public void failQuery(QueryId queryId, Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        queryTracker.tryGetQuery(queryId)
                .ifPresent(query -> query.fail(cause));
    }

    @Override
    public void cancelQuery(QueryId queryId)
    {
        log.debug("Cancel query %s", queryId);

        queryTracker.tryGetQuery(queryId)
                .ifPresent(QueryExecution::cancelQuery);
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");

        log.debug("Cancel stage %s", stageId);

        queryTracker.tryGetQuery(stageId.getQueryId())
                .ifPresent(query -> query.cancelStage(stageId));
    }

    @Managed(description = "Query scheduler executor")
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return queryExecutorMBean;
    }

    @Managed(description = "Query query management executor")
    @Nested
    public ThreadPoolExecutorMBean getManagementExecutor()
    {
        return queryManagementExecutorMBean;
    }

    /**
     * Enforce memory limits at the query level
     */
    private void enforceMemoryLimits()
    {
        List<QueryExecution> runningQueries = queryTracker.getAllQueries().stream()
                .filter(query -> query.getState() == RUNNING)
                .collect(toImmutableList());
        memoryManager.process(runningQueries, this::getQueries);
    }

    /**
     * Enforce query CPU time limits
     */
    private void enforceCpuLimits()
    {
        for (QueryExecution query : queryTracker.getAllQueries()) {
            Duration cpuTime = query.getTotalCpuTime();
            Duration sessionLimit = getQueryMaxCpuTime(query.getSession());
            Duration limit = Ordering.natural().min(maxQueryCpuTime, sessionLimit);
            if (cpuTime.compareTo(limit) > 0) {
                query.fail(new ExceededCpuLimitException(limit));
            }
        }
    }

    /**
     * Enforce query scan physical bytes limits
     */
    private void enforceScanLimits()
    {
        for (QueryExecution query : queryTracker.getAllQueries()) {
            Optional<DataSize> limitOpt = getQueryMaxScanPhysicalBytes(query.getSession());
            if (maxQueryScanPhysicalBytes.isPresent()) {
                limitOpt = limitOpt
                        .flatMap(sessionLimit -> maxQueryScanPhysicalBytes.map(serverLimit -> Ordering.natural().min(serverLimit, sessionLimit)))
                        .or(() -> maxQueryScanPhysicalBytes);
            }

            limitOpt.ifPresent(limit -> {
                DataSize scan = query.getBasicQueryInfo().getQueryStats().getPhysicalInputDataSize();
                if (scan.compareTo(limit) > 0) {
                    query.fail(new ExceededScanLimitException(limit));
                }
            });
        }
    }
}
