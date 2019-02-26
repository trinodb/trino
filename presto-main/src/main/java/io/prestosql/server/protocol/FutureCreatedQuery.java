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

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.execution.QueryExecution;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.spi.StandardErrorCode.ABANDONED_QUERY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class FutureCreatedQuery
{
    private final long submittedNanoTime = System.nanoTime();

    private final QueryId queryId;
    private final Supplier<QueryExecution> queryExecutionSupplier;
    private final Function<Throwable, QueryExecution> failedQueryExecutionFactory;
    private final ListeningExecutorService executor;

    @GuardedBy("this")
    private ListenableFuture<QueryExecution> queryCreatedFuture;

    // Use a separate future for each listener so canceled listeners can be removed
    @GuardedBy("this")
    private final Set<SettableFuture<QueryExecution>> listeners = new HashSet<>();

    public FutureCreatedQuery(
            QueryId queryId,
            ExecutorService executor,
            Supplier<QueryExecution> queryExecutionSupplier,
            Function<Throwable, QueryExecution> failedQueryExecutionFactory)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(executor, "executor is null");
        requireNonNull(queryExecutionSupplier, "queryExecutionSupplier is null");
        requireNonNull(failedQueryExecutionFactory, "failedQueryExecutionFactory is null");

        this.queryId = queryId;
        this.executor = listeningDecorator(executor);
        this.queryExecutionSupplier = queryExecutionSupplier;
        this.failedQueryExecutionFactory = failedQueryExecutionFactory;
    }

    public synchronized void startQueryCreation()
    {
        if (queryCreatedFuture != null) {
            return;
        }
        this.queryCreatedFuture = this.executor.submit(queryExecutionSupplier::get);
        addSuccessCallback(queryCreatedFuture, this::queryCreated);
    }

    public synchronized void failIfAbandoned()
    {
        if (queryCreatedFuture == null && TimeUnit.NANOSECONDS.toMinutes(System.nanoTime() - submittedNanoTime) > 5) {
            PrestoException prestoException = new PrestoException(
                    ABANDONED_QUERY,
                    format(
                            "Query %s has not been accessed in %s",
                            queryId,
                            nanosSince(submittedNanoTime)));
            this.queryCreatedFuture = this.executor.submit(() -> failedQueryExecutionFactory.apply(prestoException));
            addSuccessCallback(queryCreatedFuture, this::queryCreated);
        }
    }

    public synchronized boolean isQueryCreated()
    {
        return queryCreatedFuture != null && queryCreatedFuture.isDone();
    }

    public ListenableFuture<QueryExecution> createNewListener()
    {
        SettableFuture<QueryExecution> future;
        synchronized (this) {
            if (isQueryCreated()) {
                return immediateFuture(getDone(queryCreatedFuture));
            }
            future = SettableFuture.create();
            listeners.add(future);
        }

        // remove the future when the future completes
        future.addListener(
                () -> {
                    synchronized (this) {
                        listeners.remove(future);
                    }
                },
                directExecutor());

        return future;
    }

    private void queryCreated(QueryExecution queryExecution)
    {
        Set<SettableFuture<QueryExecution>> futures;
        synchronized (this) {
            futures = ImmutableSet.copyOf(listeners);
            listeners.clear();
        }

        // set the futures outside of lock to in case direct executor was used
        for (SettableFuture<QueryExecution> future : futures) {
            executor.execute(() -> future.set(queryExecution));
        }
    }
}
