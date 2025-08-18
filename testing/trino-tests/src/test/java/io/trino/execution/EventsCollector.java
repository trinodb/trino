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

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.units.Duration;
import io.trino.spi.QueryId;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
final class EventsCollector
{
    private final ConcurrentHashMap<QueryId, QueryEvents> queryEvents = new ConcurrentHashMap<>();
    private final AtomicBoolean requiresAnonymizedPlan = new AtomicBoolean(false);

    public synchronized void addQueryCreated(QueryCreatedEvent event)
    {
        getQueryEvents(new QueryId(event.getMetadata().getQueryId())).addQueryCreated(event);
    }

    public synchronized void addQueryCompleted(QueryCompletedEvent event)
    {
        getQueryEvents(new QueryId(event.getMetadata().getQueryId())).addQueryCompleted(event);
    }

    public void setRequiresAnonymizedPlan(boolean value)
    {
        requiresAnonymizedPlan.set(value);
    }

    public boolean requiresAnonymizedPlan()
    {
        return requiresAnonymizedPlan.get();
    }

    public QueryEvents getQueryEvents(QueryId queryId)
    {
        return queryEvents.computeIfAbsent(queryId, _ -> new QueryEvents());
    }

    @ThreadSafe
    public static class QueryEvents
    {
        @GuardedBy("this")
        private QueryCreatedEvent queryCreatedEvent;
        @GuardedBy("this")
        private QueryCompletedEvent queryCompletedEvent;
        @GuardedBy("this")
        private final CountDownLatch queryCompleteLatch = new CountDownLatch(1);

        @GuardedBy("this")
        private final List<Exception> failures = new ArrayList<>();

        public synchronized QueryCreatedEvent getQueryCreatedEvent()
        {
            checkFailure();
            if (queryCreatedEvent == null) {
                throw new IllegalStateException("QueryCreatedEvent has not been set");
            }
            return queryCreatedEvent;
        }

        public synchronized QueryCompletedEvent getQueryCompletedEvent()
        {
            checkFailure();
            if (queryCompletedEvent == null) {
                throw new IllegalStateException("QueryCompletedEvent has not been set");
            }
            return queryCompletedEvent;
        }

        private synchronized void addQueryCreated(QueryCreatedEvent event)
        {
            requireNonNull(event, "event is null");
            if (queryCreatedEvent != null) {
                failures.add(new RuntimeException("QueryCreateEvent already set"));
                return;
            }
            queryCreatedEvent = event;

            if (queryCompletedEvent != null) {
                queryCompleteLatch.countDown();
            }
        }

        private synchronized void addQueryCompleted(QueryCompletedEvent event)
        {
            requireNonNull(event, "event is null");
            if (queryCompletedEvent != null) {
                failures.add(new RuntimeException("QueryCompletedEvent already set"));
                return;
            }
            queryCompletedEvent = event;

            if (queryCreatedEvent != null) {
                queryCompleteLatch.countDown();
            }
        }

        public void waitForQueryCompletion(Duration timeout)
                throws InterruptedException, TimeoutException
        {
            CountDownLatch latch;
            synchronized (this) {
                latch = queryCompleteLatch;
            }

            boolean finished = latch.await(timeout.toMillis(), MILLISECONDS);
            if (!finished) {
                synchronized (this) {
                    TimeoutException exception = new TimeoutException("Query did not complete in %s. Currently, queryCreatedEvent=%s queryCompletedEvent=%s queryCompleteLatch=%s"
                                    .formatted(timeout, queryCreatedEvent, queryCompletedEvent, queryCompleteLatch));
                    failures.forEach(exception::addSuppressed);
                    throw exception;
                }
            }
        }

        private synchronized void checkFailure()
        {
            if (failures.isEmpty()) {
                return;
            }
            RuntimeException exception = new RuntimeException("Event collection failed");
            failures.forEach(exception::addSuppressed);
        }
    }
}
