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

import com.google.common.collect.ImmutableList;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

@ThreadSafe
final class EventsCollector
{
    private final EventFilters eventFilters;
    @GuardedBy("this")
    private ImmutableList.Builder<QueryCreatedEvent> queryCreatedEvents;
    @GuardedBy("this")
    private ImmutableList.Builder<QueryCompletedEvent> queryCompletedEvents;
    @GuardedBy("this")
    private ImmutableList.Builder<SplitCompletedEvent> splitCompletedEvents;
    @GuardedBy("this")
    private CountDownLatch eventsLatch;

    public EventsCollector()
    {
        this(EventFilters.builder().build());
    }

    public EventsCollector(EventFilters eventFilters)
    {
        this.eventFilters = requireNonNull(eventFilters, "eventFilters is null");
        reset(0);
    }

    public synchronized void reset(int numEvents)
    {
        queryCreatedEvents = ImmutableList.builder();
        queryCompletedEvents = ImmutableList.builder();
        splitCompletedEvents = ImmutableList.builder();

        eventsLatch = new CountDownLatch(numEvents);
    }

    public void waitForEvents(int timeoutSeconds)
            throws InterruptedException
    {
        CountDownLatch eventsLatch;
        synchronized (this) {
            // since the eventsLatch is replaced in the reset method, a lock is required for proper memory visibility
            eventsLatch = this.eventsLatch;
        }
        eventsLatch.await(timeoutSeconds, TimeUnit.SECONDS);
    }

    public synchronized void addQueryCreated(QueryCreatedEvent event)
    {
        if (!eventFilters.getQueryCreatedFilter().test(event)) {
            return;
        }
        queryCreatedEvents.add(event);
        eventsLatch.countDown();
    }

    public synchronized void addQueryCompleted(QueryCompletedEvent event)
    {
        if (!eventFilters.getQueryCompletedFilter().test(event)) {
            return;
        }
        queryCompletedEvents.add(event);
        eventsLatch.countDown();
    }

    public synchronized void addSplitCompleted(SplitCompletedEvent event)
    {
        if (!eventFilters.getSplitCompletedFilter().test(event)) {
            return;
        }
        splitCompletedEvents.add(event);
        eventsLatch.countDown();
    }

    public synchronized List<QueryCreatedEvent> getQueryCreatedEvents()
    {
        return queryCreatedEvents.build();
    }

    public synchronized List<QueryCompletedEvent> getQueryCompletedEvents()
    {
        return queryCompletedEvents.build();
    }

    public synchronized List<SplitCompletedEvent> getSplitCompletedEvents()
    {
        return splitCompletedEvents.build();
    }

    @ThreadSafe
    public static class EventFilters
    {
        private final Predicate<QueryCreatedEvent> queryCreatedFilter;
        private final Predicate<QueryCompletedEvent> queryCompletedFilter;
        private final Predicate<SplitCompletedEvent> splitCompletedFilter;

        private EventFilters(
                Predicate<QueryCreatedEvent> queryCreatedFilter,
                Predicate<QueryCompletedEvent> queryCompletedFilter,
                Predicate<SplitCompletedEvent> splitCompletedFilter)
        {
            this.queryCreatedFilter = requireNonNull(queryCreatedFilter, "queryCreatedFilter is null");
            this.queryCompletedFilter = requireNonNull(queryCompletedFilter, "queryCompletedFilter is null");
            this.splitCompletedFilter = requireNonNull(splitCompletedFilter, "splitCompletedFilter is null");
        }

        Predicate<QueryCreatedEvent> getQueryCreatedFilter()
        {
            return queryCreatedFilter;
        }

        Predicate<QueryCompletedEvent> getQueryCompletedFilter()
        {
            return queryCompletedFilter;
        }

        Predicate<SplitCompletedEvent> getSplitCompletedFilter()
        {
            return splitCompletedFilter;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static class Builder
        {
            private Predicate<QueryCreatedEvent> queryCreatedFilter = queryCreatedEvent -> true;
            private Predicate<QueryCompletedEvent> queryCompletedFilter = queryCompletedEvent -> true;
            private Predicate<SplitCompletedEvent> splitCompletedFilter = splitCompletedEvent -> true;

            public Builder setQueryCreatedFilter(Predicate<QueryCreatedEvent> queryCreatedFilter)
            {
                this.queryCreatedFilter = queryCreatedFilter;
                return this;
            }

            public Builder setQueryCompletedFilter(Predicate<QueryCompletedEvent> queryCompletedFilter)
            {
                this.queryCompletedFilter = queryCompletedFilter;
                return this;
            }

            public Builder setSplitCompletedFilter(Predicate<SplitCompletedEvent> splitCompletedFilter)
            {
                this.splitCompletedFilter = splitCompletedFilter;
                return this;
            }

            public EventFilters build()
            {
                return new EventFilters(queryCreatedFilter, queryCompletedFilter, splitCompletedFilter);
            }
        }
    }
}
