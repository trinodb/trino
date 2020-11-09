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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryCreatedEvent;
import io.prestosql.spi.eventlistener.QueryMetadata;
import io.prestosql.spi.eventlistener.SplitCompletedEvent;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysTrue;
import static java.util.Objects.requireNonNull;

class EventsBuilder
{
    private final Predicate<QueryMetadata> queryFilter;
    private ImmutableList.Builder<QueryCreatedEvent> queryCreatedEvents;
    private ImmutableList.Builder<QueryCompletedEvent> queryCompletedEvents;
    private ImmutableList.Builder<SplitCompletedEvent> splitCompletedEvents;

    private CountDownLatch eventsLatch;

    public EventsBuilder()
    {
        this(alwaysTrue());
    }

    public EventsBuilder(Predicate<QueryMetadata> queryFilter)
    {
        this.queryFilter = requireNonNull(queryFilter, "filter is null");
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
        eventsLatch.await(timeoutSeconds, TimeUnit.SECONDS);
    }

    public synchronized void addQueryCreated(QueryCreatedEvent event)
    {
        if (!queryFilter.test(event.getMetadata())) {
            return;
        }
        queryCreatedEvents.add(event);
        eventsLatch.countDown();
    }

    public synchronized void addQueryCompleted(QueryCompletedEvent event)
    {
        if (!queryFilter.test(event.getMetadata())) {
            return;
        }
        queryCompletedEvents.add(event);
        eventsLatch.countDown();
    }

    public synchronized void addSplitCompleted(SplitCompletedEvent event)
    {
        splitCompletedEvents.add(event);
        eventsLatch.countDown();
    }

    public List<QueryCreatedEvent> getQueryCreatedEvents()
    {
        return queryCreatedEvents.build();
    }

    public List<QueryCompletedEvent> getQueryCompletedEvents()
    {
        return queryCompletedEvents.build();
    }

    public List<SplitCompletedEvent> getSplitCompletedEvents()
    {
        return splitCompletedEvents.build();
    }
}
