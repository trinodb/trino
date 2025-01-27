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
package io.trino.testing.eventlistener;

import com.google.errorprone.annotations.ThreadSafe;
import io.trino.spi.QueryId;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@ThreadSafe
public class EventsCollector
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

    public synchronized void addSplitCompleted(SplitCompletedEvent event)
    {
        getQueryEvents(new QueryId(event.getQueryId())).addSplitCompleted(event);
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
}
