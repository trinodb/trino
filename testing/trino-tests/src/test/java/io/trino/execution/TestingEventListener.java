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

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;

class TestingEventListener
        implements EventListener
{
    private final EventsCollector eventsCollector;

    public TestingEventListener(EventsCollector eventsCollector)
    {
        this.eventsCollector = eventsCollector;
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        eventsCollector.addQueryCreated(queryCreatedEvent);
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        eventsCollector.addQueryCompleted(queryCompletedEvent);
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        eventsCollector.addSplitCompleted(splitCompletedEvent);
    }

    @Override
    public boolean requiresAnonymizedPlan()
    {
        return eventsCollector.requiresAnonymizedPlan();
    }
}
