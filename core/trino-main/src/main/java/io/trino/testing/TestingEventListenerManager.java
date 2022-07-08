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
package io.trino.testing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TestingEventListenerManager
        extends EventListenerManager
{
    public static TestingEventListenerManager emptyEventListenerManager()
    {
        return new TestingEventListenerManager(new EventListenerConfig());
    }

    private final Set<EventListener> configuredEventListeners = Collections.synchronizedSet(new HashSet<>());

    @Inject
    public TestingEventListenerManager(EventListenerConfig config)
    {
        super(config);
    }

    @Override
    public void addEventListenerFactory(EventListenerFactory eventListenerFactory)
    {
        configuredEventListeners.add(eventListenerFactory.create(ImmutableMap.of()));
    }

    @Override
    public void addEventListener(EventListener eventListener)
    {
        this.configuredEventListeners.add(eventListener);
    }

    @Override
    public void queryCompleted(Function<Boolean, QueryCompletedEvent> queryCompletedEventProvider)
    {
        for (EventListener listener : configuredEventListeners) {
            listener.queryCompleted(queryCompletedEventProvider.apply(listener.requiresAnonymizedPlan()));
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        for (EventListener listener : configuredEventListeners) {
            listener.queryCreated(queryCreatedEvent);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        for (EventListener listener : configuredEventListeners) {
            listener.splitCompleted(splitCompletedEvent);
        }
    }

    @VisibleForTesting
    public Set<EventListener> getConfiguredEventListeners()
    {
        return configuredEventListeners;
    }
}
