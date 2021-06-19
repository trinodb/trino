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
import io.trino.spi.Plugin;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TestEventListenerPlugin
{
    static class TestingEventListenerPlugin
            implements Plugin
    {
        private final EventsCollector eventsCollector;

        public TestingEventListenerPlugin(EventsCollector eventsCollector)
        {
            this.eventsCollector = requireNonNull(eventsCollector, "eventsCollector is null");
        }

        @Override
        public Iterable<EventListenerFactory> getEventListenerFactories()
        {
            return ImmutableList.of(new TestingEventListenerFactory(eventsCollector));
        }
    }

    private static class TestingEventListenerFactory
            implements EventListenerFactory
    {
        private final EventsCollector eventsCollector;

        public TestingEventListenerFactory(EventsCollector eventsCollector)
        {
            this.eventsCollector = eventsCollector;
        }

        @Override
        public String getName()
        {
            return "test";
        }

        @Override
        public EventListener create(Map<String, String> config)
        {
            return new TestingEventListener(eventsCollector);
        }
    }
}
