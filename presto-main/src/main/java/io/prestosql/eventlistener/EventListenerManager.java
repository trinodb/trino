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
package io.prestosql.eventlistener;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryCreatedEvent;
import io.prestosql.spi.eventlistener.SplitCompletedEvent;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.util.PropertiesUtil.loadProperties;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class EventListenerManager
{
    private static final Logger log = Logger.get(EventListenerManager.class);

    private static final File CONFIG_FILE = new File("etc/event-listener.properties");
    private static final String NAME_PROPERTY = "event-listener.name";

    private final Map<String, EventListenerFactory> eventListenerFactories = new ConcurrentHashMap<>();
    private final AtomicReference<Optional<EventListener>> configuredEventListener = new AtomicReference<>(Optional.empty());

    public void addEventListenerFactory(EventListenerFactory eventListenerFactory)
    {
        requireNonNull(eventListenerFactory, "eventListenerFactory is null");

        if (eventListenerFactories.putIfAbsent(eventListenerFactory.getName(), eventListenerFactory) != null) {
            throw new IllegalArgumentException(format("Event listener '%s' is already registered", eventListenerFactory.getName()));
        }
    }

    public void loadConfiguredEventListener()
            throws Exception
    {
        File configFile = CONFIG_FILE.getAbsoluteFile();
        if (!configFile.exists()) {
            return;
        }

        Map<String, String> properties = new HashMap<>(loadProperties(configFile));

        String name = properties.remove(NAME_PROPERTY);
        checkState(!isNullOrEmpty(name), "Access control configuration %s does not contain '%s'", configFile, NAME_PROPERTY);

        setConfiguredEventListener(name, properties);
    }

    @VisibleForTesting
    protected void setConfiguredEventListener(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading event listener --");

        EventListenerFactory eventListenerFactory = eventListenerFactories.get(name);
        checkState(eventListenerFactory != null, "Event listener '%s' is not registered", name);

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(eventListenerFactory.getClass().getClassLoader())) {
            EventListener eventListener = eventListenerFactory.create(ImmutableMap.copyOf(properties));
            this.configuredEventListener.set(Optional.of(eventListener));
        }

        log.info("-- Loaded event listener %s --", name);
    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        if (configuredEventListener.get().isPresent()) {
            configuredEventListener.get().get().queryCompleted(queryCompletedEvent);
        }
    }

    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (configuredEventListener.get().isPresent()) {
            configuredEventListener.get().get().queryCreated(queryCreatedEvent);
        }
    }

    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        if (configuredEventListener.get().isPresent()) {
            configuredEventListener.get().get().splitCompleted(splitCompletedEvent);
        }
    }
}
