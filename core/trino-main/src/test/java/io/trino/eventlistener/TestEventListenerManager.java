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
package io.trino.eventlistener;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.secrets.SecretsResolver;
import io.trino.client.NodeVersion;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.opentelemetry.api.OpenTelemetry.noop;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TestEventListenerManager
{
    @Test
    public void testShutdownIsForwardedToListeners()
    {
        EventListenerManager eventListenerManager = new EventListenerManager(new EventListenerConfig(), new SecretsResolver(ImmutableMap.of()), noop(), noopTracer(), new NodeVersion("test-version"));
        AtomicBoolean wasCalled = new AtomicBoolean(false);
        EventListener listener = new EventListener()
        {
            @Override
            public void shutdown()
            {
                wasCalled.set(true);
            }
        };

        eventListenerManager.addEventListener(listener);
        eventListenerManager.loadEventListeners(true);
        eventListenerManager.shutdown();

        assertThat(wasCalled.get()).isTrue();
    }

    @Test
    void testListenerRunsOnCoordinator()
            throws Exception
    {
        String name = "testEventListener";
        Path configFile = createConfigFile("event-listener.name=" + name,
                "event-listener.should-run-on-worker=false");

        AtomicBoolean wasCalled = new AtomicBoolean(false);
        EventListenerFactory testEventListenerFactory = getEventListenerFactory(wasCalled, name);

        EventListenerConfig eventListenerConfig = new EventListenerConfig();
        eventListenerConfig.setEventListenerFiles(List.of(configFile.toString()));
        EventListenerManager manager = new EventListenerManager(eventListenerConfig, new SecretsResolver(ImmutableMap.of()), noop(), noopTracer(), new NodeVersion("test-version"));
        manager.addEventListenerFactory(testEventListenerFactory);
        manager.loadEventListeners(true);
        manager.shutdown();

        assertThat(wasCalled.get()).isTrue();
    }

    @Test
    void testListenerRunsOnWorker()
            throws Exception
    {
        String name = "testEventListener";
        Path configFile = createConfigFile("event-listener.name=" + name,
                "event-listener.should-run-on-worker=true");

        AtomicBoolean wasCalled = new AtomicBoolean(false);
        EventListenerFactory testEventListenerFactory = getEventListenerFactory(wasCalled, name);

        EventListenerConfig eventListenerConfig = new EventListenerConfig();
        eventListenerConfig.setEventListenerFiles(List.of(configFile.toString()));
        EventListenerManager manager = new EventListenerManager(eventListenerConfig, new SecretsResolver(ImmutableMap.of()), noop(), noopTracer(), new NodeVersion("test-version"));
        manager.addEventListenerFactory(testEventListenerFactory);
        manager.loadEventListeners(false);
        manager.shutdown();

        assertThat(wasCalled.get()).isTrue();
    }

    @Test
    void testListenerRunsOnWorkerCheckCaseSensitive()
            throws Exception
    {
        String name = "testEventListener";
        Path configFile = createConfigFile("event-listener.name=" + name,
                "event-listener.should-run-on-worker=TrUe");

        AtomicBoolean wasCalled = new AtomicBoolean(false);
        EventListenerFactory testEventListenerFactory = getEventListenerFactory(wasCalled, name);

        EventListenerConfig eventListenerConfig = new EventListenerConfig();
        eventListenerConfig.setEventListenerFiles(List.of(configFile.toString()));
        EventListenerManager manager = new EventListenerManager(eventListenerConfig, new SecretsResolver(ImmutableMap.of()), noop(), noopTracer(), new NodeVersion("test-version"));
        manager.addEventListenerFactory(testEventListenerFactory);
        manager.loadEventListeners(false);
        manager.shutdown();

        assertThat(wasCalled.get()).isTrue();
    }

    @Test
    void testListenerShouldNotRunsOnWorker()
            throws Exception
    {
        String name = "testEventListener";
        Path configFile = createConfigFile("event-listener.name=" + name,
                "event-listener.should-run-on-worker=false");

        AtomicBoolean wasCalled = new AtomicBoolean(false);
        EventListenerFactory testEventListenerFactory = getEventListenerFactory(wasCalled, name);

        EventListenerConfig eventListenerConfig = new EventListenerConfig();
        eventListenerConfig.setEventListenerFiles(List.of(configFile.toString()));
        EventListenerManager manager = new EventListenerManager(eventListenerConfig, new SecretsResolver(ImmutableMap.of()), noop(), noopTracer(), new NodeVersion("test-version"));
        manager.addEventListenerFactory(testEventListenerFactory);
        manager.loadEventListeners(false);
        manager.shutdown();

        assertThat(wasCalled.get()).isFalse();
    }

    @Test
    void testListenerShouldNotRunsOnWorkerWithNotExistsProperty()
            throws Exception
    {
        String name = "testEventListener";
        Path configFile = createConfigFile("event-listener.name=" + name);

        AtomicBoolean wasCalled = new AtomicBoolean(false);
        EventListenerFactory testEventListenerFactory = getEventListenerFactory(wasCalled, name);

        EventListenerConfig eventListenerConfig = new EventListenerConfig();
        eventListenerConfig.setEventListenerFiles(List.of(configFile.toString()));
        EventListenerManager manager = new EventListenerManager(eventListenerConfig, new SecretsResolver(ImmutableMap.of()), noop(), noopTracer(), new NodeVersion("test-version"));
        manager.addEventListenerFactory(testEventListenerFactory);
        manager.loadEventListeners(false);
        manager.shutdown();

        assertThat(wasCalled.get()).isFalse();
    }

    private Path createConfigFile(String... lines)
            throws Exception
    {
        File tmpDir = Files.createTempDirectory("trino-test").toFile();
        tmpDir.mkdirs();
        Path configFile = Path.of(tmpDir.getAbsolutePath(), "event-listener.properties");
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(configFile, UTF_8)) {
            for (String line : lines) {
                bufferedWriter.write(line);
                bufferedWriter.write("\n");
            }
        }
        return configFile;
    }

    private static EventListenerFactory getEventListenerFactory(AtomicBoolean wasCalled, String name)
    {
        EventListener listener = new EventListener()
        {
            @Override
            public void shutdown()
            {
                wasCalled.set(true);
            }
        };

        return new EventListenerFactory() {
            @Override
            public String getName()
            {
                return name;
            }

            @Override
            public EventListener create(Map<String, String> config, EventListenerContext context)
            {
                return listener;
            }
        };
    }
}
