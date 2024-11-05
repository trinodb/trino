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
package io.trino.server.protocol.spooling;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.server.ServerConfig;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.protocol.SpoolingManager;
import io.trino.spi.protocol.SpoolingManagerContext;
import io.trino.spi.protocol.SpoolingManagerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SpoolingManagerRegistry
{
    private final Map<String, SpoolingManagerFactory> spoolingManagerFactories = new ConcurrentHashMap<>();

    private static final Logger log = Logger.get(SpoolingManagerRegistry.class);

    static final File CONFIG_FILE = new File("etc/spooling-manager.properties");
    private static final String SPOOLING_MANAGER_NAME_PROPERTY = "spooling-manager.name";

    private final boolean enabled;
    private final boolean coordinator;
    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private volatile SpoolingManager spoolingManager;

    @Inject
    public SpoolingManagerRegistry(ServerConfig serverConfig, SpoolingEnabledConfig config, OpenTelemetry openTelemetry, Tracer tracer)
    {
        this.enabled = config.isEnabled();
        this.coordinator = serverConfig.isCoordinator();
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    public void addSpoolingManagerFactory(SpoolingManagerFactory factory)
    {
        requireNonNull(factory, "factory is null");
        if (spoolingManagerFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Spooling manager factory '%s' is already registered", factory.getName()));
        }
    }

    public void loadSpoolingManager()
    {
        if (!enabled) {
            // don't load SpoolingManager when spooling is not enabled
            return;
        }

        if (!CONFIG_FILE.exists()) {
            return;
        }

        Map<String, String> properties = loadProperties();
        String name = properties.remove(SPOOLING_MANAGER_NAME_PROPERTY);
        checkArgument(!isNullOrEmpty(name), "Spooling manager configuration %s does not contain %s", CONFIG_FILE, SPOOLING_MANAGER_NAME_PROPERTY);
        loadSpoolingManager(name, properties);
    }

    public boolean isLoaded()
    {
        return this.spoolingManager != null;
    }

    public synchronized void loadSpoolingManager(String name, Map<String, String> properties)
    {
        SpoolingManagerFactory factory = spoolingManagerFactories.get(name);
        checkArgument(factory != null, "Spooling manager factory '%s' is not registered. Available factories: %s", name, spoolingManagerFactories.keySet());
        loadSpoolingManager(factory, properties);
    }

    public synchronized void loadSpoolingManager(SpoolingManagerFactory factory, Map<String, String> properties)
    {
        requireNonNull(factory, "factory is null");
        log.info("-- Loading spooling manager %s --", factory.getName());
        checkState(spoolingManager == null, "spoolingManager is already loaded");
        SpoolingManagerContext context = new SpoolingManagerContext()
        {
            @Override
            public OpenTelemetry getOpenTelemetry()
            {
                return openTelemetry;
            }

            @Override
            public Tracer getTracer()
            {
                return tracer;
            }

            @Override
            public boolean isCoordinator()
            {
                return coordinator;
            }
        };

        SpoolingManager spoolingManager;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            spoolingManager = factory.create(properties, context);
        }
        this.spoolingManager = spoolingManager;
        log.info("-- Loaded spooling manager %s --", factory.getName());
    }

    public Optional<SpoolingManager> getSpoolingManager()
    {
        return Optional.ofNullable(spoolingManager);
    }

    private static Map<String, String> loadProperties()
    {
        try {
            return new HashMap<>(loadPropertiesFrom(CONFIG_FILE.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read spooling manager configuration file: " + CONFIG_FILE, e);
        }
    }
}
