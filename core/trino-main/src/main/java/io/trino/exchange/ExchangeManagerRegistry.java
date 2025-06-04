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
package io.trino.exchange;

import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeManagerFactory;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.spi.StandardErrorCode.EXCHANGE_MANAGER_NOT_CONFIGURED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ExchangeManagerRegistry
{
    private static final Logger log = Logger.get(ExchangeManagerRegistry.class);

    private static final File CONFIG_FILE = new File("etc/exchange-manager.properties");
    private static final String EXCHANGE_MANAGER_NAME_PROPERTY = "exchange-manager.name";

    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private final Map<String, ExchangeManagerFactory> exchangeManagerFactories = new ConcurrentHashMap<>();

    private volatile ExchangeManager exchangeManager;
    private final SecretsResolver secretsResolver;

    @Inject
    public ExchangeManagerRegistry(
            OpenTelemetry openTelemetry,
            Tracer tracer,
            SecretsResolver secretsResolver)
    {
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
    }

    public void addExchangeManagerFactory(ExchangeManagerFactory factory)
    {
        requireNonNull(factory, "factory is null");
        if (exchangeManagerFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Exchange manager factory '%s' is already registered", factory.getName()));
        }
    }

    public void loadExchangeManager()
    {
        if (!CONFIG_FILE.exists()) {
            return;
        }

        Map<String, String> properties = loadProperties(CONFIG_FILE);
        String name = properties.remove(EXCHANGE_MANAGER_NAME_PROPERTY);
        checkArgument(!isNullOrEmpty(name), "Exchange manager configuration %s does not contain %s", CONFIG_FILE, EXCHANGE_MANAGER_NAME_PROPERTY);

        loadExchangeManager(name, properties);
    }

    public synchronized void loadExchangeManager(String name, Map<String, String> properties)
    {
        log.info("-- Loading exchange manager %s --", name);

        checkState(exchangeManager == null, "exchangeManager is already loaded");

        ExchangeManagerFactory factory = exchangeManagerFactories.get(name);
        checkArgument(factory != null, "Exchange manager factory '%s' is not registered. Available factories: %s", name, exchangeManagerFactories.keySet());

        ExchangeManager exchangeManager;
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            exchangeManager = factory.create(secretsResolver.getResolvedConfiguration(properties), new ExchangeManagerContextInstance(openTelemetry, tracer));
        }

        log.info("-- Loaded exchange manager %s --", name);

        this.exchangeManager = exchangeManager;
    }

    public ExchangeManager getExchangeManager()
    {
        ExchangeManager exchangeManager = this.exchangeManager;
        if (exchangeManager == null) {
            throw new TrinoException(EXCHANGE_MANAGER_NOT_CONFIGURED, "Exchange manager must be configured for the failure recovery capabilities to be fully functional");
        }
        return exchangeManager;
    }

    @PreDestroy
    public void shutdown()
    {
        try {
            if (this.exchangeManager != null) {
                exchangeManager.shutdown();
            }
        }
        catch (Throwable t) {
            log.error(t, "Error shutting down exchange manager: %s", exchangeManager);
        }
    }

    private static Map<String, String> loadProperties(File configFile)
    {
        try {
            return new HashMap<>(loadPropertiesFrom(configFile.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read configuration file: " + configFile, e);
        }
    }
}
