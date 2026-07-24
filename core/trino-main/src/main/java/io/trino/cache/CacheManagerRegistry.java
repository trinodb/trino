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
package io.trino.cache;

import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.cache.BlobCacheManager;
import io.trino.spi.cache.BlobCacheManagerFactory;
import io.trino.spi.cache.CacheRequirements;
import io.trino.spi.cache.ConnectorCacheFactory;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.classloader.ThreadContextClassLoader;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CacheManagerRegistry
{
    private static final Logger log = Logger.get(CacheManagerRegistry.class);

    private static final String CACHE_MANAGER_NAME_PROPERTY = "cache-manager.name";
    private static final String DEFAULT_CACHE_MANAGER_NAME = "memory";

    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private final SecretsResolver secretsResolver;
    private final List<File> configFiles;

    private final Map<String, BlobCacheManagerFactory> blobCacheFactories = new ConcurrentHashMap<>();
    private final Map<String, LoadedCacheManager> blobCacheManagers = new ConcurrentHashMap<>();
    private final Map<UsageKey, CacheRequirements> issuedUsages = new ConcurrentHashMap<>();

    private record LoadedCacheManager(String name, BlobCacheManager manager) {}

    private record UsageKey(CatalogName catalog, String usageName) {}

    @Inject
    public CacheManagerRegistry(
            OpenTelemetry openTelemetry,
            Tracer tracer,
            SecretsResolver secretsResolver,
            CacheManagerConfig config)
    {
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
        this.configFiles = List.copyOf(config.getCacheManagerConfigFiles());
    }

    public void addBlobCacheManagerFactory(BlobCacheManagerFactory factory)
    {
        requireNonNull(factory, "factory is null");
        if (blobCacheFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Blob cache manager factory '%s' is already registered", factory.getName()));
        }
    }

    public synchronized void loadCacheManagers()
    {
        if (configFiles.isEmpty()) {
            // Coordinator metadata caching is an engine default: load the memory manager with
            // default configuration when the operator configured nothing explicitly. Operators
            // using cache-manager.config-files take full control of what is loaded.
            if (blobCacheFactories.containsKey(DEFAULT_CACHE_MANAGER_NAME)) {
                loadBlobCacheManager(DEFAULT_CACHE_MANAGER_NAME, Map.of());
            }
            return;
        }
        for (File configFile : configFiles) {
            Map<String, String> properties = loadProperties(configFile);
            String name = properties.remove(CACHE_MANAGER_NAME_PROPERTY);
            checkArgument(!isNullOrEmpty(name), "Cache manager configuration %s does not contain %s", configFile, CACHE_MANAGER_NAME_PROPERTY);
            loadBlobCacheManager(name, properties);
        }
    }

    public synchronized void loadBlobCacheManager(String name, Map<String, String> properties)
    {
        log.info("-- Loading blob cache manager %s --", name);

        BlobCacheManagerFactory factory = blobCacheFactories.get(name);
        checkArgument(factory != null, "Blob cache manager factory '%s' is not registered. Available factories: %s", name, blobCacheFactories.keySet());

        if (blobCacheManagers.containsKey(name)) {
            throw new IllegalStateException(format("Blob cache manager %s is already loaded", name));
        }

        BlobCacheManager manager;
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            manager = factory.create(secretsResolver.getResolvedConfiguration(properties), new CacheManagerContextInstance(openTelemetry, tracer));
        }

        blobCacheManagers.put(name, new LoadedCacheManager(name, manager));
        log.info("-- Loaded blob cache manager %s --", name);
    }

    public ConnectorCacheFactory createConnectorCacheFactory(CatalogName catalog)
    {
        requireNonNull(catalog, "catalog is null");
        return requirements -> {
            requireNonNull(requirements, "requirements is null");

            List<LoadedCacheManager> qualifying = blobCacheManagers.values().stream()
                    .filter(loaded -> requirements.capabilities().stream().allMatch(loaded.manager()::hasCapability))
                    .collect(toImmutableList());
            if (qualifying.isEmpty()) {
                log.debug("No loaded blob cache manager provides %s required by usage %s of catalog %s", requirements.capabilities(), requirements.usageName(), catalog);
                return Optional.empty();
            }
            if (qualifying.size() > 1) {
                List<String> names = qualifying.stream()
                        .map(LoadedCacheManager::name)
                        .collect(toImmutableList());
                throw new IllegalStateException(format("Blob cache managers %s all provide %s required by usage %s: unload all but one of them", names, requirements.capabilities(), requirements.usageName()));
            }

            // The usage name scopes the catalog's cache key namespace, so two consumers must
            // not alias each other's entries: identical repeated requests are idempotent, but
            // reusing a usage name with different requirements is rejected
            CacheRequirements issued = issuedUsages.putIfAbsent(new UsageKey(catalog, requirements.usageName()), requirements);
            if (issued != null && !issued.equals(requirements)) {
                throw new IllegalStateException(format("Usage %s of catalog %s was already issued a cache with %s, conflicting with requested %s", requirements.usageName(), catalog, issued.capabilities(), requirements.capabilities()));
            }

            LoadedCacheManager selected = qualifying.getFirst();
            log.info("Usage %s of catalog %s requires %s, selected blob cache manager %s", requirements.usageName(), catalog, requirements.capabilities(), selected.name());
            return Optional.of(new ScopedBlobCache(selected.manager().create(catalog, requirements.capabilities()), catalog, requirements.usageName()));
        };
    }

    public void drop(CatalogName catalog)
    {
        issuedUsages.keySet().removeIf(usage -> usage.catalog().equals(catalog));
        for (LoadedCacheManager loaded : blobCacheManagers.values()) {
            log.info("Dropping blob cache of manager %s for catalog %s", loaded.name(), catalog);
            try {
                loaded.manager().drop(catalog);
            }
            catch (Throwable t) {
                log.error(t, "Error dropping blob cache of manager %s for catalog %s", loaded.name(), catalog);
            }
        }
    }

    @PreDestroy
    public void shutdown()
    {
        for (LoadedCacheManager loaded : blobCacheManagers.values()) {
            try {
                loaded.manager().shutdown();
            }
            catch (Throwable t) {
                log.error(t, "Error shutting down blob cache manager %s", loaded.name());
            }
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
