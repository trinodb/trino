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
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobCacheManager;
import io.trino.spi.cache.BlobCacheManagerFactory;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.cache.CacheTier;
import io.trino.spi.cache.ConnectorCacheFactory;
import io.trino.spi.cache.PassThroughBlob;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.classloader.ThreadContextClassLoader;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CacheManagerRegistry
{
    private static final Logger log = Logger.get(CacheManagerRegistry.class);

    private static final String CACHE_MANAGER_NAME_PROPERTY = "cache-manager.name";

    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private final SecretsResolver secretsResolver;
    private final List<File> configFiles;

    private final Map<String, BlobCacheManagerFactory> blobCacheFactories = new ConcurrentHashMap<>();
    private final Map<CacheTier, BlobCacheManager> blobCacheManagers = new ConcurrentHashMap<>();

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

        CacheTier tier = factory.tier();
        if (blobCacheManagers.containsKey(tier)) {
            throw new IllegalStateException(format("Blob cache manager for tier %s is already loaded", tier));
        }

        BlobCacheManager manager;
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            manager = factory.create(secretsResolver.getResolvedConfiguration(properties), new CacheManagerContextInstance(openTelemetry, tracer));
        }

        blobCacheManagers.put(tier, manager);
        log.info("-- Loaded blob cache manager %s for tier %s --", name, tier);
    }

    public ConnectorCacheFactory createConnectorCacheFactory(CatalogName catalog)
    {
        requireNonNull(catalog, "catalog is null");
        return tier -> {
            BlobCacheManager manager = blobCacheManagers.get(tier);
            if (manager == null) {
                log.warn("Catalog %s requested blob cache manager tier %s but none registered, using noop", catalog, tier);
                return new NoopBlobCache();
            }
            log.debug("Created new blob cache on tier %s for catalog %s", tier, catalog);
            return manager.createBlobCache(catalog);
        };
    }

    public void drop(CatalogName catalog)
    {
        for (Map.Entry<CacheTier, BlobCacheManager> entry : blobCacheManagers.entrySet()) {
            log.info("Dropping blob cache for tier %s and catalog %s", entry.getKey(), catalog);
            try {
                entry.getValue().drop(catalog);
            }
            catch (Throwable t) {
                log.error(t, "Error dropping blob cache for tier %s and catalog %s", entry.getKey(), catalog);
            }
        }
    }

    @PreDestroy
    public void shutdown()
    {
        for (Map.Entry<CacheTier, BlobCacheManager> entry : blobCacheManagers.entrySet()) {
            try {
                entry.getValue().shutdown();
            }
            catch (Throwable t) {
                log.error(t, "Error shutting down blob cache manager for tier %s", entry.getKey());
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

    private static class NoopBlobCache
            implements BlobCache
    {
        @Override
        public Blob get(CacheKey key, BlobSource source)
        {
            return new PassThroughBlob(source);
        }

        @Override
        public void invalidate(CacheKey key) {}

        @Override
        public void invalidate(Collection<CacheKey> keys) {}
    }
}
