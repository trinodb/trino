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
package io.trino.filesystem.manager;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import io.trino.filesystem.azure.AzureFileSystemModule;
import io.trino.filesystem.cache.CacheFileSystemFactory;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.filesystem.cache.CacheSplitAffinityProvider;
import io.trino.filesystem.cache.DefaultCacheKeyProvider;
import io.trino.filesystem.cache.NoopSplitAffinityProvider;
import io.trino.filesystem.cache.SplitAffinityProvider;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsFileSystemModule;
import io.trino.filesystem.local.LocalFileSystemConfig;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.filesystem.s3.FileSystemS3;
import io.trino.filesystem.s3.S3FileSystemModule;
import io.trino.filesystem.switching.SwitchingFileSystemFactory;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.filesystem.tracking.TrackingFileSystemFactory;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.CacheRequirements;
import io.trino.spi.connector.ConnectorContext;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.spi.cache.CacheCapability.CAN_EXCEED_HEAP_SIZE;
import static io.trino.spi.cache.CacheCapability.LOW_LATENCY;
import static java.util.Objects.requireNonNull;

public class FileSystemModule
        extends AbstractConfigurationAwareModule
{
    // Table data working sets read by worker scans exceed memory by design
    private static final CacheRequirements DATA_CACHE_REQUIREMENTS = new CacheRequirements("filesystem.data", Set.of(CAN_EXCEED_HEAP_SIZE));

    // Small, hot metadata files on the coordinator planning path: hits must not do I/O
    private static final CacheRequirements METADATA_CACHE_REQUIREMENTS = new CacheRequirements("filesystem.metadata", Set.of(LOW_LATENCY));

    private final String catalogName;
    private final ConnectorContext context;
    private final boolean isCoordinator;
    private final boolean coordinatorFileCaching;

    public FileSystemModule(String catalogName, ConnectorContext context, boolean coordinatorFileCaching)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.context = requireNonNull(context, "context is null");
        this.isCoordinator = context.getCurrentNode().isCoordinator();
        this.coordinatorFileCaching = coordinatorFileCaching;
    }

    @Override
    protected void setup(Binder binder)
    {
        FileSystemConfig config = buildConfigObject(FileSystemConfig.class);

        newOptionalBinder(binder, HdfsFileSystemLoader.class);

        if (config.isHadoopEnabled()) {
            HdfsFileSystemLoader loader = new HdfsFileSystemLoader(getProperties(), catalogName, context);

            loader.configure().forEach((name, securitySensitive) ->
                    consumeProperty(new ConfigPropertyMetadata(name, securitySensitive)));

            binder.bind(HdfsFileSystemLoader.class).toInstance(loader);
        }

        var factories = newMapBinder(binder, String.class, TrinoFileSystemFactory.class);

        if (config.isAzureEnabled()) {
            install(new AzureFileSystemModule());
            factories.addBinding("abfs").to(AzureFileSystemFactory.class);
            factories.addBinding("abfss").to(AzureFileSystemFactory.class);
            factories.addBinding("wasb").to(AzureFileSystemFactory.class);
            factories.addBinding("wasbs").to(AzureFileSystemFactory.class);
        }

        if (config.isS3Enabled()) {
            install(new S3FileSystemModule());
            factories.addBinding("s3").to(Key.get(TrinoFileSystemFactory.class, FileSystemS3.class));
            factories.addBinding("s3a").to(Key.get(TrinoFileSystemFactory.class, FileSystemS3.class));
            factories.addBinding("s3n").to(Key.get(TrinoFileSystemFactory.class, FileSystemS3.class));
        }

        if (config.isGcsEnabled()) {
            install(new GcsFileSystemModule());
            factories.addBinding("gs").to(GcsFileSystemFactory.class);
        }

        if (config.isLocalEnabled()) {
            configBinder(binder).bindConfig(LocalFileSystemConfig.class);
            factories.addBinding("local").to(LocalFileSystemFactory.class);
            factories.addBinding("file").to(LocalFileSystemFactory.class);
        }

        newOptionalBinder(binder, CacheKeyProvider.class).setDefault().to(DefaultCacheKeyProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, SplitAffinityProvider.class).setDefault().to(NoopSplitAffinityProvider.class).in(Scopes.SINGLETON);

        if (config.isCacheEnabled() && isCoordinator) {
            newOptionalBinder(binder, SplitAffinityProvider.class).setBinding().to(CacheSplitAffinityProvider.class).in(Scopes.SINGLETON);
        }
    }

    @Provides
    @Singleton
    Optional<BlobCache> createBlobCache(FileSystemConfig config)
    {
        if (config.isCacheEnabled()) {
            // The operator explicitly enabled caching for this catalog, so every node must
            // have a manager providing it
            return Optional.of(context.getCacheFactory().createBlobCache(DATA_CACHE_REQUIREMENTS)
                    .orElseThrow(() -> new IllegalStateException(
                            "fs.cache.enabled is set for catalog %s but no loaded blob cache manager provides %s: configure one via cache-manager.config-files".formatted(
                                    catalogName, DATA_CACHE_REQUIREMENTS.capabilities()))));
        }
        if (coordinatorFileCaching && isCoordinator) {
            // Metadata caching is an engine default, not an operator opt-in: degrade quietly
            // when no manager provides it
            return context.getCacheFactory().createBlobCache(METADATA_CACHE_REQUIREMENTS);
        }
        return Optional.empty();
    }

    @Provides
    @Singleton
    static TrinoFileSystemFactory createFileSystemFactory(
            FileSystemConfig config,
            Optional<HdfsFileSystemLoader> hdfsFileSystemLoader,
            Map<String, TrinoFileSystemFactory> factories,
            Optional<BlobCache> blobCache,
            Optional<CacheKeyProvider> keyProvider,
            Tracer tracer)
    {
        Optional<TrinoFileSystemFactory> hdfsFactory = hdfsFileSystemLoader.map(HdfsFileSystemLoader::create);

        Function<Location, TrinoFileSystemFactory> loader = location -> location.scheme()
                .map(factories::get)
                .or(() -> hdfsFactory)
                .orElseThrow(() -> new IllegalArgumentException("No factory for location: " + location));

        TrinoFileSystemFactory delegate = new SwitchingFileSystemFactory(loader);
        delegate = new TracingFileSystemFactory(tracer, delegate);

        if (config.isTrackingEnabled()) {
            delegate = new TrackingFileSystemFactory(delegate);
        }

        if (blobCache.isPresent()) {
            return new CacheFileSystemFactory(tracer, delegate, blobCache.orElseThrow(), keyProvider.orElseThrow());
        }
        return delegate;
    }
}
