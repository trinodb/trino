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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.alluxio.AlluxioFileSystemCacheModule;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import io.trino.filesystem.azure.AzureFileSystemModule;
import io.trino.filesystem.cache.CacheFileSystemFactory;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.filesystem.cache.DefaultCacheKeyProvider;
import io.trino.filesystem.cache.DefaultCachingHostAddressProvider;
import io.trino.filesystem.cache.TrinoFileSystemCache;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsFileSystemModule;
import io.trino.filesystem.memory.MemoryFileSystemCache;
import io.trino.filesystem.memory.MemoryFileSystemCacheModule;
import io.trino.filesystem.s3.FileSystemS3;
import io.trino.filesystem.s3.S3FileSystemModule;
import io.trino.filesystem.switching.SwitchingFileSystemFactory;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.spi.NodeManager;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;

public class FileSystemModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;
    private final NodeManager nodeManager;
    private final OpenTelemetry openTelemetry;
    private final boolean coordinatorFileCaching;

    public FileSystemModule(String catalogName, NodeManager nodeManager, OpenTelemetry openTelemetry, boolean coordinatorFileCaching)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.coordinatorFileCaching = coordinatorFileCaching;
    }

    @Override
    protected void setup(Binder binder)
    {
        FileSystemConfig config = buildConfigObject(FileSystemConfig.class);

        newOptionalBinder(binder, HdfsFileSystemLoader.class);

        if (config.isHadoopEnabled()) {
            HdfsFileSystemLoader loader = new HdfsFileSystemLoader(
                    getProperties(),
                    !config.isNativeAzureEnabled(),
                    !config.isNativeGcsEnabled(),
                    !config.isNativeS3Enabled(),
                    catalogName,
                    nodeManager,
                    openTelemetry);

            loader.configure().forEach(this::consumeProperty);
            binder.bind(HdfsFileSystemLoader.class).toInstance(loader);
        }

        var factories = newMapBinder(binder, String.class, TrinoFileSystemFactory.class);

        if (config.isNativeAzureEnabled()) {
            install(new AzureFileSystemModule());
            factories.addBinding("abfs").to(AzureFileSystemFactory.class);
            factories.addBinding("abfss").to(AzureFileSystemFactory.class);
            factories.addBinding("wasb").to(AzureFileSystemFactory.class);
        }

        if (config.isNativeS3Enabled()) {
            install(new S3FileSystemModule());
            factories.addBinding("s3").to(Key.get(TrinoFileSystemFactory.class, FileSystemS3.class));
            factories.addBinding("s3a").to(Key.get(TrinoFileSystemFactory.class, FileSystemS3.class));
            factories.addBinding("s3n").to(Key.get(TrinoFileSystemFactory.class, FileSystemS3.class));
        }

        if (config.isNativeGcsEnabled()) {
            install(new GcsFileSystemModule());
            factories.addBinding("gs").to(GcsFileSystemFactory.class);
        }

        newOptionalBinder(binder, CachingHostAddressProvider.class).setDefault().to(DefaultCachingHostAddressProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, CacheKeyProvider.class).setDefault().to(DefaultCacheKeyProvider.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, TrinoFileSystemCache.class);
        newOptionalBinder(binder, MemoryFileSystemCache.class);

        boolean isCoordinator = nodeManager.getCurrentNode().isCoordinator();
        if (config.isCacheEnabled()) {
            install(new AlluxioFileSystemCacheModule(isCoordinator));
        }
        if (coordinatorFileCaching) {
            install(new MemoryFileSystemCacheModule(isCoordinator));
        }
    }

    @Provides
    @Singleton
    static TrinoFileSystemFactory createFileSystemFactory(
            Optional<HdfsFileSystemLoader> hdfsFileSystemLoader,
            Map<String, TrinoFileSystemFactory> factories,
            Optional<TrinoFileSystemCache> fileSystemCache,
            Optional<MemoryFileSystemCache> memoryFileSystemCache,
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
        if (fileSystemCache.isPresent()) {
            return new CacheFileSystemFactory(tracer, delegate, fileSystemCache.orElseThrow(), keyProvider.orElseThrow());
        }
        // use MemoryFileSystemCache only when no other TrinoFileSystemCache is configured
        if (memoryFileSystemCache.isPresent()) {
            return new CacheFileSystemFactory(tracer, delegate, memoryFileSystemCache.orElseThrow(), keyProvider.orElseThrow());
        }
        return delegate;
    }
}
