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
package io.trino.filesystem.cache.alluxio;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.CacheTier;
import io.trino.spi.cache.FileSystemCacheManager;
import io.trino.spi.cache.FileSystemCacheManagerFactory;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class AlluxioFileSystemCacheManagerFactory
        implements FileSystemCacheManagerFactory
{
    @Override
    public String getName()
    {
        return "alluxio";
    }

    @Override
    public CacheTier cacheTier()
    {
        return CacheTier.DISK;
    }

    @Override
    public FileSystemCacheManager create(Map<String, String> config, CacheManagerContext context)
    {
        Module module = (Binder binder) -> {
            binder.bind(Tracer.class).toInstance(context.getTracer());
            configBinder(binder).bindConfig(AlluxioFileSystemCacheConfig.class);
            binder.bind(AlluxioCacheStats.class).in(Scopes.SINGLETON);
            binder.bind(AlluxioFileSystemCache.class).in(Scopes.SINGLETON);
            binder.bind(AlluxioFileSystemCacheManager.class).in(Scopes.SINGLETON);
        };

        Bootstrap app = new Bootstrap(module)
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config);

        Injector injector = app.initialize();
        return injector.getInstance(AlluxioFileSystemCacheManager.class);
    }
}
