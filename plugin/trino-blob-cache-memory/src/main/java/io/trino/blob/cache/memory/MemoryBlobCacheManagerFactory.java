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
package io.trino.blob.cache.memory;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.cache.BlobCacheManager;
import io.trino.spi.cache.BlobCacheManagerFactory;
import io.trino.spi.cache.CacheManagerContext;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class MemoryBlobCacheManagerFactory
        implements BlobCacheManagerFactory
{
    @Override
    public String getName()
    {
        return "memory";
    }

    @Override
    public BlobCacheManager create(Map<String, String> config, CacheManagerContext context)
    {
        Module module = (Binder binder) -> {
            configBinder(binder).bindConfig(MemoryBlobCacheConfig.class);
            binder.bind(MemoryBlobCache.class).in(Scopes.SINGLETON);
            binder.bind(InMemoryBlobCacheManager.class).in(Scopes.SINGLETON);
            // The cache is a single shared instance, so it exports under one generated name
            newExporter(binder).export(MemoryBlobCache.class).withGeneratedName();
        };

        Bootstrap app = new Bootstrap(new MBeanModule(), new MBeanServerModule(), module)
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config);

        Injector injector = app.initialize();
        return injector.getInstance(InMemoryBlobCacheManager.class);
    }
}
