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
package io.trino.plugin.memory;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.CacheManagerFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class MemoryCacheManagerFactory
        implements CacheManagerFactory
{
    public static final String NAME = "memory-cache";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public CacheManager create(Map<String, String> config, CacheManagerContext context)
    {
        requireNonNull(config, "requiredConfig is null");

        // A plugin is not required to use Guice; it is just very convenient
        Bootstrap app = new Bootstrap(
                new MemoryCacheModule(),
                new MBeanModule(),
                new MBeanServerModule(),
                binder -> binder.bind(CacheManagerContext.class).toInstance(context));

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(ConcurrentCacheManager.class);
    }
}
