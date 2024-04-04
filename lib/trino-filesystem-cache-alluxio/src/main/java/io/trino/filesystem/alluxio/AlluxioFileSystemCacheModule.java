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
package io.trino.filesystem.alluxio;

import alluxio.metrics.MetricsConfig;
import alluxio.metrics.MetricsSystem;
import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.InvalidConfigurationException;
import io.trino.filesystem.cache.AllowFilesystemCacheOnCoordinator;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.filesystem.cache.ConsistentHashingHostAddressProvider;
import io.trino.filesystem.cache.ConsistentHashingHostAddressProviderConfiguration;
import io.trino.filesystem.cache.TrinoFileSystemCache;
import jakarta.annotation.PostConstruct;

import java.util.Properties;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class AlluxioFileSystemCacheModule
        extends AbstractConfigurationAwareModule
{
    private final boolean isCoordinator;
    private final boolean coordinatorOnly;

    public AlluxioFileSystemCacheModule(boolean isCoordinator, boolean coordinatorOnly)
    {
        this.isCoordinator = isCoordinator;
        this.coordinatorOnly = coordinatorOnly;
    }

    @Override
    protected void setup(Binder binder)
    {
        install(new AlluxioJmxStatsModule());
        configBinder(binder).bindConfig(AlluxioFileSystemCacheConfig.class);
        configBinder(binder).bindConfig(ConsistentHashingHostAddressProviderConfiguration.class);
        binder.bind(TrinoFileSystemCache.class).to(AlluxioFileSystemCache.class).in(SINGLETON);

        if (isCoordinator) {
            newOptionalBinder(binder, CachingHostAddressProvider.class).setBinding().to(ConsistentHashingHostAddressProvider.class).in(SINGLETON);
        }

        Properties metricProps = new Properties();
        metricProps.put("sink.jmx.class", "alluxio.metrics.sink.JmxSink");
        metricProps.put("sink.jmx.domain", "org.alluxio");
        MetricsSystem.startSinksFromConfig(new MetricsConfig(metricProps));
    }

    @PostConstruct
    public void validateCachingConfiguration(@AllowFilesystemCacheOnCoordinator boolean allowOnCoordinator)
            throws InvalidConfigurationException
    {
        if (isCoordinator && coordinatorOnly && !allowOnCoordinator) {
            throw new InvalidConfigurationException("Caching was requested on coordinator only but caching is not supported by connector");
        }
    }

    public static class AlluxioJmxStatsModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            binder.bind(AlluxioCacheStats.class).in(SINGLETON);
            newExporter(binder).export(AlluxioCacheStats.class).as(generator -> generator.generatedNameOf(AlluxioCacheStats.class));
        }
    }
}
