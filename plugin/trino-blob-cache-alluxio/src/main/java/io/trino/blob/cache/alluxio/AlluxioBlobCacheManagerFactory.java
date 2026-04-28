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
package io.trino.blob.cache.alluxio;

import alluxio.metrics.MetricsConfig;
import alluxio.metrics.MetricsSystem;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.opentelemetry.api.trace.Tracer;
import io.trino.plugin.base.jmx.RebindSafeMBeanServer;
import io.trino.spi.cache.BlobCacheManager;
import io.trino.spi.cache.BlobCacheManagerFactory;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.CacheTier;
import org.weakref.jmx.MBeanExporter;

import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class AlluxioBlobCacheManagerFactory
        implements BlobCacheManagerFactory
{
    private static final AtomicBoolean metricsInitialized = new AtomicBoolean();

    @Override
    public String getName()
    {
        return "alluxio";
    }

    @Override
    public CacheTier tier()
    {
        return CacheTier.DISK;
    }

    @Override
    public BlobCacheManager create(Map<String, String> config, CacheManagerContext context)
    {
        MBeanServer mbeanServer = new RebindSafeMBeanServer(ManagementFactory.getPlatformMBeanServer());
        MBeanExporter mbeanExporter = new MBeanExporter(mbeanServer);

        Module module = (Binder binder) -> {
            binder.bind(Tracer.class).toInstance(context.getTracer());
            binder.bind(MBeanExporter.class).toInstance(mbeanExporter);
            configBinder(binder).bindConfig(AlluxioCacheConfig.class);
            binder.bind(AlluxioCache.class).in(Scopes.SINGLETON);
            binder.bind(AlluxioBlobCacheManager.class).in(Scopes.SINGLETON);
        };

        Bootstrap app = new Bootstrap(module)
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config);

        Injector injector = app.initialize();

        if (metricsInitialized.compareAndSet(false, true)) {
            Properties metricProps = new Properties();
            metricProps.put("sink.jmx.class", "alluxio.metrics.sink.JmxSink");
            metricProps.put("sink.jmx.domain", "org.alluxio");
            MetricsSystem.startSinksFromConfig(new MetricsConfig(metricProps));
        }

        return injector.getInstance(AlluxioBlobCacheManager.class);
    }
}
