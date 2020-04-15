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
package io.prestosql.plugin.hive.metastore.cache;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.prestosql.plugin.base.CatalogName;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.HiveMetastoreDecorator;

import javax.inject.Singleton;

import java.util.Optional;
import java.util.concurrent.Executor;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.hive.metastore.cache.CachingHiveMetastore.cachingHiveMetastore;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CachingHiveMetastoreModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(CachingHiveMetastoreConfig.class);
        newOptionalBinder(binder, HiveMetastoreDecorator.class);
        newExporter(binder).export(HiveMetastore.class)
                .as(generator -> generator.generatedNameOf(CachingHiveMetastore.class));
    }

    @Provides
    @Singleton
    public HiveMetastore createCachingHiveMetastore(
            @ForCachingHiveMetastore HiveMetastore delegate,
            @ForCachingHiveMetastore Executor executor,
            CachingHiveMetastoreConfig config,
            Optional<HiveMetastoreDecorator> hiveMetastoreDecorator)
    {
        HiveMetastore decoratedDelegate = hiveMetastoreDecorator.map(decorator -> decorator.decorate(delegate))
                .orElse(delegate);
        return cachingHiveMetastore(decoratedDelegate, executor, config);
    }

    @Provides
    @Singleton
    @ForCachingHiveMetastore
    public Executor createCachingHiveMetastoreExecutor(CatalogName catalogName, CachingHiveMetastoreConfig hiveConfig)
    {
        return new ReentrantBoundedExecutor(
                newCachedThreadPool(daemonThreadsNamed("hive-metastore-" + catalogName + "-%s")),
                hiveConfig.getMaxMetastoreRefreshThreads());
    }
}
