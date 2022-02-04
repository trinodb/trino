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
package io.trino.plugin.hive.metastore.cache;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreDecorator;
import io.trino.plugin.hive.metastore.procedure.FlushHiveMetastoreCacheProcedure;
import io.trino.spi.NodeManager;
import io.trino.spi.procedure.Procedure;

import javax.inject.Qualifier;
import javax.inject.Singleton;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Optional;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.cachingHiveMetastore;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
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
        newSetBinder(binder, Procedure.class).addBinding().toProvider(FlushHiveMetastoreCacheProcedure.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @DecoratedForCachingHiveMetastore
    public HiveMetastore createDecoratedHiveMetastore(
            @ForCachingHiveMetastore HiveMetastore delegate,
            Optional<HiveMetastoreDecorator> hiveMetastoreDecorator)
    {
        return hiveMetastoreDecorator
                .map(decorator -> decorator.decorate(delegate))
                .orElse(delegate);
    }

    @Provides
    @Singleton
    public Optional<CachingHiveMetastore> createCachingHiveMetastore(
            NodeManager nodeManager,
            @DecoratedForCachingHiveMetastore HiveMetastore delegate,
            CachingHiveMetastoreConfig config,
            CatalogName catalogName)
    {
        if (!nodeManager.getCurrentNode().isCoordinator() || !config.isCacheEnabled()) {
            // Disable caching on workers, because there currently is no way to invalidate such a cache.
            // Note: while we could skip CachingHiveMetastoreModule altogether on workers, we retain it so that catalog
            // configuration can remain identical for all nodes, making cluster configuration easier.
            return Optional.empty();
        }

        return Optional.of(cachingHiveMetastore(
                delegate,
                // Loading of cache entry in CachingHiveMetastore might trigger loading of another cache entry for different object type
                // In case there are no empty executor slots, such operation would deadlock. Therefore, a reentrant executor needs to be
                // used.
                new ReentrantBoundedExecutor(
                        newCachedThreadPool(daemonThreadsNamed("hive-metastore-" + catalogName + "-%s")),
                        config.getMaxMetastoreRefreshThreads()),
                config));
    }

    @Provides
    @Singleton
    public HiveMetastore createHiveMetastore(
            @DecoratedForCachingHiveMetastore HiveMetastore delegate,
            Optional<CachingHiveMetastore> cachingMetastore)
    {
        return cachingMetastore.map(metastore -> (HiveMetastore) metastore).orElse(delegate);
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    public @interface DecoratedForCachingHiveMetastore
    {
    }
}
