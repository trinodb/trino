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
package io.trino.plugin.hive.metastore.glue;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.glue.model.Table;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;

import java.util.concurrent.Executor;
import java.util.function.Predicate;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class GlueMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(GlueHiveMetastoreConfig.class);
        configBinder(binder).bindConfig(HiveConfig.class);
        binder.bind(AWSCredentialsProvider.class).toProvider(GlueCredentialsProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(RequestHandler2.class, ForGlueHiveMetastore.class));

        newOptionalBinder(binder, Key.get(new TypeLiteral<Predicate<Table>>() {}, ForGlueHiveMetastore.class))
                .setDefault().toProvider(DefaultGlueMetastoreTableFilterProvider.class).in(Scopes.SINGLETON);

        binder.bind(GlueHiveMetastore.class).in(Scopes.SINGLETON);
        binder.bind(HiveMetastoreFactory.class)
                .annotatedWith(RawHiveMetastoreFactory.class)
                .to(GlueHiveMetastoreFactory.class)
                .in(Scopes.SINGLETON);

        // export under the old name, for backwards compatibility
        binder.bind(GlueHiveMetastoreFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(GlueHiveMetastoreFactory.class).as(generator -> generator.generatedNameOf(GlueHiveMetastore.class));

        install(conditionalModule(
                HiveConfig.class,
                HiveConfig::isTableStatisticsEnabled,
                getGlueStatisticsModule(DefaultGlueColumnStatisticsProviderFactory.class),
                getGlueStatisticsModule(DisabledGlueColumnStatisticsProviderFactory.class)));
    }

    private Module getGlueStatisticsModule(Class<? extends GlueColumnStatisticsProviderFactory> statisticsPrividerFactoryClass)
    {
        return internalBinder -> newOptionalBinder(internalBinder, GlueColumnStatisticsProviderFactory.class)
                .setDefault()
                .to(statisticsPrividerFactoryClass)
                .in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForGlueHiveMetastore
    public Executor createExecutor(CatalogName catalogName, GlueHiveMetastoreConfig hiveConfig)
    {
        return createExecutor("hive-glue-partitions-%s", hiveConfig.getGetPartitionThreads());
    }

    @Provides
    @Singleton
    @ForGlueColumnStatisticsRead
    public Executor createStatisticsReadExecutor(CatalogName catalogName, GlueHiveMetastoreConfig hiveConfig)
    {
        return createExecutor("hive-glue-statistics-read-%s", hiveConfig.getReadStatisticsThreads());
    }

    @Provides
    @Singleton
    @ForGlueColumnStatisticsWrite
    public Executor createStatisticsWriteExecutor(CatalogName catalogName, GlueHiveMetastoreConfig hiveConfig)
    {
        return createExecutor("hive-glue-statistics-write-%s", hiveConfig.getWriteStatisticsThreads());
    }

    private Executor createExecutor(String nameTemplate, int threads)
    {
        if (threads == 1) {
            return directExecutor();
        }
        return new BoundedExecutor(
                newCachedThreadPool(daemonThreadsNamed(nameTemplate)),
                threads);
    }
}
