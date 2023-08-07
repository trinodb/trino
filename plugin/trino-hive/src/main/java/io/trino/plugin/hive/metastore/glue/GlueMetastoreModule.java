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
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.Table;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.awssdk.v1_11.AwsSdkTelemetry;
import io.trino.plugin.hive.AllowHiveTableRename;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;

import java.util.concurrent.Executor;
import java.util.function.Predicate;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
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
        GlueHiveMetastoreConfig glueConfig = buildConfigObject(GlueHiveMetastoreConfig.class);
        Multibinder<RequestHandler2> requestHandlers = newSetBinder(binder, RequestHandler2.class, ForGlueHiveMetastore.class);
        glueConfig.getCatalogId().ifPresent(catalogId -> requestHandlers.addBinding().toInstance(new GlueCatalogIdRequestHandler(catalogId)));
        glueConfig.getGlueProxyApiId().ifPresent(glueProxyApiId -> requestHandlers.addBinding()
                .toInstance(new ProxyApiRequestHandler(glueProxyApiId)));
        configBinder(binder).bindConfig(HiveConfig.class);
        binder.bind(AWSCredentialsProvider.class).toProvider(GlueCredentialsProvider.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(new TypeLiteral<Predicate<Table>>() {}, ForGlueHiveMetastore.class))
                .setDefault().toProvider(DefaultGlueMetastoreTableFilterProvider.class).in(Scopes.SINGLETON);

        binder.bind(GlueHiveMetastore.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class))
                .setDefault()
                .to(GlueHiveMetastoreFactory.class)
                .in(Scopes.SINGLETON);

        // export under the old name, for backwards compatibility
        binder.bind(GlueHiveMetastoreFactory.class).in(Scopes.SINGLETON);
        binder.bind(Key.get(GlueMetastoreStats.class, ForGlueHiveMetastore.class)).toInstance(new GlueMetastoreStats());
        binder.bind(AWSGlueAsync.class).toProvider(HiveGlueClientProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(GlueHiveMetastoreFactory.class).as(generator -> generator.generatedNameOf(GlueHiveMetastore.class));

        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(false);

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

    @ProvidesIntoSet
    @Singleton
    @ForGlueHiveMetastore
    public RequestHandler2 createRequestHandler(OpenTelemetry openTelemetry)
    {
        return AwsSdkTelemetry.builder(openTelemetry)
                .setCaptureExperimentalSpanAttributes(true)
                .build()
                .newRequestHandler();
    }

    @Provides
    @Singleton
    @ForGlueHiveMetastore
    public Executor createExecutor(GlueHiveMetastoreConfig hiveConfig)
    {
        return createExecutor("hive-glue-partitions-%s", hiveConfig.getGetPartitionThreads());
    }

    @Provides
    @Singleton
    @ForGlueColumnStatisticsRead
    public Executor createStatisticsReadExecutor(GlueHiveMetastoreConfig hiveConfig)
    {
        return createExecutor("hive-glue-statistics-read-%s", hiveConfig.getReadStatisticsThreads());
    }

    @Provides
    @Singleton
    @ForGlueColumnStatisticsWrite
    public Executor createStatisticsWriteExecutor(GlueHiveMetastoreConfig hiveConfig)
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
