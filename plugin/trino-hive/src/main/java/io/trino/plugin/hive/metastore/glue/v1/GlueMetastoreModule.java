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
package io.trino.plugin.hive.metastore.glue.v1;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.Table;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
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
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;

import java.lang.annotation.Annotation;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
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
        closingBinder(binder).registerResource(AWSGlueAsync.class, AWSGlueAsync::shutdown);
        newExporter(binder).export(GlueHiveMetastore.class).withGeneratedName();

        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(false);

        newOptionalBinder(binder, GlueColumnStatisticsProviderFactory.class)
                .setDefault().to(DefaultGlueColumnStatisticsProviderFactory.class).in(Scopes.SINGLETON);

        createExecutor(ForGlueHiveMetastore.class, "hive-glue-partitions-%s", GlueHiveMetastoreConfig::getGetPartitionThreads);
        createExecutor(ForGlueColumnStatisticsRead.class, "hive-glue-statistics-read-%s", GlueHiveMetastoreConfig::getReadStatisticsThreads);
        createExecutor(ForGlueColumnStatisticsWrite.class, "hive-glue-statistics-write-%s", GlueHiveMetastoreConfig::getWriteStatisticsThreads);
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

    private void createExecutor(Class<? extends Annotation> annotationClass, String nameTemplate, Function<GlueHiveMetastoreConfig, Integer> threads)
    {
        install(conditionalModule(
                GlueHiveMetastoreConfig.class,
                config -> threads.apply(config) > 1,
                binder -> {
                    binder.bind(Key.get(ExecutorService.class, annotationClass)).toInstance(newCachedThreadPool(daemonThreadsNamed(nameTemplate)));
                    binder.bind(Key.get(Executor.class, annotationClass)).toProvider(new BoundedExecutorProvider(annotationClass, threads)).in(Scopes.SINGLETON);
                    closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, annotationClass));
                },
                binder -> binder.bind(Key.get(Executor.class, annotationClass))
                        .toInstance(directExecutor())));
    }

    private static class BoundedExecutorProvider
            implements Provider<BoundedExecutor>
    {
        private final Class<? extends Annotation> annotationClass;
        private final Function<GlueHiveMetastoreConfig, Integer> threads;
        private Injector injector;

        public BoundedExecutorProvider(Class<? extends Annotation> annotationClass, Function<GlueHiveMetastoreConfig, Integer> threads)
        {
            this.annotationClass = annotationClass;
            this.threads = threads;
        }

        @Inject
        public void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Override
        public BoundedExecutor get()
        {
            ExecutorService executor = injector.getInstance(Key.get(ExecutorService.class, annotationClass));
            int threads = this.threads.apply(injector.getInstance(GlueHiveMetastoreConfig.class));
            checkArgument(threads > 0, "Invalid number of threads: %s", threads);
            return new BoundedExecutor(executor, threads);
        }
    }
}
