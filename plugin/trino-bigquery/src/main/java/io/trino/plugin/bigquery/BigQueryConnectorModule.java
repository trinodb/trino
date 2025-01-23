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
package io.trino.plugin.bigquery;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.OptionalBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.logging.FormatInterpolator;
import io.trino.plugin.base.logging.SessionInterpolatedValues;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.bigquery.procedure.ExecuteProcedure;
import io.trino.plugin.bigquery.ptf.Query;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;

import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static io.trino.plugin.bigquery.BigQueryConfig.ARROW_SERIALIZATION_ENABLED;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toSet;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class BigQueryConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        install(new ClientModule());
        install(new StaticCredentialsModule());
    }

    public static class ClientModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            // BigQuery related
            binder.bind(BigQueryReadClientFactory.class).in(Scopes.SINGLETON);
            binder.bind(BigQueryWriteClientFactory.class).in(Scopes.SINGLETON);
            binder.bind(BigQueryClientFactory.class).in(Scopes.SINGLETON);
            binder.bind(BigQueryTypeManager.class).in(Scopes.SINGLETON);

            // Connector implementation
            binder.bind(BigQueryConnector.class).in(Scopes.SINGLETON);
            binder.bind(BigQueryMetadataFactory.class).to(DefaultBigQueryMetadataFactory.class).in(Scopes.SINGLETON);
            binder.bind(BigQueryTransactionManager.class).in(Scopes.SINGLETON);
            binder.bind(BigQuerySplitManager.class).in(Scopes.SINGLETON);
            binder.bind(BigQueryPageSourceProvider.class).in(Scopes.SINGLETON);
            binder.bind(BigQueryPageSinkProvider.class).in(Scopes.SINGLETON);
            binder.bind(ViewMaterializationCache.class).in(Scopes.SINGLETON);
            configBinder(binder).bindConfig(BigQueryConfig.class);
            configBinder(binder).bindConfig(BigQueryRpcConfig.class);
            newOptionalBinder(binder, BigQueryArrowBufferAllocator.class);
            install(conditionalModule(
                    BigQueryConfig.class,
                    BigQueryConfig::isArrowSerializationEnabled,
                    new ArrowSerializationModule()));
            newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
            newSetBinder(binder, Procedure.class).addBinding().toProvider(ExecuteProcedure.class).in(Scopes.SINGLETON);
            newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(BigQuerySessionProperties.class).in(Scopes.SINGLETON);

            Multibinder<BigQueryOptionsConfigurer> optionsConfigurers = newSetBinder(binder, BigQueryOptionsConfigurer.class);
            optionsConfigurers.addBinding().to(CredentialsOptionsConfigurer.class).in(Scopes.SINGLETON);
            optionsConfigurers.addBinding().to(HeaderOptionsConfigurer.class).in(Scopes.SINGLETON);
            optionsConfigurers.addBinding().to(RetryOptionsConfigurer.class).in(Scopes.SINGLETON);
            optionsConfigurers.addBinding().to(GrpcChannelOptionsConfigurer.class).in(Scopes.SINGLETON);
            optionsConfigurers.addBinding().to(TracingOptionsConfigurer.class).in(Scopes.SINGLETON);
            newOptionalBinder(binder, ProxyTransportFactory.class);

            install(conditionalModule(
                    BigQueryConfig.class,
                    BigQueryConfig::isProxyEnabled,
                    proxyBinder -> {
                        configBinder(proxyBinder).bindConfig(BigQueryProxyConfig.class);
                        newSetBinder(proxyBinder, BigQueryOptionsConfigurer.class).addBinding().to(ProxyOptionsConfigurer.class).in(Scopes.SINGLETON);
                        newOptionalBinder(binder, ProxyTransportFactory.class).setDefault().to(ProxyTransportFactory.DefaultProxyTransportFactory.class).in(Scopes.SINGLETON);
                    }));

            closingBinder(binder).registerExecutor(ListeningExecutorService.class);
            closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForBigQueryPageSource.class));
        }

        @Provides
        @Singleton
        public static HeaderProvider createHeaderProvider(NodeManager nodeManager)
        {
            return FixedHeaderProvider.create("user-agent", "Trino/" + nodeManager.getCurrentNode().getVersion());
        }

        @Provides
        @Singleton
        public static BigQueryLabelFactory labelFactory(BigQueryConfig config)
        {
            return new BigQueryLabelFactory(config.getQueryLabelName(), new FormatInterpolator<>(config.getQueryLabelFormat(), SessionInterpolatedValues.values()));
        }

        @Provides
        @Singleton
        public ListeningExecutorService provideListeningExecutor(BigQueryConfig config)
        {
            return listeningDecorator(newFixedThreadPool(config.getMetadataParallelism(), daemonThreadsNamed("big-query-%s"))); // limit parallelism
        }

        @Provides
        @Singleton
        @ForBigQueryPageSource
        public ExecutorService provideExecutor(CatalogName catalogName)
        {
            return newCachedThreadPool(daemonThreadsNamed("bigquery-" + catalogName + "-%s"));
        }
    }

    public static class StaticCredentialsModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(StaticCredentialsConfig.class);
            // SingletonIdentityCacheMapping is safe to use with StaticBigQueryCredentialsSupplier
            // as credentials do not depend on actual connector session.
            newOptionalBinder(binder, IdentityCacheMapping.class)
                    .setDefault()
                    .to(IdentityCacheMapping.SingletonIdentityCacheMapping.class)
                    .in(Scopes.SINGLETON);

            OptionalBinder<BigQueryCredentialsSupplier> credentialsSupplierBinder = newOptionalBinder(binder, BigQueryCredentialsSupplier.class);
            credentialsSupplierBinder
                    .setDefault()
                    .to(DefaultBigQueryCredentialsProvider.class)
                    .in(Scopes.SINGLETON);

            StaticCredentialsConfig staticCredentialsConfig = buildConfigObject(StaticCredentialsConfig.class);
            if (staticCredentialsConfig.getCredentialsFile().isPresent() || staticCredentialsConfig.getCredentialsKey().isPresent()) {
                credentialsSupplierBinder
                        .setBinding()
                        .to(StaticBigQueryCredentialsSupplier.class)
                        .in(Scopes.SINGLETON);
            }
        }
    }

    public static class ArrowSerializationModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            verifyPackageAccessAllowed(binder);

            configBinder(binder).bindConfig(BigQueryArrowConfig.class);
            binder.bind(BigQueryArrowBufferAllocator.class).in(Scopes.SINGLETON);
            binder.bind(BigQueryArrowAllocatorStats.class).in(Scopes.SINGLETON);

            newExporter(binder).export(BigQueryArrowBufferAllocator.class).withGeneratedName();
        }

        /**
         * Apache Arrow requires reflective access to certain Java internals prohibited since Java 17.
         * Adds an error to the {@code binder} if required --add-opens is not passed to the JVM.
         */
        private static void verifyPackageAccessAllowed(Binder binder)
        {
            // Match an --add-opens argument that opens a package to unnamed modules.
            // The first group is the opened package.
            Pattern argPattern = Pattern.compile(
                    "^--add-opens=(.*)=([A-Za-z0-9_.]+,)*ALL-UNNAMED(,[A-Za-z0-9_.]+)*$");
            // We don't need to check for values in separate arguments because
            // they are joined with "=" before we get them.

            Set<String> openedModules = ManagementFactory.getRuntimeMXBean()
                    .getInputArguments()
                    .stream()
                    .map(argPattern::matcher)
                    .filter(Matcher::matches)
                    .map(matcher -> matcher.group(1))
                    .collect(toSet());

            if (!openedModules.contains("java.base/java.nio")) {
                binder.addError(
                        "BigQuery connector requires additional JVM arguments to run when '" + ARROW_SERIALIZATION_ENABLED + "' is enabled. " +
                                "Please add '--add-opens=java.base/java.nio=ALL-UNNAMED' to the JVM configuration.");
            }
        }
    }
}
