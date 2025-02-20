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
package io.trino.plugin.spanner;

import com.google.cloud.spanner.jdbc.JdbcDriver;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConfiguringConnectionFactory;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DefaultJdbcMetadataFactory;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.DynamicFilteringStats;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.ForJdbcDynamicFiltering;
import io.trino.plugin.jdbc.ForLazyConnectionFactory;
import io.trino.plugin.jdbc.ForRecordCursor;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcDiagnosticModule;
import io.trino.plugin.jdbc.JdbcDynamicFilteringConfig;
import io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties;
import io.trino.plugin.jdbc.JdbcDynamicFilteringSplitManager;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcMetadataSessionProperties;
import io.trino.plugin.jdbc.JdbcPageSinkProvider;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.JdbcRecordSetProvider;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTransactionManager;
import io.trino.plugin.jdbc.JdbcWriteConfig;
import io.trino.plugin.jdbc.JdbcWriteSessionProperties;
import io.trino.plugin.jdbc.LazyConnectionFactory;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.ReusableConnectionFactoryModule;
import io.trino.plugin.jdbc.StatsCollecting;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.plugin.jdbc.TypeHandlingJdbcConfig;
import io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties;
import io.trino.plugin.jdbc.credential.EmptyCredentialProvider;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.logging.RemoteQueryModifierModule;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.mapping.IdentifierMappingModule;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.TypeManager;

import javax.annotation.PreDestroy;
import javax.inject.Provider;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class SpannerModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;

    public SpannerModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(
            SpannerConfig spannerConfig)
            throws ClassNotFoundException
    {
        Class.forName("com.google.cloud.spanner.jdbc.JdbcDriver");
        Properties connectionProperties = new Properties();
        String connectionUrlTemplate = "jdbc:cloudspanner:/%s/projects/%s/instances/%s/databases/%s%s";
        String host = "/";
        String configureEmulator = ";autoConfigEmulator=true";
        if (spannerConfig.isEmulator()) {
            host = host + spannerConfig.getHost();
        }
        String url = String.format(connectionUrlTemplate, host, spannerConfig.getProjectId(), spannerConfig.getInstanceId(), spannerConfig.getDatabase(), configureEmulator);
        JdbcDriver driver = new JdbcDriver();
        System.out.println("USING connection URL " + url);
        //File credentials = new File(spannerConfig.getCredentialsFile());
        if (!driver.acceptsURL(url)) {
            throw new RuntimeException(
                    url + " is incorrect");
        }
        //jdbc:cloudspanner://0.0.0.0:9010/projects/test-project/instances/test-instance/databases/trinodb;autoConfigEmulator=true
        //jdbc:cloudspanner://localhost:9010/projects/test-project/instances/test-instance/databases/test-db;usePlainText=true
        //connectionProperties.put("credentials", spannerConfig.getCredentialsFile());
        connectionProperties.setProperty("retryAbortsInternally", "true");
        return new ConfiguringConnectionFactory(new DriverConnectionFactory(
                driver,
                url,
                connectionProperties,
                new EmptyCredentialProvider()),
                connection -> {
                });
    }

    @Provides
    @Singleton
    public static JdbcStatisticsConfig getConf()
    {
        JdbcStatisticsConfig jdbcStatisticsConfig = new JdbcStatisticsConfig();
        jdbcStatisticsConfig.setEnabled(true);
        return jdbcStatisticsConfig;
    }

    @Provides
    @Singleton
    public static SpannerClient getSpannerClient(
            SpannerConfig spannerConfig,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        return new SpannerClient(
                spannerConfig,
                statisticsConfig,
                connectionFactory,
                queryBuilder,
                typeManager,
                identifierMapping,
                queryModifier);
    }

    @Provides
    public static SpannerTableProperties tableProperties()
    {
        return new SpannerTableProperties();
    }

    @Singleton
    public static SpannerSinkProvider configureSpannerSink(
            ConnectionFactory factory,
            RemoteQueryModifier modifier,
            SpannerClient spannerClient,
            SpannerConfig config,
            SpannerTableProperties propertiesProvider)
    {
        return new SpannerSinkProvider(factory, modifier, spannerClient, config, propertiesProvider);
    }

    public static Multibinder<SessionPropertiesProvider> sessionPropertiesProviderBinder(Binder binder)
    {
        return newSetBinder(binder, SessionPropertiesProvider.class);
    }

    public static void bindSessionPropertiesProvider(Binder binder, Class<? extends SessionPropertiesProvider> type)
    {
        sessionPropertiesProviderBinder(binder).addBinding().to(type).in(Scopes.SINGLETON);
    }

    public static Multibinder<Procedure> procedureBinder(Binder binder)
    {
        return newSetBinder(binder, Procedure.class);
    }

    public static void bindProcedure(Binder binder, Class<? extends Provider<? extends Procedure>> type)
    {
        procedureBinder(binder).addBinding().toProvider(type).in(Scopes.SINGLETON);
    }

    public static Multibinder<TablePropertiesProvider> tablePropertiesProviderBinder(Binder binder)
    {
        return newSetBinder(binder, TablePropertiesProvider.class);
    }

    public static void bindTablePropertiesProvider(Binder binder, Class<? extends TablePropertiesProvider> type)
    {
        tablePropertiesProviderBinder(binder).addBinding().to(type).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public BaseJdbcConfig getBaseJdbcConfig(SpannerConfig config)
    {
        return new BaseJdbcConfig();
    }

    @Override
    public void setup(Binder binder)
    {
        install(new RemoteQueryModifierModule());
        binder.bind(ConnectorSplitManager.class).annotatedWith(ForClassLoaderSafe.class).to(JdbcDynamicFilteringSplitManager.class).in(Scopes.SINGLETON);
        //binder.bind(ConnectorSplitManager.class).to(ClassLoaderSafeConnectorSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).annotatedWith(ForClassLoaderSafe.class).to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        //binder.bind(ConnectorRecordSetProvider.class).to(ClassLoaderSafeConnectorRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).annotatedWith(ForClassLoaderSafe.class).to(JdbcPageSinkProvider.class).in(Scopes.SINGLETON);
        //binder.bind(ConnectorPageSinkProvider.class).to(ClassLoaderSafeConnectorPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(QueryBuilder.class).to(DefaultQueryBuilder.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class));
        //configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setDomainCompactionThreshold(DEFAULT_DOMAIN_COMPACTION_THRESHOLD));

        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        bindSessionPropertiesProvider(binder, TypeHandlingJdbcSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcMetadataSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcWriteSessionProperties.class);
        bindSessionPropertiesProvider(binder, SpannerSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcDynamicFilteringSessionProperties.class);
        newOptionalBinder(binder, Key.get(ConnectorSplitManager.class, ForJdbcDynamicFiltering.class)).setDefault().to(JdbcSplitManager.class).in(Scopes.SINGLETON);

        binder.bind(DynamicFilteringStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(DynamicFilteringStats.class)
                .as(generator -> generator.generatedNameOf(DynamicFilteringStats.class, catalogName));

        newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(DefaultJdbcMetadataFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(ConnectorSplitManager.class, ForJdbcDynamicFiltering.class)).setDefault().to(JdbcSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorSplitManager.class).setBinding().to(JdbcDynamicFilteringSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorRecordSetProvider.class).setBinding().to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorPageSinkProvider.class).setBinding().to(SpannerSinkProvider.class).in(Scopes.SINGLETON);

        binder.bind(JdbcTransactionManager.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(JdbcMetadataConfig.class);
        configBinder(binder).bindConfig(JdbcWriteConfig.class);
        configBinder(binder).bindConfig(JdbcDynamicFilteringConfig.class);

        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(Key.get(SpannerClient.class)).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).to(Key.get(JdbcClient.class, StatsCollecting.class)).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).annotatedWith(ForClassLoaderSafe.class).to(SpannerConnectorMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectionFactory.class)
                .annotatedWith(ForLazyConnectionFactory.class)
                .to(Key.get(ConnectionFactory.class, StatsCollecting.class))
                .in(Scopes.SINGLETON);
        install(conditionalModule(
                SpannerConfig.class,
                (p) -> true,
                new ReusableConnectionFactoryModule(),
                innerBinder -> innerBinder.bind(ConnectionFactory.class).to(LazyConnectionFactory.class).in(Scopes.SINGLETON)));

        bindTablePropertiesProvider(binder, SpannerTableProperties.class);

        binder.bind(SpannerConnector.class).in(Scopes.SINGLETON);
        install(new JdbcDiagnosticModule());
        install(new IdentifierMappingModule());
        install(new DecimalModule());
    }

    @PreDestroy
    public void shutdownRecordCursorExecutor(@ForRecordCursor ExecutorService executor)
    {
        executor.shutdownNow();
    }

    @Singleton
    @ForRecordCursor
    @Provides
    public ExecutorService createRecordCursorExecutor()
    {
        return newDirectExecutorService();
    }

    @Singleton
    @Provides
    public SpannerConnectorMetadata getSpannerMetadata(
            SpannerClient client, IdentifierMapping mapping,
            Set<JdbcQueryEventListener> listeners)
    {
        return new SpannerConnectorMetadata(client, mapping, listeners);
    }
}
