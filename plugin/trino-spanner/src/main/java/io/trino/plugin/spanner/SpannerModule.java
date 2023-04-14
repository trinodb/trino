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
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CachingJdbcClient;
import io.trino.plugin.jdbc.ConfiguringConnectionFactory;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DefaultJdbcMetadataFactory;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.DynamicFilteringStats;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.ForJdbcDynamicFiltering;
import io.trino.plugin.jdbc.ForLazyConnectionFactory;
import io.trino.plugin.jdbc.ForRecordCursor;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcConnector;
import io.trino.plugin.jdbc.JdbcDynamicFilteringConfig;
import io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties;
import io.trino.plugin.jdbc.JdbcDynamicFilteringSplitManager;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcMetadataSessionProperties;
import io.trino.plugin.jdbc.JdbcRecordSetProvider;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTransactionManager;
import io.trino.plugin.jdbc.JdbcWriteConfig;
import io.trino.plugin.jdbc.JdbcWriteSessionProperties;
import io.trino.plugin.jdbc.LazyConnectionFactory;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.QueryConfig;
import io.trino.plugin.jdbc.ReusableConnectionFactoryModule;
import io.trino.plugin.jdbc.StatsCollecting;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.plugin.jdbc.TypeHandlingJdbcConfig;
import io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.logging.RemoteQueryModifierModule;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.mapping.IdentifierMappingModule;
import io.trino.plugin.jdbc.procedure.FlushJdbcMetadataCacheProcedure;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.type.TypeManager;

import javax.annotation.PreDestroy;
import javax.inject.Provider;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;

public class SpannerModule
        extends AbstractConfigurationAwareModule
{

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            SpannerConfig spannerConfig)
            throws ClassNotFoundException
    {
        Class.forName("com.google.cloud.spanner.jdbc.JdbcDriver");
        Properties connectionProperties = new Properties();
        String connectionUrl = config.getConnectionUrl();
        JdbcDriver driver = new JdbcDriver();
        //File credentials = new File(spannerConfig.getCredentialsFile());
        if (!driver.acceptsURL(connectionUrl)) {
            throw new RuntimeException(config.getConnectionUrl() + " is incorrect");
        }
        //connectionProperties.put("credentials", spannerConfig.getCredentialsFile());
        connectionProperties.setProperty("retryAbortsInternally", "true");
        return new ConfiguringConnectionFactory(new DriverConnectionFactory(
                driver,
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider),
                connection -> {
                });
    }

    @Provides
    @Singleton
    public static SpannerClient getSpannerClient(
            BaseJdbcConfig config,
            SpannerConfig spannerConfig,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        return new SpannerClient(config,
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
    public static SpannerSinkProvider configureSnowflakeSink(
            ConnectionFactory factory,
            JdbcClient client,
            RemoteQueryModifier modifier,
            SpannerClient snowflakeConfig,
            SpannerTableProperties propertiesProvider)
    {
        return new SpannerSinkProvider(factory, client, modifier, snowflakeConfig, propertiesProvider);
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

    @Override
    public void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(SpannerClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(SpannerConfig.class);
        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        install(new IdentifierMappingModule());
        install(new RemoteQueryModifierModule());
        install(new JdbcJoinPushdownSupportModule());

        newOptionalBinder(binder, ConnectorAccessControl.class);
        newOptionalBinder(binder, QueryBuilder.class).setDefault().to(DefaultQueryBuilder.class).in(Scopes.SINGLETON);

        procedureBinder(binder);
        tablePropertiesProviderBinder(binder);

        newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(DefaultJdbcMetadataFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(ConnectorSplitManager.class, ForJdbcDynamicFiltering.class)).setDefault().to(JdbcSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorSplitManager.class).setBinding().to(JdbcDynamicFilteringSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorRecordSetProvider.class).setBinding().to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorPageSinkProvider.class).setBinding().to(SpannerSinkProvider.class).in(Scopes.SINGLETON);

        binder.bind(JdbcTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(JdbcConnector.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(JdbcMetadataConfig.class);
        configBinder(binder).bindConfig(JdbcWriteConfig.class);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(SpannerConfig.class);
        configBinder(binder).bindConfig(JdbcDynamicFilteringConfig.class);
        bindTablePropertiesProvider(binder, SpannerTableProperties.class);

        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        bindSessionPropertiesProvider(binder, TypeHandlingJdbcSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcMetadataSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcWriteSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcDynamicFilteringSessionProperties.class);
        bindSessionPropertiesProvider(binder, SpannerSessionProperties.class);
        binder.bind(DynamicFilteringStats.class).in(Scopes.SINGLETON);

        binder.bind(CachingJdbcClient.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).to(Key.get(CachingJdbcClient.class)).in(Scopes.SINGLETON);

        newSetBinder(binder, Procedure.class).addBinding().toProvider(FlushJdbcMetadataCacheProcedure.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class);

        binder.bind(ConnectionFactory.class)
                .annotatedWith(ForLazyConnectionFactory.class)
                .to(Key.get(ConnectionFactory.class, StatsCollecting.class))
                .in(Scopes.SINGLETON);
        install(conditionalModule(
                QueryConfig.class,
                QueryConfig::isReuseConnection,
                new ReusableConnectionFactoryModule(),
                innerBinder -> innerBinder.bind(ConnectionFactory.class).to(LazyConnectionFactory.class).in(Scopes.SINGLETON)));

        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class));

        newOptionalBinder(binder, Key.get(ExecutorService.class, ForRecordCursor.class))
                .setBinding()
                .toProvider(MoreExecutors::newDirectExecutorService)
                .in(Scopes.SINGLETON);
    }

    @PreDestroy
    public void shutdownRecordCursorExecutor(@ForRecordCursor ExecutorService executor)
    {
        executor.shutdownNow();
    }
}
