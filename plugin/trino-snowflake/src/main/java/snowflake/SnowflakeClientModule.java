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
package snowflake;

import com.google.common.base.Preconditions;
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
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcMetadataSessionProperties;
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
import net.snowflake.client.jdbc.SnowflakeDriver;

import javax.annotation.PreDestroy;
import javax.inject.Provider;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class SnowflakeClientModule
        extends AbstractConfigurationAwareModule
{

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            SnowflakeConfig snowflakeConfig)
            throws SQLException, MalformedURLException
    {

        Properties connectionProperties = new Properties();
        SnowflakeDriver driver = new SnowflakeDriver();
        String connectionUrl = config.getConnectionUrl();
        if (!driver.acceptsURL(connectionUrl)) {
            throw new RuntimeException(config.getConnectionUrl() + " is incorrect");
        }
        String catalog = snowflakeConfig.getCatalog();
        String warehouse = snowflakeConfig.getWarehouse();
        String stage = snowflakeConfig.getStageName();
        Preconditions.checkArgument(catalog != null && !catalog.equals(""),
                new RuntimeException("Snowflake catalog is set to null. Please set " + SnowflakeConfig.SNOWFLAKE_CATALOG));
        Preconditions.checkArgument(warehouse != null && !warehouse.equals(""),
                new RuntimeException("Snowflake warehouse is set to null. Please set " + SnowflakeConfig.SNOWFLAKE_WAREHOUSE));
        Preconditions.checkArgument(stage != null && !stage.equals(""),
                new RuntimeException("Snowflake stage is set to null. Please set " + SnowflakeConfig.SNOWFLAKE_STAGE_NAME));

        connectionProperties.setProperty("db", catalog);
        connectionProperties.setProperty("warehouse", warehouse);
        return new ConfiguringConnectionFactory(new DriverConnectionFactory(
                driver,
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider),
                connecton -> {
                    Statement statement = connecton.createStatement();
                    statement.executeQuery("ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'");
                    statement.execute("USE DATABASE " + catalog);
                    statement.execute("USE WAREHOUSE " + warehouse);
                });
    }

    @Provides
    @Singleton
    public static SnowflakeSessionPropertiesProvider propertiesProvider(SnowflakeConfig config)
    {
        return new SnowflakeSessionPropertiesProvider(config);
    }

    @Provides
    @Singleton
    public static SnowflakeClient getSnowflakeClient(
            BaseJdbcConfig config,
            SnowflakeConfig snowflakeConfig,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier,
            SnowflakeSessionPropertiesProvider propertiesProvider)
    {
        return new SnowflakeClient(config,
                snowflakeConfig,
                statisticsConfig,
                connectionFactory,
                queryBuilder,
                typeManager,
                identifierMapping,
                queryModifier,
                propertiesProvider);
    }

    @Provides
    public static SnowflakeTableProperties tableProperties(SnowflakeConfig config)
    {
        return new SnowflakeTableProperties(config);
    }

    @Singleton
    public static SnowflakeSinkProvider configureSnowflakeSink(
            ConnectionFactory factory,
            JdbcClient client,
            RemoteQueryModifier modifier,
            SnowflakeConfig snowflakeConfig,
            SnowflakeTableProperties propertiesProvider)
    {
        return new SnowflakeSinkProvider(factory, client, modifier, snowflakeConfig, propertiesProvider);
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

   /* @Provides
    @Singleton
    public static SnowflakeMetadata metadata(SnowflakeClient jdbcClient, boolean precalculateStatisticsForPushdown, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        return new SnowflakeMetadata(jdbcClient, precalculateStatisticsForPushdown, jdbcQueryEventListeners);
    }*/

    @Override
    public void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(SnowflakeClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(SnowflakeConfig.class);
        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        //install(new JdbcDiagnosticModule());
        install(new IdentifierMappingModule());
        install(new RemoteQueryModifierModule());

        newOptionalBinder(binder, ConnectorAccessControl.class);
        newOptionalBinder(binder, QueryBuilder.class).setDefault().to(DefaultQueryBuilder.class).in(Scopes.SINGLETON);

        procedureBinder(binder);
        tablePropertiesProviderBinder(binder);

        newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(DefaultJdbcMetadataFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(ConnectorSplitManager.class, ForJdbcDynamicFiltering.class)).setDefault().to(JdbcSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorSplitManager.class).setBinding().to(JdbcDynamicFilteringSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorRecordSetProvider.class).setBinding().to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorPageSinkProvider.class).setBinding().to(SnowflakeSinkProvider.class).in(Scopes.SINGLETON);

        binder.bind(JdbcTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(JdbcConnector.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(JdbcMetadataConfig.class);
        configBinder(binder).bindConfig(JdbcWriteConfig.class);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(SnowflakeConfig.class);
        configBinder(binder).bindConfig(JdbcDynamicFilteringConfig.class);
        bindTablePropertiesProvider(binder, SnowflakeTableProperties.class);

        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        bindSessionPropertiesProvider(binder, TypeHandlingJdbcSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcMetadataSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcWriteSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcDynamicFilteringSessionProperties.class);
        bindSessionPropertiesProvider(binder, SnowflakeSessionPropertiesProvider.class);

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

        newSetBinder(binder, JdbcQueryEventListener.class);
        //binder.bind(ConnectorMetadata.class).annotatedWith(ForClassLoaderSafe.class).to(SnowflakeMetadata.class).in(Scopes.SINGLETON);
    }

    @PreDestroy
    public void shutdownRecordCursorExecutor(@ForRecordCursor ExecutorService executor)
    {
        executor.shutdownNow();
    }
}
