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
package io.trino.plugin.phoenix5;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.base.mapping.IdentifierMappingModule;
import io.trino.plugin.jdbc.ConfiguringConnectionFactory;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
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
import io.trino.plugin.jdbc.JdbcMetadataSessionProperties;
import io.trino.plugin.jdbc.JdbcPageSourceProvider;
import io.trino.plugin.jdbc.JdbcWriteConfig;
import io.trino.plugin.jdbc.JdbcWriteSessionProperties;
import io.trino.plugin.jdbc.LazyConnectionFactory;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RetryingConnectionFactory;
import io.trino.plugin.jdbc.RetryingModule;
import io.trino.plugin.jdbc.ReusableConnectionFactoryModule;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.plugin.jdbc.TypeHandlingJdbcConfig;
import io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties;
import io.trino.plugin.jdbc.credential.EmptyCredentialProvider;
import io.trino.plugin.jdbc.jmx.StatisticsAwareJdbcClient;
import io.trino.plugin.jdbc.logging.RemoteQueryModifierModule;
import io.trino.plugin.jdbc.procedure.ExecuteProcedure;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.procedure.Procedure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.jdbc.ConnectionInfo;
import org.apache.phoenix.jdbc.PhoenixDriver;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;
import static io.trino.plugin.phoenix5.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.phoenix5.PhoenixClient.DEFAULT_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.phoenix5.PhoenixErrorCode.PHOENIX_CONFIG_ERROR;
import static java.util.Objects.requireNonNull;
import static org.apache.phoenix.util.ReadOnlyProps.EMPTY_PROPS;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class PhoenixClientModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;

    public PhoenixClientModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        install(new RemoteQueryModifierModule());
        install(new RetryingModule());
        binder.bind(ConnectorSplitManager.class).annotatedWith(ForJdbcDynamicFiltering.class).to(PhoenixSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).annotatedWith(ForClassLoaderSafe.class).to(JdbcDynamicFilteringSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(ClassLoaderSafeConnectorSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).annotatedWith(ForClassLoaderSafe.class).to(PhoenixPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(ClassLoaderSafeConnectorPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).annotatedWith(ForClassLoaderSafe.class).to(JdbcPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(ClassLoaderSafeConnectorPageSourceProvider.class).in(Scopes.SINGLETON);

        binder.bind(QueryBuilder.class).to(DefaultQueryBuilder.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class));
        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setDomainCompactionThreshold(DEFAULT_DOMAIN_COMPACTION_THRESHOLD));

        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        bindSessionPropertiesProvider(binder, TypeHandlingJdbcSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcMetadataSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcWriteSessionProperties.class);
        bindSessionPropertiesProvider(binder, PhoenixSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcDynamicFilteringSessionProperties.class);

        binder.bind(DynamicFilteringStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(DynamicFilteringStats.class)
                .as(generator -> generator.generatedNameOf(DynamicFilteringStats.class, catalogName));

        configBinder(binder).bindConfig(JdbcMetadataConfig.class);
        configBinder(binder).bindConfig(JdbcWriteConfig.class);
        configBinder(binder).bindConfig(JdbcDynamicFilteringConfig.class);

        binder.bind(PhoenixClient.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(Key.get(PhoenixClient.class)).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).to(StatisticsAwareJdbcClient.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).annotatedWith(ForClassLoaderSafe.class).to(PhoenixMetadata.class).in(Scopes.SINGLETON);
        binder.bind(TimestampTimeZoneDomain.class).toInstance(TimestampTimeZoneDomain.ANY);
        binder.bind(ConnectorMetadata.class).to(ClassLoaderSafeConnectorMetadata.class).in(Scopes.SINGLETON);
        newSetBinder(binder, Procedure.class).addBinding().toProvider(ExecuteProcedure.class).in(Scopes.SINGLETON);

        binder.bind(ConnectionFactory.class).annotatedWith(ForLazyConnectionFactory.class).to(Key.get(RetryingConnectionFactory.class)).in(Scopes.SINGLETON);
        install(conditionalModule(
                PhoenixConfig.class,
                PhoenixConfig::isReuseConnection,
                new ReusableConnectionFactoryModule(),
                innerBinder -> innerBinder.bind(ConnectionFactory.class).to(LazyConnectionFactory.class).in(Scopes.SINGLETON)));

        bindTablePropertiesProvider(binder, PhoenixTableProperties.class);
        binder.bind(PhoenixColumnProperties.class).in(Scopes.SINGLETON);

        binder.bind(PhoenixConnector.class).in(Scopes.SINGLETON);

        checkConfiguration(buildConfigObject(PhoenixConfig.class).getConnectionUrl());

        install(new JdbcDiagnosticModule());
        install(new IdentifierMappingModule());
        install(new DecimalModule());

        closingBinder(binder)
                .registerExecutor(Key.get(ExecutorService.class, ForRecordCursor.class));
    }

    private void checkConfiguration(String connectionUrl)
    {
        try {
            PhoenixDriver driver = PhoenixDriver.INSTANCE;
            checkArgument(driver.acceptsURL(connectionUrl), "Invalid JDBC URL for Phoenix connector");
        }
        catch (SQLException e) {
            throw new TrinoException(PHOENIX_CONFIG_ERROR, e);
        }
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public ConnectionFactory getConnectionFactory(PhoenixConfig config, OpenTelemetry openTelemetry)
            throws SQLException
    {
        return new ConfiguringConnectionFactory(
                DriverConnectionFactory.builder(
                                PhoenixDriver.INSTANCE, // Note: for some reason new PhoenixDriver won't work.
                                config.getConnectionUrl(),
                                new EmptyCredentialProvider())
                        .setConnectionProperties(getConnectionProperties(config))
                        .setOpenTelemetry(openTelemetry)
                        .build(),
                connection -> {
                    // Per JDBC spec, a Driver is expected to have new connections in auto-commit mode.
                    // This seems not to be true for PhoenixDriver, so we need to be explicit here.
                    connection.setAutoCommit(true);
                });
    }

    public static Properties getConnectionProperties(PhoenixConfig config)
            throws SQLException
    {
        Configuration resourcesConfig = readConfig(config);
        Properties connectionProperties = new Properties();
        for (Map.Entry<String, String> entry : resourcesConfig) {
            connectionProperties.setProperty(entry.getKey(), entry.getValue());
        }

        ConnectionInfo connectionInfo = ConnectionInfo.create(config.getConnectionUrl(), EMPTY_PROPS, new Properties());
        connectionInfo.asProps().asMap().forEach(connectionProperties::setProperty);
        return connectionProperties;
    }

    private static Configuration readConfig(PhoenixConfig config)
    {
        Configuration result = newEmptyConfiguration();
        for (String resourcePath : config.getResourceConfigFiles()) {
            result.addResource(new Path(resourcePath));
        }
        return result;
    }

    @Singleton
    @ForRecordCursor
    @Provides
    public ExecutorService createRecordCursorExecutor()
    {
        return newDirectExecutorService();
    }
}
