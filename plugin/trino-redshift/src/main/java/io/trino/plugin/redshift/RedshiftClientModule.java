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
package io.trino.plugin.redshift;

import com.amazon.redshift.Driver;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.S3FileSystemModule;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.ForJdbcDynamicFiltering;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcConnector;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.RemoteQueryCancellationModule;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.util.Properties;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;

public class RedshiftClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(RedshiftConfig.class);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(RedshiftClient.class).in(SINGLETON);
        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setBulkListColumns(true));
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(SINGLETON);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        bindSessionPropertiesProvider(binder, RedshiftSessionProperties.class);

        install(new DecimalModule());
        install(new JdbcJoinPushdownSupportModule());
        install(new RemoteQueryCancellationModule());

        install(conditionalModule(
                RedshiftConfig.class,
                config -> config.getUnloadLocation().isPresent(),
                unloadBinder -> {
                    install(new S3FileSystemModule());
                    unloadBinder.bind(JdbcSplitManager.class).in(Scopes.SINGLETON);
                    unloadBinder.bind(Connector.class).to(RedshiftUnloadConnector.class).in(Scopes.SINGLETON);
                    unloadBinder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);

                    newSetBinder(unloadBinder, JdbcQueryEventListener.class).addBinding().to(RedshiftUnloadJdbcQueryEventListener.class).in(Scopes.SINGLETON);

                    newOptionalBinder(unloadBinder, Key.get(ConnectorSplitManager.class, ForJdbcDynamicFiltering.class))
                            .setBinding().to(RedshiftSplitManager.class).in(SINGLETON);
                },
                jdbcBinder -> jdbcBinder.bind(Connector.class).to(JdbcConnector.class).in(Scopes.SINGLETON)));
    }

    @Singleton
    @Provides
    @ForBaseJdbc
    public static ConnectionFactory getConnectionFactory(
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            OpenTelemetry openTelemetry)
    {
        Properties properties = new Properties();
        properties.put("reWriteBatchedInserts", "true");
        properties.put("reWriteBatchedInsertsSize", "512");

        return DriverConnectionFactory.builder(new Driver(), config.getConnectionUrl(), credentialProvider)
                .setConnectionProperties(properties)
                .setOpenTelemetry(openTelemetry)
                .build();
    }
}
