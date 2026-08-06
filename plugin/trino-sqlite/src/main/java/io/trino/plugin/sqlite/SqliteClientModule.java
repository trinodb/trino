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
package io.trino.plugin.sqlite;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;
import org.sqlite.JDBC;

import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;
import static io.trino.plugin.sqlite.SqliteConfig.SQLITE_OPEN_MODE;

public class SqliteClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(SqliteClient.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(SqliteMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(SqliteConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(SqliteJdbcConfig.class);
        configBinder(binder).bindConfig(SqliteConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        bindTablePropertiesProvider(binder, SqliteTableProperties.class);
        binder.bind(SqliteColumnProperties.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory connectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, SqliteConfig sqliteConfig, OpenTelemetry openTelemetry)
    {
        DriverConnectionFactory.Builder factoryBuilder = DriverConnectionFactory.builder(new JDBC(), config.getConnectionUrl(), credentialProvider)
                .setOpenTelemetry(openTelemetry);

        if (sqliteConfig.hasOpenModeFlags()) {
            factoryBuilder.setConnectionProperties(getConnectionProperties(sqliteConfig));
        }

        return factoryBuilder.build();
    }

    private static Properties getConnectionProperties(SqliteConfig sqliteConfig)
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(SQLITE_OPEN_MODE, String.valueOf(sqliteConfig.getOpenModeFlags()));
        return connectionProperties;
    }
}
