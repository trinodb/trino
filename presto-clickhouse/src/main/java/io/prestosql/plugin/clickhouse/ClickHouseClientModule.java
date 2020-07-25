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
package io.prestosql.plugin.clickhouse;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DecimalModule;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import ru.yandex.clickhouse.ClickHouseDriver;

import java.sql.SQLException;
import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ClickHouseClientModule
        implements Module
{
    private static final Logger log = Logger.get(ClickHouseClientModule.class);

    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(ClickHouseClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(ClickHouseConfig.class);
        binder.install(new DecimalModule());
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(ClickHouseConfig config, CredentialProvider credentialProvider, ClickHouseConfig clickhouseConfig)
            throws SQLException
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("user", config.getConnectionuser());
        connectionProperties.setProperty("password", config.getConnectionpassword());
        if (clickhouseConfig.getConnectionTimeout() != null) {
            connectionProperties.setProperty("connectionTimeout", String.valueOf(clickhouseConfig.getConnectionTimeout().toMillis()));
        }
        return new DriverConnectionFactory(
                new ClickHouseDriver(),
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider);
    }
}
