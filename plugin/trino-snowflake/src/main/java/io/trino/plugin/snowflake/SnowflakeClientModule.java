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
package io.trino.plugin.snowflake;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.snowflake.client.jdbc.SnowflakeDriver;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalConfig;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.TypeHandlingJdbcConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;

import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class SnowflakeClientModule
        implements Module
{
    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, SnowflakeConfig snowflakeConfig)
    {
        return new DriverConnectionFactory(new SnowflakeDriver(), config.getConnectionUrl(), getConnectionProperties(snowflakeConfig), credentialProvider);
    }

    private static Properties getConnectionProperties(SnowflakeConfig snowflakeConfig)
    {
        Properties connectionProperties = new Properties();
        snowflakeConfig.getAccount().ifPresent(account -> connectionProperties.setProperty("account", account));
        snowflakeConfig.getDatabase().ifPresent(database -> connectionProperties.setProperty("db", database));
        snowflakeConfig.getRole().ifPresent(role -> connectionProperties.setProperty("role", role));
        if (snowflakeConfig.getWarehouse() != null) {
            connectionProperties.setProperty("warehouse", snowflakeConfig.getWarehouse());
        }

        return connectionProperties;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(SnowflakeClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        configBinder(binder).bindConfig(SnowflakeConfig.class);
        configBinder(binder).bindConfig(DecimalConfig.class);
        binder.install(new DecimalModule());
    }
}
