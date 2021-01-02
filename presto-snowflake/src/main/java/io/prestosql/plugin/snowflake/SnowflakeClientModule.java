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
package io.prestosql.plugin.snowflake;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import net.snowflake.client.jdbc.SnowflakeDriver;

import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class SnowflakeClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(Key.get(JdbcClient.class, ForBaseJdbc.class))
                .to(SnowflakeClient.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(SnowflakeConfig.class);
    }

    @Singleton
    @Provides
    @ForBaseJdbc
    public ConnectionFactory getConnectionFactory(BaseJdbcConfig baseJdbcConfig, SnowflakeConfig snowflakeConfig, CredentialProvider credentialProvider)
    {
        Properties properties = new Properties();
        snowflakeConfig.getAccount().ifPresent(account -> properties.setProperty("account", account));
        snowflakeConfig.getDatabase().ifPresent(database -> properties.setProperty("db", database));
        snowflakeConfig.getRole().ifPresent(role -> properties.setProperty("role", role));
        snowflakeConfig.getWarehouse().ifPresent(warehouse -> properties.setProperty("warehouse", warehouse));

        return new DriverConnectionFactory(new SnowflakeDriver(), baseJdbcConfig.getConnectionUrl(), properties, credentialProvider);
    }
}
