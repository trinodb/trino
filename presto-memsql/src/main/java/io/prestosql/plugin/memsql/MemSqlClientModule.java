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
package io.prestosql.plugin.memsql;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import org.mariadb.jdbc.Driver;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class MemSqlClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(Key.get(JdbcClient.class, ForBaseJdbc.class))
                .to(MemSqlClient.class).in(Scopes.SINGLETON);
        ensureCatalogIsEmpty(buildConfigObject(BaseJdbcConfig.class).getConnectionUrl());
        configBinder(binder).bindConfig(MemSqlConfig.class);
    }

    private static void ensureCatalogIsEmpty(String connectionUrl)
    {
        try {
            Driver driver = new Driver();
            DriverPropertyInfo[] urlProperties = driver.getPropertyInfo(connectionUrl, null);
            checkArgument(urlProperties != null, "Invalid JDBC URL for MemSQL connector: '%s'", connectionUrl);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, MemSqlConfig memsqlConfig)
    {
        Properties connectionProperties = new Properties();
        // we don't want to interpret tinyInt type (with cardinality as 2) as boolean/bit
        connectionProperties.setProperty("tinyInt1isBit", "false");
        connectionProperties.setProperty("autoReconnect", String.valueOf(memsqlConfig.isAutoReconnect()));
        connectionProperties.setProperty("connectTimeout", String.valueOf(memsqlConfig.getConnectionTimeout().toMillis()));

        return new DriverConnectionFactory(
                new Driver(),
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider);
    }
}
