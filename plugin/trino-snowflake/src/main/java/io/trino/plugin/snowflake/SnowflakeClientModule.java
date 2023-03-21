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
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConfiguringConnectionFactory;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.ptf.ConnectorTableFunction;
import net.snowflake.client.jdbc.SnowflakeDriver;

import java.net.MalformedURLException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
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
        checkArgument(catalog != null && !catalog.equals(""),
                new RuntimeException("Snowflake catalog is set to null. Please set snowflake.catalog"));
        checkArgument(warehouse != null && !warehouse.equals(""),
                new RuntimeException("Snowflake warehouse is set to null. Please set snowflake.warehouse"));

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

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(SnowflakeClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(SnowflakeConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        install(new DecimalModule());
        install(new JdbcJoinPushdownSupportModule());
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }
}
