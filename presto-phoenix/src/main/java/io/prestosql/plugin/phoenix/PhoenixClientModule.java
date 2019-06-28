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
package io.prestosql.plugin.phoenix;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.InternalBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcPageSinkProvider;
import io.prestosql.plugin.jdbc.JdbcRecordSetProvider;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_CONFIG_ERROR;

public class PhoenixClientModule
        extends AbstractConfigurationAwareModule
{
    private final TypeManager typeManager;

    public PhoenixClientModule(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(ConnectorSplitManager.class).to(PhoenixSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(JdbcPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(PhoenixClient.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class)
                // TODO support JMX stats collection for phoenix connector
                .annotatedWith(InternalBaseJdbc.class)
                .to(PhoenixClient.class)
                .in(Scopes.SINGLETON);
        binder.bind(PhoenixMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PhoenixTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(PhoenixColumnProperties.class).in(Scopes.SINGLETON);
        binder.bind(TypeManager.class).toInstance(typeManager);

        checkConfiguration(buildConfigObject(PhoenixConfig.class).getConnectionUrl());
    }

    private void checkConfiguration(String connectionUrl)
    {
        try {
            PhoenixDriver driver = PhoenixDriver.INSTANCE;
            checkArgument(driver.acceptsURL(connectionUrl), "Invalid JDBC URL for Phoenix connector");
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_CONFIG_ERROR, e);
        }
    }

    @Provides
    @Singleton
    public ConnectionFactory getConnectionFactory(PhoenixConfig config)
            throws SQLException
    {
        return new DriverConnectionFactory(
                DriverManager.getDriver(config.getConnectionUrl()),
                config.getConnectionUrl(),
                Optional.empty(),
                Optional.empty(),
                getConnectionProperties(config));
    }

    public static Properties getConnectionProperties(PhoenixConfig config)
            throws SQLException
    {
        Configuration resourcesConfig = readConfig(config);
        Properties connectionProperties = new Properties();
        for (Map.Entry<String, String> entry : resourcesConfig) {
            connectionProperties.put(entry.getKey(), entry.getValue());
        }

        PhoenixEmbeddedDriver.ConnectionInfo connectionInfo = PhoenixEmbeddedDriver.ConnectionInfo.create(config.getConnectionUrl());
        connectionProperties.putAll(connectionInfo.asProps().asMap());
        return connectionProperties;
    }

    private static Configuration readConfig(PhoenixConfig config)
    {
        Configuration result = new Configuration(false);
        for (String resourcePath : config.getResourceConfigFiles()) {
            result.addResource(new Path(resourcePath));
        }
        return result;
    }
}
