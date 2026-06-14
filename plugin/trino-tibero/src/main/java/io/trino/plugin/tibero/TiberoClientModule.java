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
package io.trino.plugin.tibero;

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
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class TiberoClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(TiberoClient.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, OpenTelemetry openTelemetry)
    {
        // The Tibero JDBC driver (tibero7-jdbc.jar) is not available in public Maven repositories
        // and cannot be bundled with the connector. Users must obtain the driver from TmaxSoft
        // and place it in the plugin directory manually (see connector documentation).
        //
        // Driver loading is deferred to the first actual connection attempt to allow the connector
        // to initialize successfully even when the driver is not present at startup time.
        // This enables plugin loading and configuration validation without requiring the driver JAR.
        String connectionUrl = config.getConnectionUrl();
        return new ConnectionFactory()
        {
            private volatile ConnectionFactory delegate;

            @Override
            public Connection openConnection(ConnectorSession session)
                    throws SQLException
            {
                return getDelegate().openConnection(session);
            }

            @Override
            public void close()
                    throws SQLException
            {
                if (delegate != null) {
                    delegate.close();
                }
            }

            private ConnectionFactory getDelegate()
                    throws SQLException
            {
                if (delegate == null) {
                    synchronized (this) {
                        if (delegate == null) {
                            delegate = DriverConnectionFactory.builder(
                                            DriverManager.getDriver(connectionUrl),
                                            connectionUrl,
                                            credentialProvider)
                                    .setConnectionProperties(new Properties())
                                    .setOpenTelemetry(openTelemetry)
                                    .build();
                        }
                    }
                }
                return delegate;
            }
        };
    }
}
