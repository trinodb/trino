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
package io.trino.plugin.sqlserver;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;

import java.util.Properties;

import static java.lang.Math.toIntExact;

public class SqlServerConnectionFactoryModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder) {}

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory getConnectionFactory(
            BaseJdbcConfig config,
            SqlServerConfig sqlServerConfig,
            CredentialProvider credentialProvider,
            OpenTelemetry openTelemetry)
    {
        Properties connectionProperties = new Properties();
        // Applies SO_TIMEOUT to every socket read of the TCP/prelogin/TLS/login handshake. The driver defaults
        // to an infinite timeout, and its loginTimeout does not cover a read blocked on an unresponsive server,
        // so without this a hung handshake parks the split thread forever. SqlServerConnectionFactory replaces
        // it with the steady-state socket timeout once the connection is established.
        connectionProperties.setProperty("socketTimeout", String.valueOf(sqlServerConfig.getConnectSocketTimeout().toMillis()));
        // Defaults to 0 to disable idle connection resiliency. It transparently reconnects only a connection
        // that broke while idle (a connection that breaks mid-query always fails), and reconnecting re-runs
        // the login sequence, which resets SO_TIMEOUT back to the socketTimeout property above and silently
        // discards the steady-state timeout that SqlServerConnectionFactory establishes. Trino connections
        // spend nearly all their life executing statements, so recovery is better left to Trino's own retry
        // mechanisms.
        connectionProperties.setProperty("connectRetryCount", String.valueOf(sqlServerConfig.getConnectRetryCount()));
        int socketTimeoutMillis = sqlServerConfig.getSocketTimeout()
                .map(timeout -> toIntExact(timeout.toMillis()))
                .orElse(0);
        return new SqlServerConnectionFactory(
                DriverConnectionFactory.builder(new SQLServerDriver(), config.getConnectionUrl(), credentialProvider)
                        .setConnectionProperties(connectionProperties)
                        .setOpenTelemetry(openTelemetry)
                        .build(),
                sqlServerConfig.isSnapshotIsolationDisabled(),
                socketTimeoutMillis);
    }
}
