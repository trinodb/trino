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

import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.SQLException;

import static io.airlift.units.Duration.nanosSince;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSqlServerConnectionFactory
{
    @Test
    public void testConnectSocketTimeoutAppliedToHandshake()
            throws Exception
    {
        // A server that accepts TCP connections but never responds, like a black-holed SQL Server endpoint.
        // The driver blocks reading the prelogin handshake response; only socketTimeout bounds that read.
        try (ServerSocket silentServer = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            SqlServerConfig sqlServerConfig = new SqlServerConfig()
                    .setConnectSocketTimeout(new Duration(1, SECONDS));
            try (ConnectionFactory connectionFactory = connectionFactory(
                    "jdbc:sqlserver://%s:%s".formatted(silentServer.getInetAddress().getHostAddress(), silentServer.getLocalPort()),
                    sqlServerConfig,
                    StaticCredentialProvider.of("user", "password"))) {
                long start = System.nanoTime();
                assertThatThrownBy(() -> connectionFactory.openConnection(SESSION))
                        .isInstanceOf(SQLException.class);
                // Without socketTimeout the read blocks until the driver's loginTimeout (30s default) at best
                assertThat(nanosSince(start)).isLessThan(new Duration(20, SECONDS));
            }
        }
    }

    @Test
    public void testSocketTimeoutRelaxedAfterHandshake()
            throws Exception
    {
        try (TestingSqlServer sqlServer = new TestingSqlServer()) {
            CredentialProvider credentialProvider = StaticCredentialProvider.of(sqlServer.getUsername(), sqlServer.getPassword());

            SqlServerConfig defaultTimeouts = new SqlServerConfig();
            try (ConnectionFactory connectionFactory = connectionFactory(sqlServer.getJdbcUrl(), defaultTimeouts, credentialProvider);
                    Connection connection = connectionFactory.openConnection(SESSION)) {
                assertThat(connection.getNetworkTimeout()).isEqualTo(0);
            }

            SqlServerConfig explicitSocketTimeout = new SqlServerConfig()
                    .setSocketTimeout(new Duration(10, MINUTES));
            try (ConnectionFactory connectionFactory = connectionFactory(sqlServer.getJdbcUrl(), explicitSocketTimeout, credentialProvider);
                    Connection connection = connectionFactory.openConnection(SESSION)) {
                assertThat(connection.getNetworkTimeout()).isEqualTo(MINUTES.toMillis(10));
            }
        }
    }

    private static ConnectionFactory connectionFactory(String connectionUrl, SqlServerConfig sqlServerConfig, CredentialProvider credentialProvider)
    {
        return SqlServerConnectionFactoryModule.getConnectionFactory(
                new BaseJdbcConfig().setConnectionUrl(connectionUrl),
                sqlServerConfig,
                credentialProvider,
                OpenTelemetry.noop());
    }
}
