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
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.sql.SQLException;

import static io.airlift.units.Duration.nanosSince;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSqlServerConnectionFactory
{
    @Test
    void testConnectFailsWhenServerNeverResponds()
            throws Exception
    {
        // A server that accepts TCP connections but never responds, like a black-holed SQL Server endpoint.
        // The driver blocks reading the prelogin handshake response, so the read must be bounded by loginTimeout.
        // mssql-jdbc before 13.6.0 left the socket without any read timeout, so this hung forever (microsoft/mssql-jdbc#2925).
        try (ServerSocket silentServer = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            String connectionUrl = "jdbc:sqlserver://%s:%s;loginTimeout=2".formatted(silentServer.getInetAddress().getHostAddress(), silentServer.getLocalPort());
            try (ConnectionFactory connectionFactory = SqlServerConnectionFactoryModule.getConnectionFactory(
                    new BaseJdbcConfig().setConnectionUrl(connectionUrl),
                    new SqlServerConfig(),
                    StaticCredentialProvider.of("user", "password"),
                    OpenTelemetry.noop())) {
                long start = System.nanoTime();
                assertThatThrownBy(() -> connectionFactory.openConnection(SESSION))
                        .isInstanceOf(SQLException.class)
                        .hasRootCauseInstanceOf(SocketTimeoutException.class);
                Duration elapsed = nanosSince(start);
                // Not an immediate failure, but bounded by loginTimeout rather than blocking indefinitely
                assertThat(elapsed).isGreaterThanOrEqualTo(new Duration(1, SECONDS));
                assertThat(elapsed).isLessThan(new Duration(10, SECONDS));
            }
        }
    }
}
