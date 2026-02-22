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
package io.trino.tests.product.tls;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
@RequiresEnvironment(TlsEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.Tls
@TestGroup.ProfileSpecificTests
class TestTlsJunit
{
    @Test
    void testHttpPortIsClosed(TlsEnvironment env)
            throws Exception
    {
        waitForNodeRefresh(env);

        try (Connection connection = env.createTrinoConnection("hive");
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT 1")) {
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(1);
            assertThat(resultSet.next()).isFalse();
        }

        // HTTP endpoint should not accept non-TLS queries for TLS-only config.
        assertThat(canQueryOverHttp(env)).isFalse();
    }

    private static void waitForNodeRefresh(TlsEnvironment env)
            throws InterruptedException
    {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (isPortOpen(env.getHost(), env.getHttpsPort())) {
                return;
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("TLS HTTPS endpoint did not become reachable");
    }

    private static boolean isPortOpen(String host, int port)
    {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1_000);
            return true;
        }
        catch (SocketTimeoutException e) {
            return false;
        }
        catch (IOException e) {
            return false;
        }
    }

    private static boolean canQueryOverHttp(TlsEnvironment env)
    {
        String jdbcUrl = "jdbc:trino://%s:%d".formatted(env.getHost(), env.getHttpPort());
        try (Connection connection = DriverManager.getConnection(jdbcUrl, "hive", null);
                Statement statement = connection.createStatement()) {
            statement.execute("SELECT 1");
            return true;
        }
        catch (SQLException e) {
            return false;
        }
    }
}
