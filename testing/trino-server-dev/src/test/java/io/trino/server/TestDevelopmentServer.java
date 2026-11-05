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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Throwables.getCausalChain;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.trino.spi.StandardErrorCode.SERVER_STARTING_UP;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDevelopmentServer
{
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(5);
    private static final Path LOG_FILE = Path.of("target", "development-server.log");
    private static final Set<Integer> STARTUP_ERROR_CODES = ImmutableSet.of(
            SERVER_STARTING_UP.toErrorCode().getCode(),
            NO_NODES_AVAILABLE.toErrorCode().getCode());

    @Test
    void testServerStarts()
            throws Exception
    {
        int port = findFreePort();
        Process process = startServer(port);
        try {
            assertThat(awaitQueryResult(process, "jdbc:trino://localhost:" + port, "SELECT count(*) FROM tpch.tiny.nation"))
                    .isEqualTo(25);
        }
        finally {
            stopServer(process);
        }
    }

    private static Process startServer(int port)
            throws IOException
    {
        // System properties take precedence over etc/config.properties, so main() keeps the configured port
        List<String> command = ImmutableList.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-modules=jdk.incubator.vector",
                "--sun-misc-unsafe-memory-access=allow",
                "-Dconfig=etc/config.properties",
                "-Dlog.levels-file=etc/log.properties",
                "-Dhttp-server.http.port=" + port,
                "-Ddiscovery.uri=http://localhost:" + port,
                "-cp",
                System.getProperty("java.class.path"),
                DevelopmentServer.class.getName());

        Files.createDirectories(LOG_FILE.getParent());
        return new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(LOG_FILE.toFile())
                .start();
    }

    private static long awaitQueryResult(Process process, String url, String sql)
            throws InterruptedException, SQLException
    {
        long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        while (true) {
            assertThat(process.isAlive())
                    .withFailMessage(() -> "Development server exited with code %s, see %s".formatted(process.exitValue(), LOG_FILE.toAbsolutePath()))
                    .isTrue();
            try (Connection connection = DriverManager.getConnection(url, "test", null);
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(sql)) {
                assertThat(resultSet.next()).isTrue();
                return resultSet.getLong(1);
            }
            catch (SQLException e) {
                if (!isStartingUp(e) || System.nanoTime() > deadline) {
                    throw new SQLException("Query failed against development server, see " + LOG_FILE.toAbsolutePath(), e);
                }
            }
            SECONDS.sleep(1);
        }
    }

    private static boolean isStartingUp(SQLException e)
    {
        return STARTUP_ERROR_CODES.contains(e.getErrorCode()) ||
                getCausalChain(e).stream().anyMatch(ConnectException.class::isInstance);
    }

    private static void stopServer(Process process)
            throws InterruptedException
    {
        process.destroy();
        if (!process.waitFor(30, SECONDS)) {
            process.destroyForcibly();
            process.waitFor(30, SECONDS);
        }
    }

    private static int findFreePort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
