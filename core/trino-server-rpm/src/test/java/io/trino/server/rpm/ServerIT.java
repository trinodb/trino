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
package io.trino.server.rpm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.sql.DriverManager.getConnection;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class ServerIT
{
    private static final String BASE_IMAGE = "ghcr.io/trinodb/testing/centos7-oj11";

    @Parameters("rpm")
    @Test
    public void testWithJava11(String rpm)
    {
        testServer(rpm, "11");
    }

    @Parameters("rpm")
    @Test
    public void testUninstall(String rpmHostPath)
            throws Exception
    {
        String rpm = "/" + new File(rpmHostPath).getName();
        String installAndStartTrino = "" +
                "yum localinstall -q -y " + rpm + "\n" +
                "/etc/init.d/trino start\n" +
                // allow tail to work with Docker's non-local file system
                "tail ---disable-inotify -F /var/log/trino/server.log\n";
        try (GenericContainer<?> container = new GenericContainer<>(BASE_IMAGE)) {
            container.withFileSystemBind(rpmHostPath, rpm, BindMode.READ_ONLY)
                    .withCommand("sh", "-xeuc", installAndStartTrino)
                    .waitingFor(forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(Duration.ofMinutes(5)))
                    .start();
            String uninstallTrino = "" +
                    "/etc/init.d/trino stop\n" +
                    "rpm -e trino-server-rpm\n";
            container.execInContainer("sh", "-xeuc", uninstallTrino);

            ExecResult actual = container.execInContainer("rpm", "-q", "trino-server-rpm");
            assertEquals(actual.getStdout(), "package trino-server-rpm is not installed\n");

            assertPathDeleted(container, "/var/lib/trino");
            assertPathDeleted(container, "/usr/lib/trino");
            assertPathDeleted(container, "/etc/init.d/trino");
            assertPathDeleted(container, "/usr/shared/doc/trino");
        }
    }

    private static void assertPathDeleted(GenericContainer container, String path)
            throws Exception
    {
        ExecResult actualResult = container.execInContainer(
                "sh",
                "-xeuc",
                format("test -d %s && echo -n 'path exists' || echo -n 'path deleted'", path));
        assertEquals(actualResult.getStdout(), "path deleted");
        assertEquals(actualResult.getExitCode(), 0);
    }

    private static void testServer(String rpmHostPath, String expectedJavaVersion)
    {
        String rpm = "/" + new File(rpmHostPath).getName();

        String command = "" +
                // install RPM
                "yum localinstall -q -y " + rpm + "\n" +
                // create Hive catalog file
                "mkdir /etc/trino/catalog\n" +
                "echo CONFIG_ENV[HMS_PORT]=9083 >> /etc/trino/env.sh\n" +
                "echo CONFIG_ENV[NODE_ID]=test-node-id-injected-via-env >> /etc/trino/env.sh\n" +
                "sed -i \"s/^node.id=.*/node.id=\\${ENV:NODE_ID}/g\" /etc/trino/node.properties\n" +
                "cat > /etc/trino/catalog/hive.properties <<\"EOT\"\n" +
                "connector.name=hive\n" +
                "hive.metastore.uri=thrift://localhost:${ENV:HMS_PORT}\n" +
                "EOT\n" +
                // create JMX catalog file
                "cat > /etc/trino/catalog/jmx.properties <<\"EOT\"\n" +
                "connector.name=jmx\n" +
                "EOT\n" +
                // start server
                "/etc/init.d/trino start\n" +
                // allow tail to work with Docker's non-local file system
                "tail ---disable-inotify -F /var/log/trino/server.log\n";

        try (GenericContainer<?> container = new GenericContainer<>(BASE_IMAGE)) {
            container.withExposedPorts(8080)
                    // the RPM is hundreds MB and file system bind is much more efficient
                    .withFileSystemBind(rpmHostPath, rpm, BindMode.READ_ONLY)
                    .withCommand("sh", "-xeuc", command)
                    .waitingFor(forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(Duration.ofMinutes(5)))
                    .start();
            QueryRunner queryRunner = new QueryRunner(container.getContainerIpAddress(), container.getMappedPort(8080));
            assertEquals(queryRunner.execute("SHOW CATALOGS"), ImmutableSet.of(asList("system"), asList("hive"), asList("jmx")));
            assertEquals(queryRunner.execute("SELECT node_id FROM system.runtime.nodes"), ImmutableSet.of(asList("test-node-id-injected-via-env")));
            // TODO remove usage of assertEventually once https://github.com/trinodb/trino/issues/2214 is fixed
            assertEventually(
                    new io.airlift.units.Duration(1, MINUTES),
                    () -> assertEquals(queryRunner.execute("SELECT specversion FROM jmx.current.\"java.lang:type=runtime\""), ImmutableSet.of(asList(expectedJavaVersion))));
        }
    }

    private static class QueryRunner
    {
        private final String host;
        private final int port;

        private QueryRunner(String host, int port)
        {
            this.host = requireNonNull(host, "host is null");
            this.port = port;
        }

        public Set<List<String>> execute(String sql)
        {
            try (Connection connection = getConnection(format("jdbc:trino://%s:%s", host, port), "test", null);
                    Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    ImmutableSet.Builder<List<String>> rows = ImmutableSet.builder();
                    int columnCount = resultSet.getMetaData().getColumnCount();
                    while (resultSet.next()) {
                        ImmutableList.Builder<String> row = ImmutableList.builder();
                        for (int column = 1; column <= columnCount; column++) {
                            row.add(resultSet.getString(column));
                        }
                        rows.add(row.build());
                    }
                    return rows.build();
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
