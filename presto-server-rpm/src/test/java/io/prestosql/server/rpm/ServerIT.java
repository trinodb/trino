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
package io.prestosql.server.rpm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testcontainers.containers.BindMode;
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

import static io.prestosql.testing.assertions.Assert.assertEventually;
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
    @Parameters("rpm")
    @Test
    public void testWithJava11(String rpm)
    {
        testServer("prestodev/centos7-oj11", rpm, "11");
    }

    private static void testServer(String baseImage, String rpmHostPath, String expectedJavaVersion)
    {
        String rpm = "/" + new File(rpmHostPath).getName();

        String command = "" +
                // install RPM
                "yum localinstall -q -y " + rpm + "\n" +
                // create Hive catalog file
                "mkdir /etc/presto/catalog\n" +
                "cat > /etc/presto/catalog/hive.properties <<\"EOT\"\n" +
                "connector.name=hive-hadoop2\n" +
                "hive.metastore.uri=thrift://localhost:9083\n" +
                "EOT\n" +
                // create JMX catalog file
                "cat > /etc/presto/catalog/jmx.properties <<\"EOT\"\n" +
                "connector.name=jmx\n" +
                "EOT\n" +
                // start server
                "/etc/init.d/presto start\n" +
                // allow tail to work with Docker's non-local file system
                "tail ---disable-inotify -F /var/log/presto/server.log\n";

        try (GenericContainer<?> container = new GenericContainer<>(baseImage)) {
            container.withExposedPorts(8080)
                    // the RPM is hundreds MB and file system bind is much more efficient
                    .withFileSystemBind(rpmHostPath, rpm, BindMode.READ_ONLY)
                    .withCommand("sh", "-xeuc", command)
                    .waitingFor(forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(Duration.ofMinutes(5)))
                    .start();
            QueryRunner queryRunner = new QueryRunner(container.getContainerIpAddress(), container.getMappedPort(8080));
            assertEquals(queryRunner.execute("SHOW CATALOGS"), ImmutableSet.of(asList("system"), asList("hive"), asList("jmx")));
            // TODO remove usage of assertEventually once https://github.com/prestosql/presto/issues/2214 is fixed
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
            try (Connection connection = getConnection(format("jdbc:presto://%s:%s", host, port), "test", null);
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
