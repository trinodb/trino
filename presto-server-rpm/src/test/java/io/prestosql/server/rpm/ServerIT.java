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
import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;
import static java.sql.DriverManager.getConnection;
import static java.util.Arrays.asList;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;
import static org.testng.Assert.assertEquals;

public class ServerIT
{
    @Parameters("rpm")
    @Test
    public void testWithJava8(String rpm)
            throws Exception
    {
        testServer("prestodev/centos7-oj8", rpm);
    }

    @Parameters("rpm")
    @Test
    public void testWithJava11(String rpm)
            throws Exception
    {
        testServer("prestodev/centos7-oj11", rpm);
    }

    private static void testServer(String baseImage, String rpmHostPath)
            throws Exception
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
                // start server
                "/etc/init.d/presto start\n" +
                // allow tail to work with Docker's non-local file system
                "tail ---disable-inotify -F /var/log/presto/server.log\n";

        try (GenericContainer<?> container = new GenericContainer<>(baseImage)) {
            container.withExposedPorts(8080)
                    .withFileSystemBind(rpmHostPath, rpm, BindMode.READ_ONLY)
                    .withCommand("sh", "-xeuc", command)
                    .waitingFor(forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(Duration.ofMinutes(5)))
                    .start();

            assertServer(container.getContainerIpAddress(), container.getMappedPort(8080));
        }
    }

    private static void assertServer(String host, int port)
            throws SQLException
    {
        String url = format("jdbc:presto://%s:%s", host, port);
        try (Connection connection = getConnection(url, "test", null);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SHOW CATALOGS")) {
            Set<String> catalogs = new HashSet<>();
            while (rs.next()) {
                catalogs.add(rs.getString(1));
            }
            assertEquals(catalogs, new HashSet<>(asList("system", "hive")));
        }
    }
}
