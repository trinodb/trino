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
package io.prestosql.plugin.sqlserver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.docker.DockerContainer;
import io.prestosql.testing.docker.DockerContainer.HostPortProvider;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static java.lang.String.format;

public final class TestingSqlServer
        implements Closeable
{
    private static final int SQL_SERVER_PORT = 1433;

    public static final String USER = "sa";
    public static final String PASSWORD = "SQLServerPass1";

    private final DockerContainer dockerContainer;

    public TestingSqlServer()
    {
        this.dockerContainer = new DockerContainer(
                "microsoft/mssql-server-linux:2017-CU13",
                ImmutableList.of(SQL_SERVER_PORT),
                ImmutableMap.of(
                        "ACCEPT_EULA", "Y",
                        "SA_PASSWORD", "SQLServerPass1"),
                portProvider -> TestingSqlServer.execute(portProvider, "SELECT 1"));
    }

    public void execute(String sql)
    {
        execute(dockerContainer::getHostPort, sql);
    }

    private static void execute(HostPortProvider hostPortProvider, String sql)
    {
        String jdbcUrl = getJdbcUrl(hostPortProvider.getHostPort(SQL_SERVER_PORT));
        try (Connection conn = DriverManager.getConnection(jdbcUrl, USER, PASSWORD);
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    public String getJdbcUrl()
    {
        return getJdbcUrl(dockerContainer.getHostPort(SQL_SERVER_PORT));
    }

    private static String getJdbcUrl(int port)
    {
        return format("jdbc:sqlserver://localhost:%s", port);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
