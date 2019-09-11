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
package io.prestosql.plugin.postgresql;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.docker.DockerContainer;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;

public class TestingPostgreSqlServer
        implements Closeable
{
    private static final String USER = "test";
    private static final String PASSWORD = "test";
    private static final String DATABASE = "tpch";
    private static final int POSTGRESQL_PORT = 5432;

    private final DockerContainer dockerContainer = DockerContainer.forImage("postgres:10.3")
            .setPorts(POSTGRESQL_PORT)
            .setEnvironment(ImmutableMap.of(
                    "POSTGRES_PASSWORD", PASSWORD,
                    "POSTGRES_USER", USER,
                    "POSTGRES_DB", DATABASE))
            .setHealthCheck(docker -> execute(getJdbcUrl(docker), "SELECT 1"))
            .start();

    public void execute(String sql)
            throws SQLException
    {
        execute(getJdbcUrl(), sql);
    }

    private static void execute(String url, String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(url);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    public String getJdbcUrl()
    {
        return getJdbcUrl(this.dockerContainer::getHostPort);
    }

    private String getJdbcUrl(DockerContainer.HostPortProvider dockerContainer)
    {
        return format("jdbc:postgresql://localhost:%s/%s?user=%s&password=%s", dockerContainer.getHostPort(POSTGRESQL_PORT), DATABASE, USER, PASSWORD);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
