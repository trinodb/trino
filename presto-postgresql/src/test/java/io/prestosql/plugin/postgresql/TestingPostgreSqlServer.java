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

import org.testcontainers.containers.PostgreSQLContainer;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public class TestingPostgreSqlServer
        implements Closeable
{
    private static final String USER = "test";
    private static final String PASSWORD = "test";
    private static final String DATABASE = "tpch";

    private final PostgreSQLContainer<?> dockerContainer;

    public TestingPostgreSqlServer()
    {
        // Use the oldest supported PostgreSQL version
        dockerContainer = new PostgreSQLContainer<>("postgres:9.5")
                .withDatabaseName(DATABASE)
                .withUsername(USER)
                .withPassword(PASSWORD);
        dockerContainer.start();
    }

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
        // TODO we should not encode user and password in JDBC url, instead connection-user and connection-password catalog properties should be used
        return format("jdbc:postgresql://%s:%s/%s?user=%s&password=%s", dockerContainer.getContainerIpAddress(), dockerContainer.getMappedPort(POSTGRESQL_PORT), DATABASE, USER, PASSWORD);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
