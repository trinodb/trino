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
package io.trino.plugin.iceberg.catalog.jdbc;

import org.apache.iceberg.jdbc.TestingTrinoIcebergJdbcUtil;
import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public class TestingIcebergJdbcServer
        implements Closeable
{
    public static final String USER = "test";
    public static final String PASSWORD = "test";
    private static final String DATABASE = "tpch";

    private final PostgreSQLContainer<?> dockerContainer;

    public TestingIcebergJdbcServer()
    {
        // TODO: Use Iceberg docker image once the community provides it
        dockerContainer = new PostgreSQLContainer<>("postgres:12.10")
                .withDatabaseName(DATABASE)
                .withUsername(USER)
                .withPassword(PASSWORD);
        dockerContainer.start();

        execute(TestingTrinoIcebergJdbcUtil.CREATE_CATALOG_TABLE);
        execute(TestingTrinoIcebergJdbcUtil.CREATE_NAMESPACE_PROPERTIES_TABLE);

        execute("CREATE SCHEMA tpch");
    }

    public void execute(@Language("SQL") String sql)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), USER, PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getJdbcUrl()
    {
        return format(
                "jdbc:postgresql://%s:%s/%s",
                dockerContainer.getHost(),
                dockerContainer.getMappedPort(POSTGRESQL_PORT),
                DATABASE);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
