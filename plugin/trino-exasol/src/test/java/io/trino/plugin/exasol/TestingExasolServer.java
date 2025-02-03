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

package io.trino.plugin.exasol;

import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolService;
import io.trino.testing.sql.JdbcSqlExecutor;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static io.trino.testing.containers.TestContainers.startOrReuse;
import static java.lang.String.format;

public class TestingExasolServer
        implements Closeable
{
    public static final String TEST_USER = "trino_test";
    /**
     * Name of the test schema. Must not contain an underscore, required by {@link io.trino.plugin.exasol.TestExasolConnectorTest#testShowSchemasLikeWithEscape()}
     */
    public static final String TEST_SCHEMA = "tpch";
    public static final String TEST_PASSWORD = "trino_test_password";

    private final ExasolContainer<?> container;

    private final Closeable cleanup;

    public TestingExasolServer()
    {
        container = new ExasolContainer<>("8.32.0")
                .withExposedPorts(8563)
                .withRequiredServices(ExasolService.JDBC);
        cleanup = startOrReuse(container);
        executeAsSys(format("CREATE USER %s IDENTIFIED BY \"%s\"", TEST_USER, TEST_PASSWORD));
        executeAsSys("GRANT CREATE SESSION TO " + TEST_USER);
        executeAsSys("GRANT CREATE SCHEMA TO " + TEST_USER);
        executeAsSys("GRANT CREATE TABLE TO " + TEST_USER);
        executeAsSys("GRANT IMPORT TO " + TEST_USER);
        executeAsSys("GRANT CREATE VIEW TO " + TEST_USER);
        executeAsSys("GRANT CREATE ANY VIEW TO " + TEST_USER);
        execute("CREATE SCHEMA " + TEST_SCHEMA);
    }

    public String getJdbcUrl()
    {
        return container.getJdbcUrl();
    }

    public void execute(@Language("SQL") String sql)
    {
        execute(sql, TEST_USER, TEST_PASSWORD);
    }

    private void executeAsSys(@Language("SQL") String sql)
    {
        execute(sql, container.getUsername(), container.getPassword());
    }

    public void execute(@Language("SQL") String sql, String user, String password)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), getProperties(user, password));
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute statement '" + sql + "'", e);
        }
    }

    public JdbcSqlExecutor getSqlExecutor()
    {
        return new JdbcSqlExecutor(getJdbcUrl(), getProperties(TEST_USER, TEST_PASSWORD));
    }

    private static Properties getProperties(String user, String password)
    {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        return properties;
    }

    @Override
    public void close()
    {
        try {
            cleanup.close();
        }
        catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }
}
