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
package io.trino.plugin.mariadb;

import io.trino.testing.ResourcePresence;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;

public class TestingMariaDbServer
        implements AutoCloseable
{
    public static final String LATEST_VERSION = "10.7.1";
    public static final String DEFAULT_VERSION = "10.2";
    private static final int MARIADB_PORT = 3306;

    private final MariaDBContainer<?> container;

    public TestingMariaDbServer()
    {
        this(DEFAULT_VERSION);
    }

    public TestingMariaDbServer(String tag)
    {
        container = new MariaDBContainer<>(DockerImageName.parse("mariadb").withTag(tag))
                .withDatabaseName("tpch");
        // character-set-server：the default character set is latin1
        // explicit-defaults-for-timestamp: 1 is ON, the default set is 0 (OFF)
        container.withCommand("--character-set-server", "utf8mb4", "--explicit-defaults-for-timestamp=1");
        container.start();
        execute(format("GRANT ALL PRIVILEGES ON *.* TO '%s'", container.getUsername()), "root", container.getPassword());
    }

    public void execute(String sql)
    {
        execute(sql, getUsername(), getPassword());
    }

    private void execute(String sql, String user, String password)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), user, password);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getUsername()
    {
        return container.getUsername();
    }

    public String getPassword()
    {
        return container.getPassword();
    }

    public String getJdbcUrl()
    {
        return format("jdbc:mariadb://%s:%s", container.getHost(), container.getMappedPort(MARIADB_PORT));
    }

    @Override
    public void close()
    {
        container.close();
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return container.getContainerId() != null;
    }
}
