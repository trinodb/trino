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
package io.trino.plugin.mysql;

import io.trino.testing.ResourcePresence;
import org.testcontainers.containers.MySQLContainer;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;

import static io.trino.testing.containers.TestContainers.startOrReuse;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

public class TestingMySqlServer
        implements AutoCloseable
{
    public static final String DEFAULT_IMAGE_8 = "mysql:8.0.36";
    public static final String DEFAULT_IMAGE = DEFAULT_IMAGE_8;
    public static final String LEGACY_IMAGE = "mysql:5.7.44"; // oldest available on RDS

    private final MySQLContainer<?> container;
    private final Closeable cleanup;

    public TestingMySqlServer()
    {
        this(false);
    }

    public TestingMySqlServer(ZoneId zoneId)
    {
        this(DEFAULT_IMAGE, false, zoneId);
    }

    public TestingMySqlServer(boolean globalTransactionEnable)
    {
        this(DEFAULT_IMAGE, globalTransactionEnable, UTC);
    }

    public TestingMySqlServer(String dockerImageName, boolean globalTransactionEnable)
    {
        this(dockerImageName, globalTransactionEnable, UTC);
    }

    public TestingMySqlServer(String dockerImageName, boolean globalTransactionEnable, ZoneId zoneId)
    {
        MySQLContainer<?> container = new MySQLContainer<>(dockerImageName);
        container = container.withDatabaseName("tpch");
        container.addEnv("TZ", zoneId.getId());
        if (globalTransactionEnable) {
            container = container.withCommand("--gtid-mode=ON", "--enforce-gtid-consistency=ON");
        }
        this.container = container;
        configureContainer(container);
        cleanup = startOrReuse(container);

        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), "root", container.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(format("GRANT ALL PRIVILEGES ON *.* TO '%s'", container.getUsername()));
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void configureContainer(MySQLContainer<?> container)
    {
        // MySQL configuration provided by default by testcontainers causes MySQL to produce poor estimates in CARDINALITY column of INFORMATION_SCHEMA.STATISTICS table.
        container.addParameter("TC_MY_CNF", null);
    }

    public void execute(String sql)
    {
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection createConnection()
            throws SQLException
    {
        return container.createConnection("");
    }

    public String getUsername()
    {
        return container.getUsername();
    }

    public String getPassword()
    {
        return container.getPassword();
    }

    public String getDatabaseName()
    {
        return container.getDatabaseName();
    }

    public String getJdbcUrl()
    {
        return format("jdbc:mysql://%s:%s?useSSL=false&allowPublicKeyRetrieval=true", container.getHost(), container.getMappedPort(MYSQL_PORT));
    }

    @Override
    public void close()
    {
        try {
            cleanup.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return container.getContainerId() != null;
    }
}
