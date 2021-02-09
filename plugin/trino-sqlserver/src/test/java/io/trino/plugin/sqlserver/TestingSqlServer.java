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
package io.trino.plugin.sqlserver;

import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static io.trino.testing.containers.TestContainers.startOrReuse;
import static java.lang.String.format;

public final class TestingSqlServer
        implements AutoCloseable
{
    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("microsoft/mssql-server-linux:2017-CU13")
            .asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server:2017-CU12");
    private final MSSQLServerContainer<?> container;
    private final boolean snapshotIsolationEnabled;
    private final String databaseName;
    private Closeable cleanup = () -> {};

    public TestingSqlServer()
    {
        this(true);
    }

    public TestingSqlServer(boolean snapshotIsolationEnabled)
    {
        container = new MSSQLServerContainer<>(DOCKER_IMAGE_NAME);
        container.addEnv("ACCEPT_EULA", "yes");
        this.snapshotIsolationEnabled = snapshotIsolationEnabled;
        this.databaseName = "database_" + UUID.randomUUID().toString().replace("-", "");
    }

    public void execute(String sql)
    {
        try (Connection connection = container.createConnection("");
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    public Connection createConnection()
            throws SQLException
    {
        return container.createConnection("");
    }

    public void start()
    {
        cleanup = startOrReuse(container);
        setUpDatabase();
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
        return container.getJdbcUrl();
    }

    private void setUpDatabase()
    {
        execute("CREATE DATABASE " + databaseName);

        if (snapshotIsolationEnabled) {
            execute(format("ALTER DATABASE %s SET READ_COMMITTED_SNAPSHOT ON", databaseName));
            execute(format("ALTER DATABASE %s SET ALLOW_SNAPSHOT_ISOLATION ON", databaseName));
        }

        container.withUrlParam("database", this.databaseName);
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
}
