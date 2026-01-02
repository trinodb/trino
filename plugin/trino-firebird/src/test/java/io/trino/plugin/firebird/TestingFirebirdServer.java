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
package io.trino.plugin.firebird;

import io.airlift.log.Logger;
import io.trino.testing.ResourcePresence;
import io.trino.testing.containers.PrintingLogConsumer;
import io.trino.testing.sql.SqlExecutor;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TestingFirebirdServer
        implements AutoCloseable
{
    public static final String TEST_SCHEMA = "tpch";

    private static final Logger log = Logger.get(TestingFirebirdServer.class);

    private static final String DEFAULT_VERSION = "5.0.2";
    private static final String DEFAULT_IMAGE = "firebirdsql/firebird:";
    private static final String DEFAULT_PATH = "/var/lib/firebird/data";
    private static final String DEFAULT_DATABASE = "firebird.fdb";
    private static final String DEFAULT_USER = "SYSDBA";
    private static final String DEFAULT_ROOT_PASSWORD = "masterkey";
    private static final String FIREBIRD_URL = "jdbc:firebird:%s/%s:%s/%s";
    private static final int FIREBIRD_PORT = 3050;

    private static class FirebirdContainer
            extends GenericContainer<FirebirdContainer>
    {
        private FirebirdContainer(String image)
        {
            super(DockerImageName.parse(image));
        }
    }

    private final FirebirdContainer container;

    public TestingFirebirdServer()
    {
        this(DEFAULT_VERSION);
    }

    public TestingFirebirdServer(String tag)
    {
        container = new FirebirdContainer(DEFAULT_IMAGE + tag);
        container.withEnv("FIREBIRD_DATABASE", DEFAULT_DATABASE);
        container.withEnv("FIREBIRD_ROOT_PASSWORD", DEFAULT_ROOT_PASSWORD);
        container.withExposedPorts(FIREBIRD_PORT);
        container.start();

        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword())) {
            log.info("Connected to Firebird with driver: %s - version: %s", connection.getMetaData().getDriverName(), connection.getMetaData().getDriverVersion());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        container.followOutput(new PrintingLogConsumer("Firebird"));
        log.info("%s listening on url: %s", TestingFirebirdServer.class.getName(), getJdbcUrl());
    }

    public SqlExecutor getSqlExecutor()
    {
        return new SqlExecutor()
        {
            @Override
            public void execute(String sql)
            {
                TestingFirebirdServer.this.execute(sql);
            }

            @Override
            public boolean supportsMultiRowInsert()
            {
                return false;
            }
        };
    }

    public void execute(String sql)
    {
        execute(sql, getUsername(), getPassword());
    }

    public void execute(String sql, String user, String password)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), user, password);
                Statement statement = connection.createStatement()) {
            log.info("TestingFirebirdServer.execute() SQL query: " + sql);
            statement.execute(sql);
        }
        catch (SQLException e) {
            log.error(e, "SQL query: " + sql);
            throw new RuntimeException(e);
        }
    }

    public String getUsername()
    {
        return DEFAULT_USER;
    }

    public String getPassword()
    {
        return DEFAULT_ROOT_PASSWORD;
    }

    public String getJdbcUrl()
    {
        return String.format(FIREBIRD_URL, container.getHost(), container.getMappedPort(FIREBIRD_PORT), DEFAULT_PATH, DEFAULT_DATABASE);
    }

    public Connection getConnection()
            throws SQLException
    {
        return DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
    }

    @Override
    public void close()
    {
        container.close();
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return container.isRunning();
    }
}
