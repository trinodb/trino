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
package io.trino.plugin.vertica;

import com.google.common.collect.ImmutableSet;
import io.trino.testing.sql.SqlExecutor;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import static java.lang.String.format;

public class TestingVerticaServer
        extends JdbcDatabaseContainer<TestingVerticaServer>
{
    public static final Integer PORT = 5433;
    private static final String DATABASE = "test_db";
    public static final String SCHEMA = "tpch";
    private static final String USER = "test_user";
    private static final String PASSWORD = "test_password";

    public TestingVerticaServer()
    {
        super(DockerImageName.parse("datagrip/vertica:9.1.1"));
        start();
        execute(format("GRANT ALL ON DATABASE %s TO %s", DATABASE, USER), "dbadmin", null);
    }

    @Override
    public Set<Integer> getLivenessCheckPortNumbers()
    {
        return ImmutableSet.of(getMappedPort(PORT));
    }

    @Override
    protected void configure()
    {
        addExposedPort(PORT);
        addEnv("VERTICA_DB", DATABASE);
        addEnv("VERTICA_SCHEMA", SCHEMA);
        addEnv("VERTICA_USER", USER);
        addEnv("VERTICA_PASSWORD", PASSWORD);
        setStartupAttempts(3);
    }

    @Override
    public String getDriverClassName()
    {
        return "com.vertica.jdbc.Driver";
    }

    @Override
    public String getUsername()
    {
        return USER;
    }

    @Override
    public String getPassword()
    {
        return PASSWORD;
    }

    @Override
    public String getJdbcUrl()
    {
        return format("jdbc:vertica://%s:%s/%s", getHost(), getMappedPort(PORT), DATABASE);
    }

    @Override
    public String getTestQueryString()
    {
        return "SELECT 1";
    }

    public void execute(String sql)
    {
        execute(sql, getUsername(), getPassword());
    }

    public void execute(String sql, String user, String password)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), user, password);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public SqlExecutor getSqlExecutor()
    {
        return new SqlExecutor()
        {
            @Override
            public void execute(String sql)
            {
                TestingVerticaServer.this.execute(sql);
            }

            @Override
            public boolean supportsMultiRowInsert()
            {
                return false;
            }
        };
    }
}
