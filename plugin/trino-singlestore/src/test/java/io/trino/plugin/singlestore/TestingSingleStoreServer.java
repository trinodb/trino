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
package io.trino.plugin.singlestore;

import com.google.common.collect.ImmutableSet;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

public class TestingSingleStoreServer
        extends JdbcDatabaseContainer<TestingSingleStoreServer>
{
    public static final String DEFAULT_VERSION = "7.8";
    public static final String LATEST_TESTED_VERSION = "8.9";

    public static final String DEFAULT_TAG = "ghcr.io/singlestore-labs/singlestoredb-dev:0.2.51";

    public static final Integer SINGLESTORE_PORT = 3306;

    public TestingSingleStoreServer()
    {
        this(DEFAULT_VERSION);
    }

    public TestingSingleStoreServer(String version)
    {
        super(DockerImageName.parse(DEFAULT_TAG));
        addEnv("ROOT_PASSWORD", "memsql_root_password");
        addEnv("SINGLESTORE_VERSION", version);
        start();
    }

    @Override
    public Set<Integer> getLivenessCheckPortNumbers()
    {
        return ImmutableSet.of(getMappedPort(SINGLESTORE_PORT));
    }

    @Override
    protected void configure()
    {
        addExposedPort(SINGLESTORE_PORT);
        setStartupAttempts(3);
    }

    @Override
    public String getDriverClassName()
    {
        return "com.singlestore.jdbc.Driver";
    }

    @Override
    public String getUsername()
    {
        return "root";
    }

    @Override
    public String getPassword()
    {
        return "memsql_root_password";
    }

    @Override
    public String getJdbcUrl()
    {
        return "jdbc:singlestore://" + getHost() + ":" + getMappedPort(SINGLESTORE_PORT);
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
}
