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
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.testing.ResourcePresence;
import io.trino.testing.sql.SqlExecutor;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Set;

import static io.trino.testing.containers.TestContainers.startOrReuse;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @see <a href="https://www.microfocus.com/productlifecycle/?term=Vertica">Vertica product lifecycle</a>
 */
public class TestingVerticaServer
        extends JdbcDatabaseContainer<TestingVerticaServer>
{
    public static final String LATEST_IMAGE = "vertica/vertica-ce:23.4.0-0";
    public static final String DEFAULT_IMAGE = "datagrip/vertica:9.1.1";

    public static final Integer PORT = 5433;
    public static final String SCHEMA = "tpch";

    public static final String DATABASE = "tpch";
    private static final String USER = "test_user";
    private static final String PASSWORD = "test_password";

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private final String database;
    private final String user;
    private final String password;

    public TestingVerticaServer()
    {
        this(DEFAULT_IMAGE, DATABASE, USER, PASSWORD);
    }

    public TestingVerticaServer(String dockerImageName)
    {
        this(dockerImageName, DATABASE, USER, PASSWORD);
    }

    public TestingVerticaServer(String dockerImageName, String database, String user, String password)
    {
        super(DockerImageName.parse(dockerImageName));
        this.database = requireNonNull(database, "database is null");
        this.user = requireNonNull(user, "user is null");
        this.password = requireNonNull(password, "password is null");
        withStartupAttempts(3);
        withStartupTimeoutSeconds((int) Duration.ofMinutes(10).toSeconds());
        closer.register(startOrReuse(this));
        execute(format("GRANT ALL ON DATABASE %s TO %s", database, user), "dbadmin", null);
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
        if (getDockerImageName().contains("datagrip/vertica:")) {
            addEnv("VERTICA_DB", database);
            addEnv("VERTICA_SCHEMA", SCHEMA);
            addEnv("VERTICA_USER", user);
            addEnv("VERTICA_PASSWORD", password);
        }
        else if (getDockerImageName().contains("vertica/vertica-ce:")) {
            addEnv("VERTICA_DB_NAME", database);
            addEnv("APP_DB_USER", user);
            addEnv("APP_DB_PASSWORD", password);
            withCopyFileToContainer(MountableFile.forClasspathResource("vmart_define_schema.sql"), "/opt/vertica/examples/VMart_Schema");
            withCopyFileToContainer(MountableFile.forClasspathResource("vmart_load_data.sql"), "/opt/vertica/examples/VMart_Schema");
        }
        setStartupAttempts(3);
    }

    @Override
    public String getDriverClassName()
    {
        return "io.trino.plugin.vertica.VerticaDriver";
    }

    @Override
    public String getUsername()
    {
        return user;
    }

    @Override
    public String getPassword()
    {
        return password;
    }

    @Override
    public String getJdbcUrl()
    {
        return format("jdbc:vertica://%s:%s/%s", getHost(), getMappedPort(PORT), database);
    }

    @Override
    public String getTestQueryString()
    {
        return "SELECT 1";
    }

    @Override
    public void close()
    {
        try {
            closer.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    @ResourcePresence
    @Override
    public boolean isRunning()
    {
        return getContainerId() != null;
    }
}
