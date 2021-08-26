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

import io.airlift.log.Logger;
import io.trino.testing.sql.SqlExecutor;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.Timeout;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.function.BiConsumer;

import static com.google.common.base.Throwables.getCausalChain;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.containers.TestContainers.startOrReuse;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class TestingSqlServer
        implements AutoCloseable
{
    private static final Logger log = Logger.get(TestingSqlServer.class);

    public static final BiConsumer<SqlExecutor, String> DEFAULT_DATABASE_SETUP = (executor, databaseName) -> {
        // Enable snapshot isolation by default to reduce flakiness on CI
        executor.execute(format("ALTER DATABASE %s SET ALLOW_SNAPSHOT_ISOLATION ON", databaseName));
        executor.execute(format("ALTER DATABASE %s SET READ_COMMITTED_SNAPSHOT ON", databaseName));
    };

    private static final RetryPolicy<InitializedState> CONTAINER_RETRY_POLICY = new RetryPolicy<InitializedState>()
            .withBackoff(1, 5, ChronoUnit.SECONDS)
            .withMaxRetries(5)
            .handleIf(throwable -> getCausalChain(throwable).stream()
                    .anyMatch(SQLException.class::isInstance))
            .onRetry(event -> log.warn(
                    "Query failed on attempt %s, will retry. Exception: %s",
                    event.getAttemptCount(),
                    event.getLastFailure().getMessage()));

    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("mcr.microsoft.com/mssql/server:2017-CU13");

    private final MSSQLServerContainer<?> container;
    private final String databaseName;
    private final Closeable cleanup;

    public TestingSqlServer()
    {
        this(DEFAULT_DATABASE_SETUP);
    }

    public TestingSqlServer(BiConsumer<SqlExecutor, String> databaseSetUp)
    {
        InitializedState initializedState = Failsafe.with(CONTAINER_RETRY_POLICY, Timeout.of(Duration.ofMinutes(5)))
                .get(() -> createContainer(databaseSetUp));

        container = initializedState.container;
        databaseName = initializedState.databaseName;
        cleanup = initializedState.cleanup;

        container.withUrlParam("database", databaseName);
    }

    private static InitializedState createContainer(BiConsumer<SqlExecutor, String> databaseSetUp)
    {
        String databaseName = "database_" + UUID.randomUUID().toString().replace("-", "");

        MSSQLServerContainer<?> container = new MSSQLServerContainer(DOCKER_IMAGE_NAME)
        {
            @Override
            public String getUsername()
            {
                // SQL Server is case sensitive (see usage of MSSQL_COLLATION env variable),
                // so user name has to be overridden to match actual case
                return super.getUsername().toLowerCase(ENGLISH);
            }
        };
        container.addEnv("ACCEPT_EULA", "yes");
        // enable case sensitive (see the CS below) collation for SQL identifiers
        container.addEnv("MSSQL_COLLATION", "Latin1_General_CS_AS");

        Closeable cleanup = startOrReuse(container);
        try {
            setUpDatabase(sqlExecutorForContainer(container), databaseName, databaseSetUp);
        }
        catch (Exception e) {
            closeAllSuppress(e, cleanup);
            throw e;
        }

        return new InitializedState(container, databaseName, cleanup);
    }

    private static void setUpDatabase(SqlExecutor executor, String databaseName, BiConsumer<SqlExecutor, String> databaseSetUp)
    {
        executor.execute("CREATE DATABASE " + databaseName);
        databaseSetUp.accept(executor, databaseName);
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public void execute(String sql)
    {
        sqlExecutorForContainer(container).execute(sql);
    }

    private static SqlExecutor sqlExecutorForContainer(MSSQLServerContainer<?> container)
    {
        requireNonNull(container, "container is null");
        return sql -> {
            try (Connection connection = container.createConnection("");
                    Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to execute statement: " + sql, e);
            }
        };
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

    public String getJdbcUrl()
    {
        return container.getJdbcUrl();
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

    private static class InitializedState
    {
        private final MSSQLServerContainer<?> container;
        private final String databaseName;
        private final Closeable cleanup;

        public InitializedState(MSSQLServerContainer<?> container, String databaseName, Closeable cleanup)
        {
            this.container = container;
            this.databaseName = databaseName;
            this.cleanup = cleanup;
        }
    }
}
