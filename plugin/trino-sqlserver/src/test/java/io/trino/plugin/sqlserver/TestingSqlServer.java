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
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static com.google.common.base.Throwables.getCausalChain;
import static io.trino.testing.containers.TestContainers.startOrReuse;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public final class TestingSqlServer
        implements AutoCloseable
{
    private static final Logger log = Logger.get(TestingSqlServer.class);

    private static final RetryPolicy<?> QUERY_RETRY_POLICY = new RetryPolicy<>()
            .withBackoff(1, 5, ChronoUnit.SECONDS)
            .withMaxRetries(5)
            .handleIf(throwable -> getCausalChain(throwable).stream()
                            .filter(SQLException.class::isInstance)
                            .anyMatch(exception -> exception.getMessage().contains("Rerun the transaction.")))
            .onRetry(event -> log.warn(
                    "Query failed on attempt %s, will retry. Exception: %s",
                    event.getAttemptCount(),
                    event.getLastFailure().getMessage()));

    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("microsoft/mssql-server-linux:2017-CU13")
            .asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server:2017-CU12");
    private final MSSQLServerContainer<?> container;
    private final String databaseName;
    private Closeable cleanup = () -> {};

    public TestingSqlServer()
    {
        container = new MSSQLServerContainer(DOCKER_IMAGE_NAME) {
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
        this.databaseName = "database_" + UUID.randomUUID().toString().replace("-", "");
    }

    public String getDatabaseName()
    {
        return databaseName;
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

        // Enable snapshot isolation by default to reduce flakiness on CI
        Failsafe.with(QUERY_RETRY_POLICY)
                .run(() -> execute(format("ALTER DATABASE %s SET ALLOW_SNAPSHOT_ISOLATION ON", databaseName)));
        Failsafe.with(QUERY_RETRY_POLICY)
                .run(() -> execute(format("ALTER DATABASE %s SET READ_COMMITTED_SNAPSHOT ON", databaseName)));

        container.withUrlParam("database", this.databaseName);
    }

    public AutoCloseable withSchema(String schemaName)
    {
        execute(format("CREATE SCHEMA %s ", schemaName));
        return () -> execute("DROP SCHEMA " + schemaName);
    }

    public AutoCloseable withTable(String tableName, String tableDefinition)
    {
        execute(format("CREATE TABLE %s %s", tableName, tableDefinition));
        return () -> execute(format("DROP TABLE %s", tableName));
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
