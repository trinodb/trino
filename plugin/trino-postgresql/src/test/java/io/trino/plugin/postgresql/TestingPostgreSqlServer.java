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
package io.trino.plugin.postgresql;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.RemoteDatabaseEvent;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.CANCELLED;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.RUNNING;
import static java.lang.String.format;
import static java.util.function.Predicate.not;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public class TestingPostgreSqlServer
        implements Closeable
{
    private static final String USER = "test";
    private static final String PASSWORD = "test";
    private static final String DATABASE = "tpch";

    private static final String LOG_PREFIX_REGEXP = "^([-:0-9. ]+UTC \\[[0-9]+\\] )";
    private static final String LOG_RUNNING_STATEMENT_PREFIX = "LOG:  execute <unnamed>: ";
    private static final String LOG_CANCELLATION_EVENT = "ERROR:  canceling statement due to user request";
    private static final String LOG_CANCELLED_STATEMENT_PREFIX = "STATEMENT:  ";

    private final PostgreSQLContainer<?> dockerContainer;

    public TestingPostgreSqlServer()
    {
        // Use the oldest supported PostgreSQL version
        dockerContainer = new PostgreSQLContainer<>("postgres:10.20")
                .withDatabaseName(DATABASE)
                .withUsername(USER)
                .withPassword(PASSWORD)
                .withCommand("postgres", "-c", "log_destination=stderr", "-c", "log_statement=all");
        dockerContainer.start();

        execute("CREATE SCHEMA tpch");
    }

    public void execute(String sql)
    {
        execute(getJdbcUrl(), getProperties(), sql);
    }

    private static void execute(String url, Properties properties, String sql)
    {
        try (Connection connection = DriverManager.getConnection(url, properties);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected List<RemoteDatabaseEvent> getRemoteDatabaseEvents()
    {
        List<String> logs = getLogs();
        Iterator<String> logsIterator = logs.iterator();
        ImmutableList.Builder<RemoteDatabaseEvent> events = ImmutableList.builder();
        while (logsIterator.hasNext()) {
            String logLine = logsIterator.next().replaceAll(LOG_PREFIX_REGEXP, "");
            if (logLine.startsWith(LOG_RUNNING_STATEMENT_PREFIX)) {
                events.add(new RemoteDatabaseEvent(logLine.substring(LOG_RUNNING_STATEMENT_PREFIX.length()), RUNNING));
            }
            if (logLine.equals(LOG_CANCELLATION_EVENT)) {
                // next line must be present
                String cancelledStatementLogLine = logsIterator.next().replaceAll(LOG_PREFIX_REGEXP, "");
                if (cancelledStatementLogLine.startsWith(LOG_CANCELLED_STATEMENT_PREFIX)) {
                    events.add(new RemoteDatabaseEvent(cancelledStatementLogLine.substring(LOG_CANCELLED_STATEMENT_PREFIX.length()), CANCELLED));
                }
            }
            // ignore unsupported log lines
        }
        return events.build();
    }

    private List<String> getLogs()
    {
        return Stream.of(dockerContainer.getLogs().split("\n"))
                .filter(not(String::isBlank))
                .collect(toImmutableList());
    }

    public String getUser()
    {
        return USER;
    }

    public String getPassword()
    {
        return PASSWORD;
    }

    public Properties getProperties()
    {
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        properties.setProperty("password", PASSWORD);
        properties.setProperty("currentSchema", "tpch,public");
        return properties;
    }

    public String getJdbcUrl()
    {
        return format("jdbc:postgresql://%s:%s/%s", dockerContainer.getContainerIpAddress(), dockerContainer.getMappedPort(POSTGRESQL_PORT), DATABASE);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
