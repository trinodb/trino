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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.plugin.jdbc.RemoteDatabaseEvent;
import net.jodah.failsafe.function.CheckedRunnable;
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

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.CANCELLED;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.RUNNING;
import static java.lang.String.format;
import static java.util.function.Predicate.not;
import static org.testcontainers.containers.BindMode.READ_ONLY;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public class TestingPostgreSqlServer
        implements Closeable
{
    private static final String USER = "test";
    private static final String PASSWORD = "test";
    private static final String DATABASE = "tpch";

    private final PostgreSQLContainer<?> dockerContainer;

    public TestingPostgreSqlServer()
    {
        // Use the oldest supported PostgreSQL version
        dockerContainer = new PostgreSQLContainer<>("postgres:9.5")
                .withDatabaseName(DATABASE)
                .withUsername(USER)
                .withPassword(PASSWORD)
                .withClasspathResourceMapping("postgresql.conf", "/etc/postgresql/postgresql.conf", READ_ONLY)
                .withCommand("postgres", "-c", "config_file=/etc/postgresql/postgresql.conf");
        dockerContainer.start();

        execute("CREATE VIEW sleep_for_a_minute AS SELECT 1 FROM pg_sleep(60)");
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

    protected List<RemoteDatabaseEvent> captureRemoteEventsDuring(CheckedRunnable runnable)
            throws Throwable
    {
        ImmutableSet<String> before = getLogs();
        runnable.run();
        ImmutableSet<String> after = getLogs();
        ImmutableList.Builder<RemoteDatabaseEvent> events = ImmutableList.builder();
        Sets.SetView<String> logs = Sets.difference(after, before);
        Iterator<String> logsIterator = logs.iterator();
        while (logsIterator.hasNext()) {
            String logLine = logsIterator.next();
            if (logLine.startsWith("LOG:  execute <unnamed>: ")) {
                events.add(new RemoteDatabaseEvent(logLine.substring(25), RUNNING));
            }
            if (logLine.equals("ERROR:  canceling statement due to user request")) {
                String cancelledStatementLogLine = logsIterator.next();
                if (cancelledStatementLogLine.startsWith("STATEMENT: ")) {
                    events.add(new RemoteDatabaseEvent(cancelledStatementLogLine.substring(12), CANCELLED));
                }
            }
            // ignore unsupported log lines
        }
        return events.build();
    }

    private ImmutableSet<String> getLogs()
    {
        return Stream.of(dockerContainer.getLogs().split("\n"))
                .filter(not(String::isBlank))
                .collect(toImmutableSet());
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
