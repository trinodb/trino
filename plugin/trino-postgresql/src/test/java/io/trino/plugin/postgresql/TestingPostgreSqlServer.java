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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.plugin.jdbc.RemoteDatabaseEvent;
import io.trino.plugin.jdbc.RemoteLogTracingEvent;
import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.CANCELLED;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.RUNNING;
import static io.trino.testing.containers.TestContainers.exposeFixedPorts;
import static io.trino.testing.containers.TestContainers.startOrReuse;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Predicate.not;
import static org.testcontainers.postgresql.PostgreSQLContainer.POSTGRESQL_PORT;

public class TestingPostgreSqlServer
        implements AutoCloseable
{
    // the oldest supported PostgreSQL version
    public static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("postgres:12");
    // first PostgreSQL version that expanded PostgreSQL numeric type
    public static final DockerImageName IMAGE_15_NAME = DockerImageName.parse("postgres:15");
    // newest tested PostgreSQL version
    public static final DockerImageName LATEST_IMAGE_NAME = DockerImageName.parse("postgres:18");

    private static final String USER = "test";
    private static final String PASSWORD = "test";
    private static final String DATABASE = "tpch";

    private static final Pattern LOG_LINE_PATTERN = Pattern.compile("^[-:0-9. ]+UTC \\[([0-9]+)\\] (.*)$");
    private static final String LOG_RUNNING_STATEMENT_PREFIX = "LOG:  execute <unnamed>";
    private static final String LOG_CANCELLATION_EVENT = "ERROR:  canceling statement due to user request";

    private static final Pattern SQL_QUERY_FIND_PATTERN = Pattern.compile("^(: |/C_\\d: )(.*)"); // In PgSQL cursor queries and non-cursor queries are logged differently
    private static final String LOG_CANCELLED_STATEMENT_PREFIX = "STATEMENT:  ";

    private static final Duration MARKER_TIMEOUT = Duration.ofSeconds(30);

    private final PostgreSQLContainer dockerContainer;
    private final Set<Consumer<RemoteDatabaseEvent>> eventListeners = ConcurrentHashMap.newKeySet();

    private final Closeable cleanup;

    public TestingPostgreSqlServer()
    {
        this(DEFAULT_IMAGE_NAME);
    }

    public TestingPostgreSqlServer(DockerImageName dockerImageName)
    {
        this(dockerImageName, false);
    }

    public TestingPostgreSqlServer(DockerImageName dockerImageName, boolean shouldExposeFixedPorts)
    {
        dockerContainer = new PostgreSQLContainer(dockerImageName)
                .withStartupAttempts(3)
                .withDatabaseName(DATABASE)
                .withUsername(USER)
                .withPassword(PASSWORD)
                .withLogConsumer(new RemoteDatabaseEventLogConsumer())
                // PostgreSQL can open the container's stderr as a log file only when the container runs as the postgres user
                .withCreateContainerCmdModifier(cmd -> cmd.withUser("postgres"))
                // The logging collector is the only process that writes to stderr so long log lines from concurrent backends do not interleave
                .withCommand("postgres", "-c", "log_destination=stderr", "-c", "log_statement=all", "-c", "logging_collector=on", "-c", "log_directory=/dev", "-c", "log_filename=stderr", "-c", "log_rotation_age=0", "-c", "log_rotation_size=0");
        if (shouldExposeFixedPorts) {
            exposeFixedPorts(dockerContainer);
        }
        cleanup = startOrReuse(dockerContainer);

        execute("CREATE SCHEMA IF NOT EXISTS tpch");
    }

    private class RemoteDatabaseEventLogConsumer
            implements Consumer<OutputFrame>
    {
        private final PostgreSqlLogParser logParser = new PostgreSqlLogParser();

        @Override
        public void accept(OutputFrame outputFrame)
        {
            logParser.parse(outputFrame.getUtf8StringWithoutLineEnding())
                    .ifPresent(remoteDatabaseEvent -> eventListeners.forEach(listener -> listener.accept(remoteDatabaseEvent)));
        }
    }

    static class PostgreSqlLogParser
    {
        private final Set<String> cancelledBackendProcessIds = new HashSet<>();

        public Optional<RemoteDatabaseEvent> parse(String logEntry)
        {
            Matcher logLineMatcher = LOG_LINE_PATTERN.matcher(logEntry);
            if (!logLineMatcher.matches()) {
                return Optional.empty();
            }

            String backendProcessId = logLineMatcher.group(1);
            String logLine = logLineMatcher.group(2);

            if (logLine.equals(LOG_CANCELLATION_EVENT)) {
                cancelledBackendProcessIds.add(backendProcessId);
                return Optional.empty();
            }

            if (logLine.startsWith(LOG_CANCELLED_STATEMENT_PREFIX) && cancelledBackendProcessIds.remove(backendProcessId)) {
                return Optional.of(new RemoteDatabaseEvent(logLine.substring(LOG_CANCELLED_STATEMENT_PREFIX.length()), CANCELLED));
            }

            if (logLine.startsWith(LOG_RUNNING_STATEMENT_PREFIX)) {
                Matcher matcher = SQL_QUERY_FIND_PATTERN.matcher(logLine.substring(LOG_RUNNING_STATEMENT_PREFIX.length()));
                if (matcher.find()) {
                    String sqlStatement = matcher.group(2);
                    return Optional.of(new RemoteDatabaseEvent(sqlStatement, RUNNING));
                }
            }

            return Optional.empty();
        }
    }

    protected void startTracingDatabaseEvent(RemoteLogTracingEvent event)
    {
        eventListeners.add(event);
    }

    protected void stopTracingDatabaseEvent(RemoteLogTracingEvent event)
    {
        eventListeners.remove(event);
    }

    public void execute(@Language("SQL") String sql)
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

    DatabaseEventsRecorder recordEventsForOperations(Runnable operation)
    {
        DatabaseEventsRecorder events = DatabaseEventsRecorder.startRecording(this);
        try {
            operation.run();
        }
        catch (RuntimeException | Error e) {
            events.stopListening();
            throw e;
        }
        return events;
    }

    protected List<RemoteDatabaseEvent> getRemoteDatabaseEvents()
    {
        PostgreSqlLogParser logParser = new PostgreSqlLogParser();
        return getLogs().stream()
                .map(logParser::parse)
                .flatMap(Optional::stream)
                .collect(toImmutableList());
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
        return format("jdbc:postgresql://%s:%s/%s", dockerContainer.getHost(), dockerContainer.getMappedPort(POSTGRESQL_PORT), DATABASE);
    }

    @Override
    public void close()
    {
        try {
            cleanup.close();
        }
        catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    public static class DatabaseEventsRecorder
            implements Consumer<RemoteDatabaseEvent>
    {
        private final TestingPostgreSqlServer server;
        private final RemoteDatabaseEvent startMarker = newMarker();
        private final RemoteDatabaseEvent stopMarker = newMarker();
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch stopped = new CountDownLatch(1);
        @GuardedBy("this")
        private final List<String> queries = new ArrayList<>();

        private DatabaseEventsRecorder(TestingPostgreSqlServer server)
        {
            this.server = requireNonNull(server, "server is null");
        }

        static DatabaseEventsRecorder startRecording(TestingPostgreSqlServer server)
        {
            DatabaseEventsRecorder recorder = new DatabaseEventsRecorder(server);
            server.eventListeners.add(recorder);
            try {
                recorder.logMarker(recorder.startMarker, recorder.started);
            }
            catch (RuntimeException | Error e) {
                recorder.stopListening();
                throw e;
            }
            return recorder;
        }

        public DatabaseEventsRecorder stopEventsRecording()
        {
            try {
                logMarker(stopMarker, stopped);
            }
            finally {
                stopListening();
            }
            return this;
        }

        public Stream<String> streamQueriesContaining(String queryPart, String... alternativeQueryParts)
        {
            List<String> queryParts = ImmutableList.<String>builder()
                    .add(queryPart)
                    .addAll(ImmutableList.copyOf(alternativeQueryParts))
                    .build();
            return recordedQueries().stream()
                    .filter(query -> queryParts.stream().anyMatch(query::contains));
        }

        // The log consumer delivers events in log order. Statements are logged asynchronously, so a marker
        // statement executed after the recorded operation is the only reliable signal that all the operation's
        // statements have been logged.
        @Override
        public synchronized void accept(RemoteDatabaseEvent event)
        {
            if (started.getCount() > 0) {
                if (event.equals(startMarker)) {
                    started.countDown();
                }
                return;
            }
            if (stopped.getCount() == 0) {
                return;
            }
            if (event.equals(stopMarker)) {
                stopped.countDown();
                return;
            }
            queries.add(event.getQuery());
        }

        private synchronized List<String> recordedQueries()
        {
            return ImmutableList.copyOf(queries);
        }

        private void logMarker(RemoteDatabaseEvent marker, CountDownLatch logged)
        {
            server.execute(marker.getQuery());
            try {
                if (!logged.await(MARKER_TIMEOUT.toMillis(), MILLISECONDS)) {
                    throw new AssertionError("Marker statement was not logged within %s: %s".formatted(MARKER_TIMEOUT, marker.getQuery()));
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        private void stopListening()
        {
            server.eventListeners.remove(this);
        }

        private static RemoteDatabaseEvent newMarker()
        {
            return new RemoteDatabaseEvent("SELECT 'events recorder marker %s'".formatted(randomUUID()), RUNNING);
        }
    }
}
