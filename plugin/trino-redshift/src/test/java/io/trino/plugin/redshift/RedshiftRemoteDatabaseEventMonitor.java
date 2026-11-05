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
package io.trino.plugin.redshift;

import io.airlift.log.Logger;
import io.trino.plugin.jdbc.RemoteDatabaseEvent;
import io.trino.plugin.jdbc.RemoteDatabaseEvent.Status;
import io.trino.plugin.jdbc.RemoteLogTracingEvent;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.redshift.TestingRedshiftServer.JDBC_PASSWORD;
import static io.trino.plugin.redshift.TestingRedshiftServer.JDBC_URL;
import static io.trino.plugin.redshift.TestingRedshiftServer.JDBC_USER;
import static io.trino.plugin.redshift.TestingRedshiftServer.TEST_DATABASE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Periodically polls {@code SYS_QUERY_HISTORY} for recent queries of the given type issued by the test user
 * and feeds them to the registered {@link RemoteLogTracingEvent}s.
 */
public final class RedshiftRemoteDatabaseEventMonitor
        implements Runnable
{
    private static final Logger log = Logger.get(RedshiftRemoteDatabaseEventMonitor.class);
    private static final String LOG_CANCELLATION_EVENT = "cancelled on user's request";

    private final Jdbi jdbi = Jdbi.create(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
    private final Set<RemoteLogTracingEvent> tracingEvents = ConcurrentHashMap.newKeySet();
    private final String queryType;
    private ScheduledThreadPoolExecutor executor;

    /**
     * @param queryType value of the {@code query_type} column in {@code SYS_QUERY_HISTORY} to monitor, e.g. {@code SELECT} or {@code UNLOAD}
     */
    public RedshiftRemoteDatabaseEventMonitor(String queryType)
    {
        this.queryType = requireNonNull(queryType, "queryType is null");
    }

    public synchronized void startTracingDatabaseEvent(RemoteLogTracingEvent event)
    {
        if (tracingEvents.isEmpty()) {
            executor = new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("redshift-database-event-monitor"));
            executor.scheduleWithFixedDelay(this, 0, 5, SECONDS);
        }
        tracingEvents.add(event);
    }

    public synchronized void stopTracingDatabaseEvent(RemoteLogTracingEvent event)
    {
        tracingEvents.remove(event);
        if (tracingEvents.isEmpty()) {
            executor.shutdown();
        }
    }

    @Override
    public void run()
    {
        if (tracingEvents.isEmpty()) {
            return;
        }

        try {
            recentQueries()
                    .forEach(remoteDatabaseEvent -> tracingEvents.forEach(tracingEvent -> tracingEvent.accept(remoteDatabaseEvent)));
        }
        catch (Exception e) {
            // ignore exceptions to keep scheduled executions going
            log.warn(e, "Encountered error while gathering Redshift remote database events");
        }
    }

    private List<RemoteDatabaseEvent> recentQueries()
    {
        try (Handle handle = jdbi.open()) {
            return handle.createQuery(
                            """
                            SELECT query_text, status, error_message
                            FROM SYS_QUERY_HISTORY
                            WHERE database_name = :db_name
                            AND query_type = :query_type
                            AND user_id = current_user_id
                            AND start_time > GETDATE() - INTERVAL '15 minutes'
                            """)
                    .bind("db_name", TEST_DATABASE)
                    .bind("query_type", queryType)
                    .map((rs, _) -> new RemoteDatabaseEvent(
                            rs.getString("query_text"),
                            switch (requireNonNull(rs.getString("status"), "status is null").trim()) {
                                case "failed" -> Optional.ofNullable(rs.getString("error_message"))
                                        .flatMap(message -> message.contains(LOG_CANCELLATION_EVENT) ? Optional.of(Status.CANCELLED) : Optional.empty())
                                        .orElse(Status.DONE);
                                case "success" -> Status.DONE;
                                case "canceled" -> Status.CANCELLED;
                                default -> Status.RUNNING;
                            }))
                    .list();
        }
    }
}
