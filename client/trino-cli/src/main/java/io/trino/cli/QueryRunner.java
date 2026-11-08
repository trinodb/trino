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
package io.trino.cli;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class QueryRunner
        implements Closeable
{
    private final ExecutorService executor = newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("query-runner-%s").setDaemon(true).build());

    private final ConnectionFactory connectionFactory;
    private final Connection connection;
    private final boolean debug;
    private final int maxQueuedRows;
    private final int maxBufferedRows;
    private final Theme theme;

    private final Object metadataLock = new Object();
    private final AtomicReference<Connection> metadataConnection = new AtomicReference<>();
    private volatile boolean closed;

    public QueryRunner(Theme theme, ConnectionFactory connectionFactory, boolean debug, int maxQueuedRows, int maxBufferedRows)
            throws SQLException
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.connection = connectionFactory.openConnection();
        this.debug = debug;
        this.maxQueuedRows = maxQueuedRows;
        this.maxBufferedRows = maxBufferedRows;
        this.theme = requireNonNull(theme, "theme is null");
    }

    public Optional<String> getCatalog()
    {
        try {
            return Optional.ofNullable(connection.getCatalog());
        }
        catch (SQLException | RuntimeException | AbstractMethodError e) {
            return Optional.empty();
        }
    }

    public Optional<String> getSchema()
    {
        try {
            return Optional.ofNullable(connection.getSchema());
        }
        catch (SQLException | RuntimeException | AbstractMethodError e) {
            // drivers implementing JDBC versions older than 4.1 do not support schemas
            return Optional.empty();
        }
    }

    public boolean isDebug()
    {
        return debug;
    }

    public Theme theme()
    {
        return theme;
    }

    public Query startQuery(String query)
            throws SQLException
    {
        Statement statement = connection.createStatement();
        return new Query(statement, query, executor, debug, maxQueuedRows, maxBufferedRows, theme);
    }

    /**
     * Runs the action on a connection that is separate from the one used to execute queries,
     * so it can be used concurrently and outside any open transaction.
     */
    public <T> T withMetadataConnection(MetadataAction<T> action)
            throws SQLException
    {
        // most drivers do not support using a connection from multiple threads
        synchronized (metadataLock) {
            Connection connection = metadataConnection.get();
            if (connection == null) {
                connection = connectionFactory.openConnection();
                metadataConnection.set(connection);
                if (closed) {
                    closeQuietly(metadataConnection.getAndSet(null));
                    throw new SQLException("Query runner is closed");
                }
            }
            return action.apply(connection);
        }
    }

    @Override
    public void close()
    {
        closed = true;
        executor.shutdownNow();
        try (connectionFactory) {
            // closing without the lock interrupts a metadata query that is still running
            closeQuietly(metadataConnection.getAndSet(null));
            closeQuietly(connection);
        }
    }

    private static void closeQuietly(Connection connection)
    {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        }
        catch (SQLException | RuntimeException ignored) {
        }
    }

    public interface MetadataAction<T>
    {
        T apply(Connection connection)
                throws SQLException;
    }
}
