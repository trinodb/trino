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
package io.trino.plugin.exasol;

import com.exasol.jdbc.EXAResultSet;
import io.trino.plugin.jdbc.BaseJdbcConnectorTableHandle;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcPageSource;
import io.trino.plugin.jdbc.JdbcProcedureHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public class ExasolParallelPageSource
        implements ConnectorPageSource
{
    private static final Logger log = LoggerFactory.getLogger(ExasolParallelPageSource.class);
    private final List<ExasolResultHandlePageSource> subConnectionPageSources;
    private final ExecutorService executor;
    private final BufferedSourcePageQueue pageQueue;
    private final Connection mainConnection;
    private final PreparedStatement mainStatement;
    private final ResultSet mainResultSet;
    private volatile List<? extends Future<?>> readerTasks = List.of();
    private volatile boolean closed;

    private ExasolParallelPageSource(List<ExasolResultHandlePageSource> subConnectionPageSources, ExecutorService executor, Connection mainConnection, PreparedStatement mainStatement, ResultSet mainResultSet)
    {
        this.subConnectionPageSources = subConnectionPageSources;
        this.executor = executor;
        this.pageQueue = new BufferedSourcePageQueue(subConnectionPageSources.size());
        this.mainConnection = mainConnection;
        this.mainStatement = mainStatement;
        this.mainResultSet = mainResultSet;
    }

    public static ConnectorPageSource create(JdbcClient jdbcClient, ExecutorService executor, ParallelConnectionFactory parallelConnectionFactory, ConnectorSession session, JdbcSplit jdbcSplit, BaseJdbcConnectorTableHandle table, List<JdbcColumnHandle> columnHandles)
    {
        int parallelImportWorkerCount = ExasolSessionProperties.getParallelImportWorkerCount(session);
        if (parallelImportWorkerCount == 0) {
            return new JdbcPageSource(jdbcClient, executor, session, jdbcSplit, table, columnHandles);
        }
        return createParallelConnectionPageSource(jdbcClient, executor, parallelConnectionFactory, session, jdbcSplit, table, columnHandles);
    }

    private static ExasolParallelPageSource createParallelConnectionPageSource(JdbcClient jdbcClient, ExecutorService executor, ParallelConnectionFactory parallelConnectionFactory, ConnectorSession session, JdbcSplit jdbcSplit, BaseJdbcConnectorTableHandle table, List<JdbcColumnHandle> columnHandles)
    {
        Connection mainConnection = null;
        PreparedStatement mainStatement = null;
        ResultSet mainResultSet = null;
        List<Connection> subConnections = List.of();
        try {
            mainConnection = createExaConnection(jdbcClient, session, table);
            subConnections = parallelConnectionFactory.createConnections(session, mainConnection);
            mainStatement = prepareStatement(jdbcClient, session, mainConnection, table, jdbcSplit, columnHandles);
            mainResultSet = mainStatement.executeQuery();
            int resultSetHandle = mainResultSet.unwrap(EXAResultSet.class).GetHandle();
            List<ExasolResultHandlePageSource> subConnectionPageSources = subConnections.stream()
                    .map(subConnection -> new ExasolResultHandlePageSource(jdbcClient, executor, session, subConnection, resultSetHandle, columnHandles))
                    .toList();
            ExasolParallelPageSource pageSource = new ExasolParallelPageSource(subConnectionPageSources, executor, mainConnection, mainStatement, mainResultSet);
            pageSource.startReading();
            return pageSource;
        }
        catch (SQLException | RuntimeException e) {
            closeResources(mainConnection, mainStatement, mainResultSet, subConnections);
            log.error("Error creating parallel connection page source: {}", e.getMessage(), e);
            throw new RuntimeException("Error creating parallel connection page source: " + e.getMessage(), e);
        }
    }

    private static void closeResources(Connection mainConnection, PreparedStatement mainStatement, ResultSet mainResultSet, List<Connection> subConnections)
    {
        try (Connection _ = mainConnection;
                Statement statement = mainStatement;
                ResultSet _ = mainResultSet) {
            if (statement != null) {
                try {
                    statement.cancel();
                }
                catch (SQLException e) {
                    log.debug("Cancel failed during cleanup", e);
                }
            }
        }
        catch (SQLException | RuntimeException e) {
            log.debug("Closing failed during cleanup", e);
        }

        for (Connection subConnection : subConnections) {
            try {
                subConnection.close();
            }
            catch (SQLException e) {
                log.debug("Failed to close subconnection during cleanup", e);
            }
        }
    }

    private void startReading()
    {
        readerTasks = IntStream.range(0, subConnectionPageSources.size())
                .mapToObj(producerId -> executor.submit(() -> consumePageSource(producerId, subConnectionPageSources.get(producerId))))
                .toList();
    }

    private void consumePageSource(int producerId, ExasolResultHandlePageSource source)
    {
        try {
            log.info("Reading all pages from {}...", source);
            while (!source.isFinished()) {
                SourcePage page = source.getNextSourcePage();
                if (page != null) {
                    pageQueue.add(page);
                    log.debug("Found page {} with {} rows from source {}, queue size: {}", page, page.getPositionCount(), source, pageQueue.size());
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Reading interrupted for source {}", source, e);
        }
        catch (Throwable e) {
            log.error("Error reading from source {}: {}", source, e.getMessage(), e);
            pageQueue.fail(producerId, e);
        }
        finally {
            pageQueue.finish(producerId);
        }
    }

    private static PreparedStatement prepareStatement(JdbcClient jdbcClient, ConnectorSession session, Connection mainConnection, BaseJdbcConnectorTableHandle table, JdbcSplit jdbcSplit, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        if (table instanceof JdbcProcedureHandle procedureHandle) {
            return jdbcClient.buildProcedure(session, mainConnection, jdbcSplit, procedureHandle);
        }
        else {
            return jdbcClient.buildSql(session, mainConnection, jdbcSplit, (JdbcTableHandle) table, columnHandles);
        }
    }

    private static Connection createExaConnection(JdbcClient jdbcClient, ConnectorSession session, BaseJdbcConnectorTableHandle table)
            throws SQLException
    {
        if (table instanceof JdbcProcedureHandle procedureHandle) {
            return jdbcClient.getConnection(session, null, procedureHandle);
        }
        else {
            return jdbcClient.getConnection(session, null, (JdbcTableHandle) table);
        }
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        Throwable throwable = pageQueue.getFailure();
        if (throwable != null) {
            throw new RuntimeException("Sub connection failure: " + throwable.getMessage(), throwable);
        }
        SourcePage page = pageQueue.poll();
        log.debug("Got next source page {}, remaining queue size: {}", page, pageQueue.size());
        return page;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return pageQueue.isBlocked();
    }

    @Override
    public long getCompletedBytes()
    {
        return subConnectionPageSources.stream().mapToLong(ConnectorPageSource::getCompletedBytes).sum();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return subConnectionPageSources.stream()
                .map(ConnectorPageSource::getCompletedPositions)
                .filter(OptionalLong::isPresent)
                .mapToLong(OptionalLong::getAsLong)
                .reduce(Long::sum);
    }

    @Override
    public long getReadTimeNanos()
    {
        return subConnectionPageSources.stream().mapToLong(ConnectorPageSource::getReadTimeNanos).sum();
    }

    @Override
    public boolean isFinished()
    {
        return pageQueue.isFinished();
    }

    @Override
    public long getMemoryUsage()
    {
        return subConnectionPageSources.stream().mapToLong(ConnectorPageSource::getMemoryUsage).sum();
    }

    @Override
    public void close()
    {
        if (closed) {
            log.debug("Page sources already closed");
            return;
        }
        readerTasks.forEach(task -> task.cancel(true));

        log.debug("Closing main connection...");
        // use try with resources to close everything properly
        try (Connection _ = this.mainConnection;
                Statement statement = this.mainStatement;
                ResultSet _ = this.mainResultSet) {
            if (statement != null) {
                try {
                    log.debug("Cancelling statement...");
                    // Trying to cancel running statement as close() may not do it
                    statement.cancel();
                }
                catch (SQLException e) {
                    log.debug("Cancel failed", e);
                    // statement already closed or cancel is not supported
                }
            }
        }
        catch (SQLException | RuntimeException e) {
            // ignore exception from close
            log.debug("Closing failed", e);
        }

        // Subconnections must be closed after the main connection.
        // When main connection is still open, closing of subconnections will block.
        log.debug("Closing {} subconnection page sources of {}...", subConnectionPageSources.size(), this);
        subConnectionPageSources.forEach(ExasolResultHandlePageSource::close);

        closed = true;
        log.debug("Closed page source {} successfully", this);
    }
}
