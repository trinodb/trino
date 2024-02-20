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
package io.trino.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.client.Column;
import io.trino.client.QueryStatusInfo;
import io.trino.client.StatementClient;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TrinoResultSet
        extends AbstractTrinoResultSet
{
    private final Statement statement;
    private final StatementClient client;
    private final String queryId;

    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private boolean closeStatementOnClose;

    static TrinoResultSet create(Statement statement, StatementClient client, long maxRows, Consumer<QueryStats> progressCallback, WarningsManager warningsManager)
            throws SQLException
    {
        requireNonNull(client, "client is null");
        List<Column> columns = getColumns(client, progressCallback);
        return new TrinoResultSet(statement, client, columns, maxRows, progressCallback, warningsManager);
    }

    private TrinoResultSet(Statement statement, StatementClient client, List<Column> columns, long maxRows, Consumer<QueryStats> progressCallback, WarningsManager warningsManager)
            throws SQLException
    {
        super(
                Optional.of(requireNonNull(statement, "statement is null")),
                columns,
                new AsyncIterator<>(flatten(new ResultsPageIterator(requireNonNull(client, "client is null"), progressCallback, warningsManager), maxRows), client));

        this.statement = statement;
        this.client = requireNonNull(client, "client is null");
        requireNonNull(progressCallback, "progressCallback is null");

        this.queryId = client.currentStatusInfo().getId();
    }

    public String getQueryId()
    {
        return queryId;
    }

    public QueryStats getStats()
    {
        return QueryStats.create(queryId, client.getStats());
    }

    void setCloseStatementOnClose()
            throws SQLException
    {
        boolean alreadyClosed;
        synchronized (this) {
            alreadyClosed = closed;
            if (!alreadyClosed) {
                closeStatementOnClose = true;
            }
        }
        if (alreadyClosed) {
            statement.close();
        }
    }

    @Override
    public void close()
            throws SQLException
    {
        boolean closeStatement;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            closeStatement = closeStatementOnClose;
        }

        ((AsyncIterator<?>) results).cancel();
        client.close();
        if (closeStatement) {
            statement.close();
        }
    }

    @Override
    public synchronized boolean isClosed()
            throws SQLException
    {
        return closed;
    }

    void partialCancel()
    {
        client.cancelLeafStage();
    }

    private static <T> Iterator<T> flatten(Iterator<Iterable<T>> iterator, long maxRows)
    {
        Stream<T> stream = Streams.stream(iterator)
                .flatMap(Streams::stream);
        if (maxRows > 0) {
            stream = stream.limit(maxRows);
        }
        return stream.iterator();
    }

    @VisibleForTesting
    static class AsyncIterator<T>
            extends AbstractIterator<T>
    {
        private static final int MAX_QUEUED_ROWS = 50_000;
        private static final ExecutorService executorService = newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("Trino JDBC worker-%s").setDaemon(true).build());

        private final StatementClient client;
        private final BlockingQueue<T> rowQueue;
        // Semaphore to indicate that some data is ready.
        // Each permit represents a row of data (or that the underlying iterator is exhausted).
        private final Semaphore semaphore = new Semaphore(0);
        private final Future<?> future;
        private volatile boolean cancelled;
        private volatile boolean finished;

        public AsyncIterator(Iterator<T> dataIterator, StatementClient client)
        {
            this(dataIterator, client, Optional.empty());
        }

        @VisibleForTesting
        AsyncIterator(Iterator<T> dataIterator, StatementClient client, Optional<BlockingQueue<T>> queue)
        {
            requireNonNull(dataIterator, "dataIterator is null");
            this.client = client;
            this.rowQueue = queue.orElseGet(() -> new ArrayBlockingQueue<>(MAX_QUEUED_ROWS));
            this.cancelled = false;
            this.finished = false;
            this.future = executorService.submit(() -> {
                try {
                    while (!cancelled && dataIterator.hasNext()) {
                        rowQueue.put(dataIterator.next());
                        semaphore.release();
                    }
                }
                catch (InterruptedException e) {
                    client.close();
                    rowQueue.clear();
                    throw new RuntimeException(new SQLException("ResultSet thread was interrupted", e));
                }
                finally {
                    semaphore.release();
                    finished = true;
                }
            });
        }

        public void cancel()
        {
            cancelled = true;
            future.cancel(true);
            // When thread interruption is mis-handled by underlying implementation of `client`, the thread which
            // is working for `future` may be blocked by `rowQueue.put` (`rowQueue` is full) and will never finish
            // its work. It is necessary to close `client` and drain `rowQueue` to avoid such leaks.
            client.close();
            rowQueue.clear();
        }

        @VisibleForTesting
        Future<?> getFuture()
        {
            return future;
        }

        @VisibleForTesting
        boolean isBackgroundThreadFinished()
        {
            return finished;
        }

        @Override
        protected T computeNext()
        {
            try {
                semaphore.acquire();
            }
            catch (InterruptedException e) {
                handleInterrupt(e);
            }
            if (rowQueue.isEmpty()) {
                // If we got here and the queue is empty the thread fetching from the underlying iterator is done.
                // Wait for Future to marked done and check status.
                try {
                    future.get();
                }
                catch (InterruptedException e) {
                    handleInterrupt(e);
                }
                catch (ExecutionException e) {
                    throwIfUnchecked(e.getCause());
                    throw new RuntimeException(e.getCause());
                }
                return endOfData();
            }
            return rowQueue.poll();
        }

        private void handleInterrupt(InterruptedException e)
        {
            cancel();
            Thread.currentThread().interrupt();
            throw new RuntimeException(new SQLException("Interrupted", e));
        }
    }

    private static class ResultsPageIterator
            extends AbstractIterator<Iterable<List<Object>>>
    {
        private final StatementClient client;
        private final Consumer<QueryStats> progressCallback;
        private final WarningsManager warningsManager;

        private ResultsPageIterator(StatementClient client, Consumer<QueryStats> progressCallback, WarningsManager warningsManager)
        {
            this.client = requireNonNull(client, "client is null");
            this.progressCallback = requireNonNull(progressCallback, "progressCallback is null");
            this.warningsManager = requireNonNull(warningsManager, "warningsManager is null");
        }

        @Override
        protected Iterable<List<Object>> computeNext()
        {
            while (client.isRunning()) {
                QueryStatusInfo results = client.currentStatusInfo();
                progressCallback.accept(QueryStats.create(results.getId(), results.getStats()));
                warningsManager.addWarnings(results.getWarnings());
                Iterable<List<Object>> data = client.currentData().getData();

                try {
                    client.advance();
                }
                catch (RuntimeException e) {
                    throw e;
                }

                if (data != null) {
                    return data;
                }
            }

            verify(client.isFinished());
            QueryStatusInfo results = client.finalStatusInfo();
            progressCallback.accept(QueryStats.create(results.getId(), results.getStats()));
            warningsManager.addWarnings(results.getWarnings());
            if (results.getError() != null) {
                throw new RuntimeException(resultsException(results));
            }
            return endOfData();
        }
    }

    private static List<Column> getColumns(StatementClient client, Consumer<QueryStats> progressCallback)
            throws SQLException
    {
        while (client.isRunning()) {
            QueryStatusInfo results = client.currentStatusInfo();
            progressCallback.accept(QueryStats.create(results.getId(), results.getStats()));
            List<Column> columns = results.getColumns();
            if (columns != null) {
                return columns;
            }
            client.advance();
        }

        verify(client.isFinished());
        QueryStatusInfo results = client.finalStatusInfo();
        if (results.getError() == null) {
            throw new SQLException(format("Query has no columns (#%s)", results.getId()));
        }
        throw resultsException(results);
    }
}
