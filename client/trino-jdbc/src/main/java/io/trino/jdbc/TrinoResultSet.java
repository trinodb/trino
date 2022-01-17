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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final StatementClient client;
    private final String queryId;

    private final AtomicBoolean closed = new AtomicBoolean();

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

    @Override
    public void close()
            throws SQLException
    {
        if (closed.compareAndSet(false, true)) {
            ((AsyncIterator<?>) results).cancel();
            client.close();
        }
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        return closed.get();
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

    private static class AsyncIterator<T>
            extends AbstractIterator<T>
    {
        private static final int MAX_QUEUED_ROWS = 50_000;
        private static final ExecutorService executorService = newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("Trino JDBC worker-%s").setDaemon(true).build());

        private final StatementClient client;
        private final BlockingQueue<T> rowQueue = new ArrayBlockingQueue<>(MAX_QUEUED_ROWS);
        // Semaphore to indicate that some data is ready.
        // Each permit represents a row of data (or that the underlying iterator is exhausted).
        private final Semaphore semaphore = new Semaphore(0);
        private final CompletableFuture<Void> future;

        public AsyncIterator(Iterator<T> dataIterator, StatementClient client)
        {
            requireNonNull(dataIterator, "dataIterator is null");
            this.client = client;
            this.future = CompletableFuture.runAsync(() -> {
                try {
                    while (dataIterator.hasNext()) {
                        rowQueue.put(dataIterator.next());
                        semaphore.release();
                    }
                }
                catch (InterruptedException e) {
                    interrupt(e);
                }
                finally {
                    semaphore.release();
                }
            }, executorService);
        }

        public void cancel()
        {
            future.cancel(true);
        }

        public void interrupt(InterruptedException e)
        {
            client.close();
            Thread.currentThread().interrupt();
            throw new RuntimeException(new SQLException("ResultSet thread was interrupted", e));
        }

        @Override
        protected T computeNext()
        {
            try {
                semaphore.acquire();
            }
            catch (InterruptedException e) {
                interrupt(e);
            }
            if (rowQueue.isEmpty()) {
                // If we got here and the queue is empty the thread fetching from the underlying iterator is done.
                // Wait for Future to marked done and check status.
                try {
                    future.get();
                }
                catch (InterruptedException e) {
                    interrupt(e);
                }
                catch (ExecutionException e) {
                    throwIfUnchecked(e.getCause());
                    throw new RuntimeException(e.getCause());
                }
                return endOfData();
            }
            return rowQueue.poll();
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
