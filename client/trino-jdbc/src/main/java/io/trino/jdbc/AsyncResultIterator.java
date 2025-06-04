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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.trino.client.CloseableIterator;
import io.trino.client.QueryStatusInfo;
import io.trino.client.StatementClient;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static io.trino.jdbc.ResultUtils.resultsException;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class AsyncResultIterator
        extends AbstractIterator<List<Object>>
        implements CloseableIterator<List<Object>>
{
    private static final int BATCH_SIZE = 100;
    private static final int MAX_QUEUED_ROWS = 50_000;
    private static final ExecutorService executorService = newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("Trino JDBC worker-%s").setDaemon(true).build());

    private final StatementClient client;
    private final BlockingQueue<List<Object>> rowQueue;
    // Semaphore to indicate that some data is ready.
    // Each permit represents a row of data (or that the underlying iterator is exhausted).
    private final Semaphore semaphore = new Semaphore(0);
    private final Future<?> future;

    private volatile boolean cancelled;
    private volatile boolean finished;

    AsyncResultIterator(StatementClient client, Consumer<QueryStats> progressCallback, WarningsManager warningsManager, Optional<BlockingQueue<List<Object>>> queue)
    {
        requireNonNull(progressCallback, "progressCallback is null");
        requireNonNull(warningsManager, "warningsManager is null");

        this.client = client;
        this.rowQueue = queue.orElseGet(() -> new ArrayBlockingQueue<>(MAX_QUEUED_ROWS));
        this.cancelled = false;
        this.finished = false;
        this.future = executorService.submit(() -> {
            try {
                int rowsProcessed = 0;
                do {
                    QueryStatusInfo results = client.currentStatusInfo();
                    progressCallback.accept(QueryStats.create(results.getId(), results.getStats()));
                    warningsManager.addWarnings(results.getWarnings());
                    for (List<Object> row : client.currentRows()) {
                        rowQueue.put(row);
                        if (rowsProcessed++ % BATCH_SIZE == 0) {
                            semaphore.release(rowsProcessed);
                            rowsProcessed = 0;
                        }
                    }
                }
                while (!cancelled && client.advance());
                if (rowsProcessed > 0) {
                    semaphore.release(rowsProcessed);
                }

                verify(client.isFinished());
                QueryStatusInfo results = client.finalStatusInfo();
                progressCallback.accept(QueryStats.create(results.getId(), results.getStats()));
                warningsManager.addWarnings(results.getWarnings());
                if (results.getError() != null) {
                    throw new RuntimeException(resultsException(results));
                }
            }
            catch (CancellationException | InterruptedException e) {
                close();
                throw new RuntimeException(new SQLException("ResultSet thread was interrupted", e));
            }
            finally {
                finished = true;
                semaphore.release();
            }
        });
    }

    @Override
    public void close()
    {
        synchronized (this) {
            if (cancelled) {
                return;
            }
            cancelled = true;
        }
        future.cancel(true);
        internalClose();
    }

    private void internalClose()
    {
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
    protected List<Object> computeNext()
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
        internalClose();
        Thread.currentThread().interrupt();
        throw new RuntimeException(new SQLException("Interrupted", e));
    }
}
