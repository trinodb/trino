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

import io.airlift.units.Duration;
import io.trino.client.StatementClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class OutputHandler
        implements Closeable
{
    private static final Duration MAX_BUFFER_TIME = new Duration(3, SECONDS);
    // how long to wait for the reader after the client was closed
    private static final Duration DEFAULT_READER_STOP_TIMEOUT = new Duration(10, SECONDS);
    private static final List<?> END_TOKEN = new ArrayList<>(0);

    private final AtomicBoolean closed = new AtomicBoolean();
    private final OutputPrinter printer;
    private final int maxQueuedRows;
    private final int maxBufferedRows;
    private final Duration readerStopTimeout;

    public OutputHandler(OutputPrinter printer, int maxQueuedRows, int maxBufferedRows)
    {
        this(printer, maxQueuedRows, maxBufferedRows, DEFAULT_READER_STOP_TIMEOUT);
    }

    OutputHandler(OutputPrinter printer, int maxQueuedRows, int maxBufferedRows, Duration readerStopTimeout)
    {
        this.printer = requireNonNull(printer, "printer is null");
        this.maxQueuedRows = maxQueuedRows;
        this.maxBufferedRows = maxBufferedRows;
        this.readerStopTimeout = requireNonNull(readerStopTimeout, "readerStopTimeout is null");
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed.getAndSet(true)) {
            printer.finish();
        }
    }

    public void processRows(StatementClient client)
            throws IOException
    {
        BlockingQueue<List<?>> rowQueue = new ArrayBlockingQueue<>(maxQueuedRows);
        CompletableFuture<Void> readerTask = CompletableFuture.runAsync(() -> {
            while (client.isRunning()) {
                for (List<Object> row : client.currentRows()) {
                    putOrThrow(rowQueue, row);
                }
                client.advance();
            }
        });
        // completes after the end token was queued, which waits while the queue is full
        CompletableFuture<Void> readerFuture = readerTask.whenComplete((result, ex) -> putOrThrow(rowQueue, END_TOKEN));

        List<List<?>> rowBuffer = new ArrayList<>(maxBufferedRows);
        long bufferStart = System.nanoTime();
        try {
            while (!readerFuture.isDone()) {
                boolean atEnd = drainDetectingEnd(rowQueue, rowBuffer, maxBufferedRows, END_TOKEN);
                if (atEnd) {
                    break;
                }

                // Flush if needed
                if (rowBuffer.size() >= maxBufferedRows || nanosSince(bufferStart).compareTo(MAX_BUFFER_TIME) >= 0) {
                    printer.printRows(unmodifiableList(rowBuffer), false);
                    rowBuffer.clear();
                    bufferStart = System.nanoTime();
                }

                List<?> row = rowQueue.poll(MAX_BUFFER_TIME.toMillis(), MILLISECONDS);
                if (row == END_TOKEN) {
                    break;
                }
                if (row != null) {
                    rowBuffer.add(row);
                }
            }
            if (!rowQueue.isEmpty()) {
                drainDetectingEnd(rowQueue, rowBuffer, Integer.MAX_VALUE, END_TOKEN);
            }
            printer.printRows(unmodifiableList(rowBuffer), true);
            readerFuture.get(); // propagate any exceptions
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (IOException | RuntimeException e) {
            if (!(e instanceof ResultLimitReachedException)) {
                // the client is not closed for a failure of the printer, which Query reports as it did before
                throw e;
            }
            // the reader must not stay blocked on a full queue
            Optional<Throwable> readerFailure = Optional.empty();
            try {
                readerFailure = stopReader(client, readerTask, readerFuture, rowQueue, readerStopTimeout);
            }
            catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            if (readerFailure.isPresent()) {
                // the query failed before it was stopped, which must not look like stopping on purpose
                Throwable failure = readerFailure.get();
                failure.addSuppressed(e);
                throwIfInstanceOf(failure, IOException.class);
                throwIfUnchecked(failure);
                throw new RuntimeException(failure);
            }
            throw e;
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause != null) {
                throwIfInstanceOf(cause, IOException.class);
                throwIfUnchecked(cause);
            }
            throw new RuntimeException(e.getCause());
        }
    }

    // returns the failure of the reader if the client failed on its own, and not because it was closed here
    private static Optional<Throwable> stopReader(StatementClient client, CompletableFuture<Void> readerTask, CompletableFuture<Void> readerFuture, BlockingQueue<List<?>> rowQueue, Duration timeout)
    {
        Optional<Throwable> failedBeforeClose = readerFailure(readerTask);
        try {
            client.close();
        }
        finally {
            awaitReader(readerFuture, rowQueue, timeout);
        }
        if (failedBeforeClose.isPresent() || !client.isClientError()) {
            return failedBeforeClose;
        }
        return readerFailure(readerTask);
    }

    private static Optional<Throwable> readerFailure(CompletableFuture<Void> readerTask)
    {
        if (!readerTask.isCompletedExceptionally()) {
            return Optional.empty();
        }
        try {
            readerTask.getNow(null);
            return Optional.empty();
        }
        catch (CompletionException e) {
            return Optional.of(e.getCause() != null ? e.getCause() : e);
        }
        catch (CancellationException e) {
            return Optional.empty();
        }
    }

    // gives up on a reader that does not return after the client was closed, as it is waiting for a response
    private static void awaitReader(CompletableFuture<Void> readerFuture, BlockingQueue<List<?>> rowQueue, Duration timeout)
    {
        long deadline = System.nanoTime() + MILLISECONDS.toNanos(timeout.toMillis());
        while (System.nanoTime() < deadline) {
            // make room for a reader that waits to add a row
            rowQueue.clear();
            try {
                readerFuture.get(10, MILLISECONDS);
                return;
            }
            catch (TimeoutException e) {
                // the reader is still running
            }
            catch (ExecutionException e) {
                // the reader failed, which is expected after the client was closed
                return;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static <E> boolean drainDetectingEnd(BlockingQueue<E> blockingQueue, List<E> buffer, int maxBufferSize, E endToken)
    {
        int drained = blockingQueue.drainTo(buffer, maxBufferSize - buffer.size());
        if (drained > 0 && buffer.get(buffer.size() - 1) == endToken) {
            buffer.remove(buffer.size() - 1);
            return true;
        }

        return false;
    }

    private static <E> void putOrThrow(BlockingQueue<E> blockingQueue, E element)
    {
        try {
            blockingQueue.put(element);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
