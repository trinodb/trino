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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
    private static final List<?> END_TOKEN = new ArrayList<>(0);

    private final AtomicBoolean closed = new AtomicBoolean();
    private final OutputPrinter printer;
    private final int maxQueuedRows;
    private final int maxBufferedRows;

    public OutputHandler(OutputPrinter printer, int maxQueuedRows, int maxBufferedRows)
    {
        this.printer = requireNonNull(printer, "printer is null");
        this.maxQueuedRows = maxQueuedRows;
        this.maxBufferedRows = maxBufferedRows;
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
        CompletableFuture<Void> readerFuture = CompletableFuture.runAsync(() -> {
            while (client.isRunning()) {
                for (List<Object> row : client.currentRows()) {
                    putOrThrow(rowQueue, row);
                }
                client.advance();
            }
        }).whenComplete((result, ex) -> putOrThrow(rowQueue, END_TOKEN));

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
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause != null) {
                throwIfInstanceOf(cause, IOException.class);
                throwIfUnchecked(cause);
            }
            throw new RuntimeException(e.getCause());
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
