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

import io.trino.client.ResultRows;
import io.trino.client.StatementClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOutputHandler
{
    @Test
    public void testCloseFailureDoesNotReplaceStoppedReading()
    {
        AtomicBoolean closed = new AtomicBoolean();
        StatementClient client = (StatementClient) Proxy.newProxyInstance(
                StatementClient.class.getClassLoader(),
                new Class<?>[] {StatementClient.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "isRunning":
                            return !closed.get();
                        case "currentRows":
                            return endlessRows(closed);
                        case "advance":
                            return !closed.get();
                        case "isClientError":
                            return false;
                        case "close":
                            closed.set(true);
                            throw new UncheckedIOException(new IOException("failed to close rows"));
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
        OutputPrinter printer = new OutputPrinter()
        {
            @Override
            public void printRows(List<List<?>> rows, boolean complete)
            {
                throw new ResultLimitReachedException("enough rows");
            }

            @Override
            public void finish() {}
        };

        // the reader has to stop although the client fails to close
        OutputHandler handler = new OutputHandler(printer, 100, 1);
        assertThatThrownBy(() -> handler.processRows(client))
                .isInstanceOf(ResultLimitReachedException.class)
                .hasMessage("enough rows")
                .satisfies(e -> assertThat(e.getSuppressed())
                        .singleElement()
                        .isInstanceOf(UncheckedIOException.class));
        assertThat(closed).isTrue();
    }

    @Test
    public void testReaderFailureBeforeStoppingIsReported()
    {
        AtomicBoolean closed = new AtomicBoolean();
        CountDownLatch advanceFailed = new CountDownLatch(1);
        AtomicReference<Thread> readerThread = new AtomicReference<>();
        StatementClient client = (StatementClient) Proxy.newProxyInstance(
                StatementClient.class.getClassLoader(),
                new Class<?>[] {StatementClient.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "isRunning":
                            return !closed.get();
                        case "currentRows":
                            return rows(3);
                        case "advance":
                            // a failure that does not leave the client in the error state
                            readerThread.set(Thread.currentThread());
                            advanceFailed.countDown();
                            throw new RuntimeException("failed to fetch the next rows");
                        case "isClientError":
                            return false;
                        case "close":
                            closed.set(true);
                            return null;
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
        AtomicInteger printed = new AtomicInteger();
        OutputPrinter printer = new OutputPrinter()
        {
            @Override
            public void printRows(List<List<?>> rows, boolean complete)
                    throws IOException
            {
                // with a queue of one row, the reader has failed while the end token waits for room in the queue
                if (printed.incrementAndGet() == 2) {
                    try {
                        assertThat(advanceFailed.await(10, SECONDS)).isTrue();
                        awaitReaderBlockedOnEndToken(readerThread.get());
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                    throw new ResultLimitReachedException("enough rows");
                }
            }

            @Override
            public void finish() {}
        };

        OutputHandler handler = new OutputHandler(printer, 1, 1);
        assertThatThrownBy(() -> handler.processRows(client))
                .hasMessage("failed to fetch the next rows")
                .satisfies(e -> assertThat(e.getSuppressed())
                        .singleElement()
                        .isInstanceOf(ResultLimitReachedException.class));
    }

    // the thread that failed also runs the completion stage, which waits to queue the end token after the task is marked as failed
    private static void awaitReaderBlockedOnEndToken(Thread reader)
            throws InterruptedException
    {
        long deadline = System.nanoTime() + SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (reader.getState() == Thread.State.WAITING
                    && Arrays.stream(reader.getStackTrace()).anyMatch(frame -> frame.getClassName().equals("java.util.concurrent.ArrayBlockingQueue") && frame.getMethodName().equals("put"))) {
                return;
            }
            Thread.sleep(1);
        }
        throw new AssertionError("the reader did not fail");
    }

    private static ResultRows rows(int count)
    {
        return new ResultRows()
        {
            @Override
            public boolean isNull()
            {
                return false;
            }

            @Override
            public Iterator<List<Object>> iterator()
            {
                return IntStream.range(0, count)
                        .mapToObj(value -> List.<Object>of((long) value))
                        .iterator();
            }

            @Override
            public void close() {}
        };
    }

    private static ResultRows endlessRows(AtomicBoolean closed)
    {
        return new ResultRows()
        {
            @Override
            public boolean isNull()
            {
                return false;
            }

            @Override
            public Iterator<List<Object>> iterator()
            {
                return new Iterator<>()
                {
                    @Override
                    public boolean hasNext()
                    {
                        return !closed.get();
                    }

                    @Override
                    public List<Object> next()
                    {
                        return List.of(1L);
                    }
                };
            }

            @Override
            public void close() {}
        };
    }
}
